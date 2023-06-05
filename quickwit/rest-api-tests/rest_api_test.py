#!/usr/bin/env python3

import subprocess
import requests
import glob
import yaml
import sys
from os import path as osp
import gzip
import http
import json

def debug_http():
    old_send = http.client.HTTPConnection.send
    def new_send(self, data):
        print(f'{"-"*9} BEGIN REQUEST {"-"*9}')
        if len(data) > 500:
            print("Data too big")
            print(data[:500])
        else:
            print(data.decode('utf-8').strip())
        print(f'{"-"*10} END REQUEST {"-"*10}')
        return old_send(self, data)
    http.client.HTTPConnection.send = new_send

# debug_http()

def open_scenario(scenario_filepath):
    data = open(scenario_filepath).read()
    steps_data = data.split("\n---")
    for step_data in steps_data:
        step_data  = step_data.strip()
        if step_data == "":
            continue
        step_dict = yaml.load(step_data, Loader=yaml.Loader)
        if type(step_dict) == dict:
            yield step_dict

def run_step(step):
    if "method" in step:
        methods = step["method"]
        if type(methods) != list:
            methods = [methods]
        for method in methods:
            run_request_step(method, step)

def load_data(path):
    if path.endswith("gz"):
        return gzip.open(path, 'rb').read()
    else:
        return open(path, 'rb').read()

def run_request_step(method, step):
    assert method in {"GET", "POST", "PUT", "DELETE"}
    if "headers" not in step:
        step["headers"] = {'user-agent': 'my-app/0.0.1'}
    method_req = getattr(requests, method.lower())
    endpoint = step.get("endpoint", "")
    url = step["api_root"] + endpoint
    kvargs = {
        k: v
        for k, v in step.items()
        if k in {"params", "data", "json", "headers"}
    }
    body_from_file = step.get("body_from_file", None)
    if body_from_file is not None:
        body_from_file = osp.join(step["cwd"], body_from_file)
        kvargs["data"] = load_data(body_from_file)
    ndjson = step.get("ndjson", None)
    if ndjson is not None:
        kvargs["data"] = "\n".join([json.dumps(doc) for doc in ndjson])
        kvargs.setdefault("headers")["Content-Type"] = "application/json"
    r = method_req(url, **kvargs)
    expected_status_code = step.get("status_code", 200)
    if expected_status_code is not None:
        if r.status_code != expected_status_code:
            print(r.text)
            raise Exception("Wrong status code. Got %s, expected %s" % (r.status_code, expected_status_code))
    expected_resp = step.get("expected", None)
    if expected_resp is not None:
        try:
            check_result(r.json(), expected_resp)
        except Exception as e:
            print(json.dumps(r.json(), indent=2))
            raise e

def check_result(result, expected, context_path = ""):
    if type(expected) == dict and "$expect" in expected:
        expectations = expected["$expect"]
        if type(expectations) == str:
            expectations = [expectations]
        for expectation in expectations:
            if not eval(expectation, None, {"val": result}):
                print(result)
                raise Exception("Failed to meet expectation %s at %s" % (expectation, context_path))
            return
    if type(result) != type(expected):
        raise Exception("Wrong type at context %s. Got %s, Expected %s" % (context_path, type(result), type(expected)))
    elif type(result) == dict:
        check_result_dict(result, expected, context_path)
    elif type(result) == list:
        check_result_list(result, expected, context_path)
    elif result != expected:
        raise Exception("Expected %s at context %s, got %s" % (expected, context_path, result))

def check_result_list(result, expected, context_path=""):
    if len(result) != len(expected):
        raise(Exception("Wrong length at context %s" % context_path))
    for (i, (left, right)) in enumerate(zip(result, expected)):
        check_result(left, right, context_path + "[%s]" % i)

def check_result_dict(result, expected, context_path=""):
    for (k, v) in expected.items():
        child = result.get(k, None)
        if child is None:
            raise Exception("Missing key %s at context %s" % (k, context_path))
        check_result(child, v, context_path + "." + k)

class PathTree:
    def __init__(self):
        self.children = {}
        self.scripts = []

    def add_child(self, seg):
        child = self.children.get(seg, None)
        if child is None:
            self.children[seg] = PathTree()
        return self.children[seg]

    def add_script(self, script):
        self.scripts.append(script)

    def add_path(self, path):
        path_segs = path.split("/")
        if path_segs[-1].startswith("_"):
            return
        path_tree = self
        for path_seg in path_segs[:-1]:
            path_tree = path_tree.add_child(path_seg)
        path_tree.add_script(path_segs[-1])

    def visit_nodes(self, visitor, path=[]):
        visitor.enter_directory(path)
        for script in self.scripts:
            visitor.run_scenario(path, script)
        for k in sorted(self.children.keys()):
            child_path = path + [k]
            self.children[k].visit_nodes(visitor, child_path)
        visitor.exit_directory(path)

# Returns a new dictionary without modifying the arguments.
# The new dictionary is the result of merging the two dictionaries
# in that order:
# The second dictionary may shadow/override the keys of the first dictionar
def stack_dicts(context, overriding):
    context = context.copy()
    context.update(overriding)
    return context

class Visitor:
    def __init__(self, engine):
        self.engine = engine
        self.context_stack = []
        self.context = {}
    def run_setup_teardown_scripts(self, script_name, path):
        cwd = "/".join(path)
        for file_name in [script_name + ".yaml", script_name + "." + self.engine + ".yaml"]:
            script_fullpath = cwd + "/" + file_name
            if osp.exists(script_fullpath):
                self.run_scenario(path, file_name)
    def load_context(self, path):
        context = {"cwd": "/".join(path)}
        for file_name in ["_ctx.yaml", "_ctx." + self.engine + ".yaml"]:
            ctx_filepath = "/".join(path + [file_name])
            if osp.exists(ctx_filepath):
                ctx = yaml.load(open(ctx_filepath), Loader=yaml.Loader)
                context.update(ctx)
        self.context_stack.append(context)
        self.context.update(context)
    def enter_directory(self, path):
        print("============")
        self.load_context(path)
        self.run_setup_teardown_scripts("_setup", path)
    def exit_directory(self, path):
        self.run_setup_teardown_scripts("_teardown", path)
        self.context_stack.pop()
        self.context = {}
        for ctx in self.context_stack:
            self.context.update(ctx)
    def run_scenario(self, path, script):
        scenario_path = "/".join(path + [script])
        steps = list(open_scenario(scenario_path))
        num_steps_executed = 0
        num_steps_skipped = 0
        for (i, step) in enumerate(steps, 1):
            step = stack_dicts(self.context, step)
            applicable_engine = step.get("engines", None)
            if applicable_engine is not None:
                if self.engine not in applicable_engine:
                    num_steps_skipped += 1
                    continue
            try:
                run_step(step)
                num_steps_executed += 1
            except Exception as e:
                print("🔴 %s" % scenario_path)
                print("Failed at step %d" % i)
                print(step)
                print(e)
                print("--------------")
                break
        else:
            print("🟢 %s: %d steps (%d skipped)" % (scenario_path, num_steps_executed, num_steps_skipped))

def build_path_tree(paths):
    paths.sort()
    path_tree = PathTree()
    for path in paths:
        path_tree.add_path(path)
    return path_tree

def run(scenario_paths, engine):
    path_tree = build_path_tree(scenario_paths)
    visitor = Visitor(engine=engine)
    path_tree.visit_nodes(visitor)

def filter_test(prefixes, test_name):
    for prefix in prefixes:
        if test_name.startswith(prefix):
            return True
    return False

def filter_tests(prefixes, test_names):
    if prefixes is None or len(prefixes) == 0:
        return test_names
    return [
        test_name
        for test_name in test_names
        if filter_test(prefixes, test_name)
    ]

def main():
    import argparse
    arg_parser = argparse.ArgumentParser(
        prog="rest-api-test",
        description="Runs a set of calls against a REST API and checks for conditions over the results."
    )
    arg_parser.add_argument("--engine", help="Targetted engine (elastic/quickwit).", default="quickwit")
    arg_parser.add_argument("--test", help="Specific prefix to select the tests to run. If not specified, all tests are run.", nargs="*")
    parsed_args = arg_parser.parse_args()
    scenario_filepaths = glob.glob("scenarii/**/*.yaml", recursive=True)
    scenario_filepaths = list(filter_tests(parsed_args.test, scenario_filepaths))
    run(scenario_filepaths, engine=parsed_args.engine)

if __name__ == "__main__":
    main()

