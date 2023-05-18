#!/usr/bin/env python3

import subprocess
import requests
import glob
import yaml
import sys
from os import path as osp

def open_scenario(scenario_filepath):
    data = open(scenario_filepath).read()
    steps_data = data.split("---")
    for step_data in steps_data:
        step_data  = step_data.strip()
        if step_data == "":
            continue
        yield yaml.load(step_data, Loader=yaml.Loader)

def run_step(step, root):
    if "method" in step:
        methods = step["method"]
        if type(methods) != list:
            methods = [methods]
        for method in methods:
            run_request_step(method, step, root)

def run_request_step(method, step, root):
    assert method in {"GET", "POST", "PUT", "DELETE"}
    if "headers" not in step:
        step["headers"] = {'user-agent': 'my-app/0.0.1'}
    method_req = getattr(requests, method.lower())
    endpoint = step.get("endpoint", "/gharchive/_search")
    url = root + endpoint
    kvargs = {
        k: v
        for k, v in step.items()
        if k in {"params", "data", "json", "headers"}
    }
    r = method_req(url, **kvargs)
    expected_status_code = step.get("status_code", 200)
    if r.status_code != expected_status_code:
        print("-----")
        print(r.text)
        raise Exception("Wrong status code. Got %s, expected %s" % (r.status_code, expected_status_code))
    expected_resp = step.get("expected", {})
    if not check_result(r.json(), expected_resp):
        return False
    return True

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
    if type(result) == dict:
        return check_result_dict(result, expected, context_path)
    if type(result) == list:
        return check_result_list(result, expected, context_path)
    if result != expected:
        print(result, expected)
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
        path_tree = self
        for path_seg in path_segs[:-1]:
            path_tree = path_tree.add_child(path_seg)
        path_tree.add_script(path_segs[-1])

    def visit_nodes(self, visitor, path=[]):
        visitor.enter_directory(path)
        for script in self.scripts:
            visitor.run_script(path, script)
        for k in sorted(self.children.keys()):
            child_path = path + [k]
            self.children[k].visit_nodes(visitor, child_path)
        visitor.exit_directory(path)


ENDPOINTS = {
    "elasticsearch": "http://localhost:9200",
    "quickwit": "http://localhost:7280/api/v1/_elastic"
}

class Visitor:
    def __init__(self, engine):
        self.endpoint = ENDPOINTS[engine]
        self.engine = engine
    def run_scripts(self, script_name, path):
        cwd = "/".join(path)
        for file_name in [script_name + ".sh", script_name + "." + self.engine + ".sh"]:
            script_fullpath = cwd + "/" + file_name
            if osp.exists(script_fullpath):
                print("Running %s script: %s" % (script_name, script_fullpath))
                subprocess.run(["sh", file_name], cwd=cwd)
                break
            else:
                pass
    def enter_directory(self, path):
        self.run_scripts("setup", path)
    def exit_directory(self, path):
        self.run_scripts("teardown", path)
    def run_script(self, path, script):
        scenario_path = "/".join(path + [script])
        steps = list(open_scenario(scenario_path))
        num_steps = 0
        for (i, step) in enumerate(steps, 1):
            applicable_engine = step.get("engines", None)
            if applicable_engine is not None:
                if self.engine not in applicable_engine:
                    continue
            try:
                run_step(step, self.endpoint)
                num_steps += 1
            except Exception as e:
                print("🔴 %s" % scenario_path)
                print("Failed at step %d" % i)
                print(step)
                print(e)
                print("--------------")
                break
        else:
            print("🟢 %s (%d steps)" % (scenario_path, num_steps))

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

def main():
    import argparse
    arg_parser = argparse.ArgumentParser(
        prog="rest-api-test",
        description="Runs a set of calls against a REST API and checks for conditions over the results."
    )
    arg_parser.add_argument("--engine", help="Targetted engine (elastic/quickwit).", default="elasticsearch")
    arg_parser.add_argument("--test", help="Specific test to run. If not specified, all tests are run.", default="scenarii/**/*.yaml")
    parsed_args = arg_parser.parse_args()
    print(parsed_args)
    print("------------------------\n")
    scenario_filepaths = glob.glob(parsed_args.test, recursive=True)
    run(scenario_filepaths, engine=parsed_args.engine)

if __name__ == "__main__":
    main()

