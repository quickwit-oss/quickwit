# Copyright 2020 The Bazel Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Copied from: https://github.com/bazelbuild/bazel/blob/6f7faa659e5eb3e56c8a6274ebcb86884703d603/src/main/starlark/builtins_bzl/common/cc/cc_helper.bzl

"""Utility functions to expand make variables. Implementation taken from cc_helper. """

def expand_make_variables(ctx, tokenization, unexpanded_tokens, additional_make_variable_substitutions = {}):
    tokens = []
    targets = []
    for additional_compiler_input in getattr(ctx.attr, "additional_compiler_inputs", []):
        targets.append(additional_compiler_input)
    for token in unexpanded_tokens:
        if tokenization:
            expanded_token = _expand(ctx, token, additional_make_variable_substitutions, targets = targets)
            _tokenize(tokens, expanded_token)
        else:
            exp = _expand_single_make_variable(ctx, token, additional_make_variable_substitutions)
            if exp != None:
                _tokenize(tokens, exp)
            else:
                tokens.append(_expand(ctx, token, additional_make_variable_substitutions, targets = targets))
    return tokens

# Tries to expand a single make variable from token.
# If token has additional characters other than ones
# corresponding to make variable returns None.
def _expand_single_make_variable(ctx, token, additional_make_variable_substitutions = {}):
    if len(token) < 3:
        return None
    if token[0] != "$" or token[1] != "(" or token[len(token) - 1] != ")":
        return None
    unexpanded_var = token[2:len(token) - 1]
    expanded_var = _expand_nested_variable(ctx, additional_make_variable_substitutions, unexpanded_var)
    return expanded_var

def _expand_nested_variable(ctx, additional_vars, exp, execpath = True, targets = []):
    # If make variable is predefined path variable(like $(location ...))
    # we will expand it first.
    if exp.find(" ") != -1:
        if not execpath:
            if exp.startswith("location"):
                exp = exp.replace("location", "rootpath", 1)
        data_targets = []
        if ctx.attr.data != None:
            data_targets = ctx.attr.data

        # Make sure we do not duplicate targets.
        unified_targets_set = {}
        for data_target in data_targets:
            unified_targets_set[data_target] = True
        for target in targets:
            unified_targets_set[target] = True
        return ctx.expand_location("$({})".format(exp), targets = unified_targets_set.keys())

    # Recursively expand nested make variables, but since there is no recursion
    # in Starlark we will do it via for loop.
    unbounded_recursion = True

    # The only way to check if the unbounded recursion is happening or not
    # is to have a look at the depth of the recursion.
    # 10 seems to be a reasonable number, since it is highly unexpected
    # to have nested make variables which are expanding more than 10 times.
    for _ in range(10):
        exp = _lookup_var(ctx, additional_vars, exp)
        if len(exp) >= 3 and exp[0] == "$" and exp[1] == "(" and exp[len(exp) - 1] == ")":
            # Try to expand once more.
            exp = exp[2:len(exp) - 1]
            continue
        unbounded_recursion = False
        break

    if unbounded_recursion:
        fail("potentially unbounded recursion during expansion of {}".format(exp))
    return exp

def _lookup_var(ctx, additional_vars, var):
    expanded_make_var = additional_vars.get(var)
    if expanded_make_var != None:
        return expanded_make_var

    expanded_make_var = ctx.var.get(var)
    if expanded_make_var != None:
        return expanded_make_var

    # ctx.rule.var is only available in Bazel 9+
    expanded_make_var = getattr(ctx.rule, "var", {}).get(var)
    if expanded_make_var != None:
        return expanded_make_var

    fail("{}: {} not defined".format(ctx.label, "$(" + var + ")"))

def _expand(ctx, expression, additional_make_variable_substitutions, execpath = True, targets = []):
    idx = 0
    last_make_var_end = 0
    result = []
    n = len(expression)
    for _ in range(n):
        if idx >= n:
            break
        if expression[idx] != "$":
            idx += 1
            continue

        idx += 1

        # We've met $$ pattern, so $ is escaped.
        if idx < n and expression[idx] == "$":
            idx += 1
            result.append(expression[last_make_var_end:idx - 1])
            last_make_var_end = idx
            # We might have found a potential start for Make Variable.

        elif idx < n and expression[idx] == "(":
            # Try to find the closing parentheses.
            make_var_start = idx
            make_var_end = make_var_start
            for j in range(idx + 1, n):
                if expression[j] == ")":
                    make_var_end = j
                    break

            # Note we cannot go out of string's bounds here,
            # because of this check.
            # If start of the variable is different from the end,
            # we found a make variable.
            if make_var_start != make_var_end:
                # Some clarifications:
                # *****$(MAKE_VAR_1)*******$(MAKE_VAR_2)*****
                #                   ^       ^          ^
                #                   |       |          |
                #   last_make_var_end  make_var_start make_var_end
                result.append(expression[last_make_var_end:make_var_start - 1])
                make_var = expression[make_var_start + 1:make_var_end]
                exp = _expand_nested_variable(ctx, additional_make_variable_substitutions, make_var, execpath, targets)
                result.append(exp)

                # Update indexes.
                idx = make_var_end + 1
                last_make_var_end = idx

    # Add the last substring which would be skipped by for loop.
    if last_make_var_end < n:
        result.append(expression[last_make_var_end:n])

    return "".join(result)

def _tokenize(options, options_string):
    token = []
    force_token = False
    quotation = "\0"
    length = len(options_string)

    # Since it is impossible to modify loop variable inside loop
    # in Starlark, and also there is no while loop, I have to
    # use this ugly hack.
    i = -1
    for _ in range(length):
        i += 1
        if i >= length:
            break
        c = options_string[i]
        if quotation != "\0":
            # In quotation.
            if c == quotation:
                # End quotation.
                quotation = "\0"
            elif c == "\\" and quotation == "\"":
                i += 1
                if i == length:
                    fail("backslash at the end of the string: {}".format(options_string))
                c = options_string[i]
                if c != "\\" and c != "\"":
                    token.append("\\")
                token.append(c)
            else:
                # Regular char, in quotation.
                token.append(c)
        else:
            # Not in quotation.
            if c == "'" or c == "\"":
                # Begin single double quotation.
                quotation = c
                force_token = True
            elif c == " " or c == "\t":
                # Space not quoted.
                if force_token or len(token) > 0:
                    options.append("".join(token))
                    token = []
                    force_token = False
            elif c == "\\":
                # Backslash not quoted.
                i += 1
                if i == length:
                    fail("backslash at the end of the string: {}".format(options_string))
                token.append(options_string[i])
            else:
                # Regular char, not quoted.
                token.append(c)
    if quotation != "\0":
        fail("unterminated quotation at the end of the string: {}".format(options_string))

    if force_token or len(token) > 0:
        options.append("".join(token))
