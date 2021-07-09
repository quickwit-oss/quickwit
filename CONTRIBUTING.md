# Contributing to Quickwit
There are many ways to contribute to Quickwit.
Code contribution are welcome of course, but also
bug reports, feature request, and evangelizing are as valuable.

# Submitting a PR
Check if your issue is already listed [github](https://github.com/quickwit-inc/quickwit/issues).
If it is not, create your own issue.

Please add the following phrase at the end of your commit.  `Closes #<Issue Number>`.
It will automatically link your PR in the issue page. Also, once your PR is merged, it will
closes the issue. If your PR only partially addresses the issue and you would like to
keep it open, just write `See #<Issue Number>`.

Feel free to send your contribution in an unfinished state to get early feedback.
In that case, simply mark the PR with the tag [WIP] (standing for work in progress).

# Signing the CLA
Quickwit is an opensource project licensed a AGPLv3.
It is also distributed under a commercial license by Quickwit Inc.

Contributors are required to sign a Contributor License Agreement.
The process is simple and fast. Upon your first pull request, you will be prompted to
[sign our CLA by visiting this link](https://cla-assistant.io/quickwit-inc/quickwit).

# Development
## Setup & run tests
1. Install docker https://docs.docker.com/engine/install/
2. Install localstack https://github.com/localstack/localstack#installing
3. Install awslocal https://github.com/localstack/awscli-local
4. Prepare s3 bucket used in tests, execute `./quickwit-cli/tests/prepare_tests.sh`
5. `QUICKWIT_ENV=LOCAL cargo test --all-features`
