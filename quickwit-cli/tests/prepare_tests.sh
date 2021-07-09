#!/bin/bash
awslocal s3 mb s3://quickwit-integration-tests && awslocal s3 rm --recursive s3://quickwit-integration-tests
