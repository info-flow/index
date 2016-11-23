#!/bin/bash

err_proc=1

function assert_status ()
{
    if [ $? -ne 0 ]
    then
        echo "FAILED: $1"
        exit ${err_proc}
    fi
}

# test one
echo "test one"
/da1/db/python27/bin/python make-partitions.py ./test/test-one.conf
assert_status "test one make-partitions.py"
/da1/db/python27/bin/python merge-partitions.py ./test/test-one.conf
assert_status "test one merge-partitions.py"

