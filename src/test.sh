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

function assert_condition ()
{
    E_PARAM_ERR=98
    E_ASSERT_FAILED=99


    if [ -z "$2" ]          #  Not enough parameters passed
    then                    #+ to assert() function.
        return $E_PARAM_ERR #  No damage done.
    fi

    lineno=$2

    if [ ! $1 ]
    then
        echo "Assertion failed:  \"$1\""
        echo "File \"$0\", line $lineno"    # Give name of file and line number.
        exit $E_ASSERT_FAILED
        # else
        #   return
        #   and continue executing the script.
    fi
}

function clean_env () {
    # test one
    rm -rf ./test/one/test_DP_output_alltag_V13
    rm -rf ./test/one/test_DP_output_kws_V10
    rm -rf ./test/one/test_DP_output_main
    rm -rf ./test/one/test_nrot_input_alltag_V13
    rm -rf ./test/one/test_nrot_input_kws_V10
    rm -rf ./test/one/test_upload
    rm -rf ./test/one/test_merger

    # test two
}

# clean env
clean_env

# test one
echo "test one"
/da1/db/python27/bin/python make-partitions.py ./test/test-one.conf
assert_status "test one make-partitions.py"

assert_condition "-d ./test/one/test_nrot_input_main"
assert_condition "-d ./test/one/test_DP_output_main"
diff -r ./test/one/test_nrot_input_main ./test/one-gold/test_nrot_input_main
assert_status "./test/one/test_nrot_input_main ./test/one-gold/test_nrot_input_main different"
diff -r ./test/one/test_DP_output_main ./test/one-gold/test_DP_output_main
assert_status "./test/one/test_DP_output_main ./test/one-gold/test_DP_output_main different"

/da1/db/python27/bin/python merge-partitions.py ./test/test-one.conf
assert_status "test one merge-partitions.py" assert_condition "-d
./test/one/test_upload" assert_condition "-d ./test/one/test_merger"
assert_status "$(ls ./test/one/test_merger) -eq 4"
assert_status "$(ls ./test/one/test_upload) -eq 6"

# test two
