#!/bin/bash
export USE_VALGRIND="YES"
#export RS_TEST_VALGRIND_EXTRA_OPTS="--leak-check=full --show-leak-kinds=all"
source ${srcdir:=.}/omhttp-batch-jsonarray-compress.sh
