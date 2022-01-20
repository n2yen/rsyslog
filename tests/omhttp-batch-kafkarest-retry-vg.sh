#!/bin/bash
export USE_VALGRIND="YES"
#export RS_TEST_VALGRIND_EXTRA_OPTS="--leak-check=full --show-leak-kinds=all"
#export RS_TEST_VALGRIND_EXTRA_OPTS="--vgdb=yes --vgdb-error=0"
source ${srcdir:=.}/omhttp-batch-kafkarest-retry.sh
