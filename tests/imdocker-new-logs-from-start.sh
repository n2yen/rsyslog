#!/bin/bash
# This is part of the rsyslog testbench, licensed under ASL 2.0

# imdocker unit tests are enabled with --enable-imdocker-tests
. ${srcdir:=.}/diag.sh init

#export RS_REDIR=-d
generate_conf
add_conf '
template(name="template_msg_only" type="string" string="%msg%\n")
module(load="../contrib/imdocker/.libs/imdocker" PollingInterval="1"
        GetContainerLogOptions="tail=1&timestamps=0&follow=1&stdout=1&stderr=0&tail=1"
        retrievenewlogsfromstart="on"
        )
action(type="omfile" template="template_msg_only"  file="'$RSYSLOG_OUT_LOG'")
'

#NUM_ITEMS=1000
# launch a docker runtime to generate some logs.
# these log items should be tailed.
docker run \
   --rm \
   -e seq_start=1001 \
   -e seq_end=2000 \
   alpine \
   /bin/sh -c 'for i in `seq $seq_start $seq_end`; do echo "tailed item $i"; sleep .01; done' > /dev/null &

#/bin/sh -c 'sleep 3; for i in `seq $seq_start $seq_end`; do echo "tailed item $i"; sleep .01; done' > /dev/null &
sleep 3

startup
NUM_ITEMS=1000
# launch a docker runtime to generate some logs.
# These logs started after start-up should get from beginning
sleep 1
docker run \
   --rm \
   -e num_items=$NUM_ITEMS \
   alpine \
   /bin/sh -c 'for i in `seq 1 $num_items`; do echo "log item $i"; sleep .01; done' > /dev/null

content_check_with_count 'log item' $NUM_ITEMS
echo "file name: $RSYSLOG_OUT_LOG"
#echo "\"tailed item\" occured: $(grep 'tailed item ' $RSYSLOG_OUT_LOG | wc -l)/1000 (expect less)."
echo "\"tailed item\" occured: $(grep -c 'tailed item ' $RSYSLOG_OUT_LOG)/1000 (expect less)."
shutdown_immediate
exit_test

