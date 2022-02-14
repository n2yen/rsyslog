#!/bin/bash
# This file is part of the rsyslog project, released under ASL 2.0

#  Starting actual testbench
. ${srcdir:=.}/diag.sh init

export NUMMESSAGES=50000

port="$(get_free_port)"
#omhttp_start_server $port --fail-every 100
#omhttp_start_server $port --fail-every 100 --delay-response-at 100 --delay-secs 5
omhttp_start_server $port --fail-every 100 --fail-with-delay-secs 3

generate_conf
add_conf '
module(load="../contrib/omhttp/.libs/omhttp")

main_queue(queue.dequeueBatchSize="2048")

template(name="tpl" type="string"
	 string="{\"msgnum\":\"%msg:F,58:2%\"}")

# Echo message as-is for retry
template(name="tpl_echo" type="string" string="%msg%")

ruleset(name="ruleset_omhttp_retry") {
    action(
        name="action_omhttp"
        action.resumeInterval="1"
	      action.resumeIntervalMax="1"
        type="omhttp"
        errorfile="'$RSYSLOG_DYNNAME/omhttp.error.log'"
        template="tpl_echo"

        server="localhost"
        serverport="'$port'"
        restpath="my/endpoint"
        restpathtimeout="1000"
        batch="on"
        batch.maxsize="100"
        batch.format="jsonarray"

        retry="on"
        retry.ruleset="ruleset_omhttp_retry"

        # Auth
        usehttps="off"

        # senderthread tests
        senderthread="on"
        senderthread.maxconnections="2"
    ) & stop
}

ruleset(name="ruleset_omhttp") {
    action(
        name="action_omhttp"
        action.resumeInterval="1"
	      action.resumeIntervalMax="1"
        type="omhttp"
        errorfile="'$RSYSLOG_DYNNAME/omhttp.error.log'"
        template="tpl"

        server="localhost"
        serverport="'$port'"
        restpath="my/endpoint"
        restpathtimeout="1000"
        batch="on"
        batch.maxsize="100"
        batch.format="jsonarray"

        retry="on"
        retry.ruleset="ruleset_omhttp_retry"

        # Auth
        usehttps="off"
        # senderthread tests
        senderthread="on"
        senderthread.maxconnections="2"
    ) & stop
}

if $msg contains "msgnum:" then
    call ruleset_omhttp
'
startup
injectmsg
shutdown_when_empty
wait_shutdown
omhttp_get_data $port my/endpoint jsonarray
omhttp_stop_server
export SEQ_CHECK_OPTIONS='-d'
seq_check
exit_test
