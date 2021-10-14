#!/bin/bash
# This file is part of the rsyslog project, released under ASL 2.0

#  Starting actual testbench
. ${srcdir:=.}/diag.sh init

export NUMMESSAGES=10000
#export NUMMESSAGES=5
#export NUMMESSAGES=20
#export NUMMESSAGES=100
#export NUMMESSAGES=500
#export NUMMESSAGES=1000
#export NUMMESSAGES=2000

port="$(get_free_port)"
omhttp_start_server $port

generate_conf
add_conf '
template(name="tpl" type="string"
	 string="{\"msgnum\":\"%msg:F,58:2%\"}")

module(load="../contrib/omhttp/.libs/omhttp")

if $msg contains "msgnum:" then
	action(
		# Payload
		name="my_http_action"
		type="omhttp"
		errorfile="'$RSYSLOG_DYNNAME/omhttp.error.log'"
		template="tpl"

		server="localhost"
		serverport="'$port'"
		restpath="my/endpoint"
		batch="off"

		# Auth
		usehttps="off"
    )
'
startup
injectmsg
#sleep 2
wait_queueempty
echo "doing shutdown"
shutdown_when_empty
wait_shutdown
omhttp_get_data $port my/endpoint
omhttp_stop_server
seq_check
exit_test
