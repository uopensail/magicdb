#!/bin/bash

CURDIR=`pwd`
OS=`uname | tr '[A-Z]' '[a-z]'`
ARCH=`uname | tr '[A-Z]' '[a-z]'`

PROJECTNAME=magicdb
APPBIN=${CURDIR}/${PROJECTNAME}
APPBIN_RUN=${CURDIR}/${PROJECTNAME}
CONF_FILE=${CURDIR}/conf/config.toml

SUPERVISORDIR=/etc/supervisord.d/
if [ ! -w "${SUPERVISORDIR}" ]; then
	SUPERVISORDIR=/data/etc/supervisor
fi

function create_supervisor_ini() {
if [ x"${OS}" == x"darwin" ]; then
    SUPERVISORENV="DYLD_LIBRARY_PATH=${CURDIR}/"
else
    SUPERVISORENV="LD_LIBRARY_PATH=${CURDIR}/"
fi


STDOUT_LOG_FILE=/opt/logs/${PROJECTNAME}/stdout.log

cat > ${SUPERVISORDIR}/${PROJECTNAME}.ini <<EOF
[program:${PROJECTNAME}]
directory=${CURDIR}
command=${APPBIN_RUN} -config=${CONF_FILE} -log=/opt/logs/${PROJECTNAME}/
environment=${SUPERVISORENV}
autostart=false
autorestart=true
startsecs=1
startretries=3
user=root
redirect_stderr=true
stdout_logfile=${STDOUT_LOG_FILE}
EOF
}



function stop() {
	sudo supervisorctl stop ${PROJECTNAME}

	if [ $? -ne 0 ]; then
		echo "stop ${PROJECTNAME} fail"
		exit 1
	fi
}

function start() {
	sudo supervisorctl status ${PROJECTNAME} |grep RUNNING > /dev/null

	if [ $? -eq 0 ]; then
		echo "${PROJECTNAME} is already runing"
        cat ${SUPERVISORDIR}/${PROJECTNAME}.ini
		exit 1
	fi

	create_supervisor_ini && (sudo supervisorctl update) && (sudo supervisorctl start ${PROJECTNAME})
	if [ $? -ne 0 ]; then
		echo "start ${PROJECTNAME} fail"
		exit 1
	fi	
	wait_check_app_run
    echo "supervisorctl start ${PROJECTNAME} success"
}

function check_app_is_running() {
    ps_out=`ps -ef | grep $1 | grep -v 'grep'| grep -v $0`
    ret=$(echo $ps_out | grep "$1")
	if [[ "$ret" != "" ]]; then
        echo "runing"
    else
        echo "not_run"
    fi
}

function get_prome_port_from_yaml() {
    line_str=`cat ${CONF_FILE} | grep "prome_port" | tr -d " "`
    kv_arr=(${line_str//:/ })
    echo $kv_arr[1]
}

function check_prome_port_is_listening() {
    port=`get_prome_port_from_yaml`
    telent_succ=`echo -e "\n" | telnet 127.0.0.1 ${port} 2>/dev/null | grep Connected | wc -l`
    if [ ${telent_succ} -eq 1 ]; then
        echo "listening"
    else 
        echo "fail"
    fi
}

function wait_check_app_run(){
    echo -e "${PROJECTNAME} wait...\c"
    for i in {1...300}
    do 
        sleep 1s
        runing=`check_app_is_running ${PROJECTNAME}`
        if [ x${runing} == x"not_run" ]; then
            echo "supervisorctl run ${PROJECTNAME} fail"
            exit 1
        fi
        
        listening=`check_prome_port_is_listening`
        if [ x${listening} == x"listening" ]; then
            break
        fi
        echo -e ".\c"
    done
    echo -e "done\n"
}

function usage() {
    echo "usage: $0 [start|stop|restart]"
}

if [ $# -lt 1 ]; then
	usage
	exit 1
else
	if [ "$1"x == 'stop'x ] ; then
		stop
		echo "${PROJECTNAME} stop"
	elif [ "$1"x == 'start'x ] ; then
		start
		echo "${PROJECTNAME} start"
	elif [ "$1"x == 'restart'x ] ; then
		stop
		start
		echo "${PROJECTNAME} restart"
	else
		usage
		exit 1
	fi
fi

exit 0
