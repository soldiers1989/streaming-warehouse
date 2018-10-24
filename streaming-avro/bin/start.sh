#!/usr/bin/env bash

bin_dir=$(cd `dirname $0`; pwd)
home_dir="${bin_dir}/.."
lib_dir="${home_dir}/lib/"
log_dir="/data0/log/streaming-avro/"
log_file="program.log"
conf_dir="${home_dir}/conf"
hadoop_conf_dir="/etc/hadoop/conf"

main_class="com.tree.finance.bigdata.kafka.connect.sink.fs.StreamingPioneer"
HADOOP_CLASS_PATH=`hadoop classpath`
CLASS_PATH="${conf_dir}:${lib_dir}/*:$hadoop_conf_dir:$HADOOP_CLASS_PATH: "

export HADOOP_USER_NAME="hive"

DEBUG_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=12346"
JVM_OPTS="-Xmx6G -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${log_dir} -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+DisableExplicitGC "
GC_LOG_OPTS=" -Xloggc:$log_dir/gc.log -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=100M "

if [ ! -e ${log_dir} ]; then
  mkdir -p ${log_dir}
fi

pid=`ps -ef | grep com.tree.finance.bigdata.kafka.connect.sink.fs.StreamingPioneer |grep -v "grep" |awk '{print($2)}'`
if [ "" != ${pid}"" ]; then
  echo "StreamingPioneer already started"
  exit
fi

if [ $1"_x" = "debug_x" ]
then
  echo "start as debug mode"
  JVM_OPTS="${JVM_OPTS} ${DEBUG_OPTS} "
fi

cmd="java ${JVM_OPTS} ${GC_LOG_OPTS} -Dpioneer.conf.file=$conf_dir/pioneer.yaml -Dlog_file=${log_file} -Dlog_dir=${log_dir} -classpath $CLASS_PATH  ${main_class} $@"

#exec ${cmd}
nohup ${cmd} 1>&2>> ${log_dir}/std.out &