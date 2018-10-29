#!/usr/bin/env bash

bin_dir=$(cd `dirname $0`; pwd)
home_dir="${bin_dir}/.."
lib_dir="${home_dir}/lib/"
log_dir="/data0/log/streaming-hive"
log_file="program.log"
conf_dir="${home_dir}/conf"
hadoop_conf_dir="/etc/hadoop/conf"

main_class="com.tree.finance.bigdata.hive.streaming.StreamingWarehouse"
HADOOP_CLASS_PATH=`hadoop classpath`
HBASE_CLASS_PATH=`hbase classpath`
CLASS_PATH="${conf_dir}:${lib_dir}/*:$hadoop_conf_dir:$HADOOP_CLASS_PATH:${HBASE_CLASS_PATH} "

export HADOOP_USER_NAME="hive"

DEBUG_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=12346"
JVM_OPTS="-Xmx4G -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${log_dir} -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+DisableExplicitGC "
GC_LOG_OPTS=" -Xloggc:$log_dir/gc.log -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=100M "


if [ ! -e ${log_dir} ]; then
  mkdir -p ${log_dir}
fi

pid=`ps -ef | grep com.tree.finance.bigdata.hive.streaming.StreamingWarehouse |grep -v "grep" |awk '{print($2)}'`
if [ "" != ${pid}"" ]; then
  echo "StreamingWarehouse already started"
  exit
fi

if [ $1"_x" = "debug_x" ]
then
  echo "start as debug mode"
  JVM_OPTS="${JVM_OPTS} ${DEBUG_OPTS} "
fi

cmd="java ${JVM_OPTS} ${GC_LOG_OPTS} -Dapp.config.file=$conf_dir/program.properties -Dlog_file=${log_file} -Dlog_dir=${log_dir} -classpath $CLASS_PATH  ${main_class} $@"

nohup ${cmd} 1>&2> ${log_dir}/std.out &

pid=`ps -ef | grep com.tree.finance.bigdata.hive.streaming.StreamingWarehouse |grep -v "grep" |awk '{print($2)}'`
if [ "" != ${pid}"" ]; then
  echo "${pid}" >> ${log_dir}/pid.history
fi