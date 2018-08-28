#!/usr/bin/env bash

bin_dir=$(cd `dirname $0`; pwd)
home_dir="${bin_dir}/.."
lib_dir="${home_dir}/lib/"
log_dir="${home_dir}/log"
conf_dir="${home_dir}/conf"
hadoop_conf_dir="/etc/hadoop/conf"

HADOOP_CLASS_PATH=`hadoop classpath`
HBASE_CLASS_PATH=`hbase classpath`
CLASS_PATH="${conf_dir}:${lib_dir}/*:$hadoop_conf_dir:$HADOOP_CLASS_PATH:${HBASE_CLASS_PATH} ";
DEBUG_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=12346"
JVM_OPTS="-Xmx1G -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${log_dir} "

if [ ! -e ${log_dir} ]; then
  mkdir -p ${log_dir}
fi

cmd="java ${JVM_OPTS} -Dapp.config.file=$conf_dir/program.properties -Dlog_dir=${log_dir} -classpath $CLASS_PATH  com.tree.finance.bigdata.hive.streaming.StreamingWarehouse"
cmd="java -Xmx1G -Dapp.config.file=$conf_dir/program.properties -Dlog_dir=${log_dir} -classpath $CLASS_PATH ${DEBUG_OPTS} com.tree.finance.bigdata.hive.streaming.StreamingWarehouse"
#exec ${cmd}
nohup ${cmd} 1>&2> ${log_dir}/std.out &