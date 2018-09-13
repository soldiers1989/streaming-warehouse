#! /usr/bin/env bash

operation=("createHiveTbl" "setCheckTime" "loadRecId")

if [ $# -eq 0 ]
then
 echo "supported operation: ${operation[@]}"
 exit
fi

op=$1
shift

if ! [[ "${operation[@]}" =~ $op ]]
then
  echo "supported operation: ${operation[@]}"
fi

main_class=""

if [ ${op} == "createHiveTbl" ] ; then
  main_class="com.tree.finance.bigdata.hive.streaming.cli.CreateTools"

elif [ ${op} == "loadRecId" ] ; then
  main_class="com.tree.finance.bigdata.hive.streaming.cli.RecordIdLoader"

elif [ ${op} == "setCheckTime" ] ; then
  main_class="com.tree.finance.bigdata.hive.streaming.cli.CheckTimeConfigTools"

fi


bin_dir=$(cd `dirname $0`; pwd)
home_dir="${bin_dir}/.."
lib_dir="${home_dir}/lib/"
log_dir="${home_dir}/log"
log_file="tools.log"
conf_dir="${home_dir}/conf"
hadoop_conf_dir="/etc/hadoop/conf"
HADOOP_CLASS_PATH=`hadoop classpath`
HBASE_CLASS_PATH=`hbase classpath`
CLASS_PATH="${conf_dir}:${lib_dir}/*:$hadoop_conf_dir:$HADOOP_CLASS_PATH:${HBASE_CLASS_PATH} "

DEBUG_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=12346"
JVM_OPTS="-Xmx1G -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${log_dir} "

if [ ! -e ${log_dir} ]; then
  mkdir -p ${log_dir}
fi

cmd="java ${JVM_OPTS} -Dapp.config.file=$conf_dir/program.properties -Dlog_file=${log_file} -Dlog_dir=${log_dir} -classpath $CLASS_PATH  ${main_class} $@"

exec ${cmd}
#nohup ${cmd} 1>&2> ${log_dir}/tools.out &