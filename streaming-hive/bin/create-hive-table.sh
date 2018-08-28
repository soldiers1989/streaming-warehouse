#!/usr/bin/env bash

bin_dir=$(cd `dirname $0`; pwd)
home_dir="${bin_dir}/.."
lib_dir="${home_dir}/lib/"
conf_dir="${home_dir}/conf"
hadoop_conf_dir="/etc/hadoop/conf"

CLASS_PATH="${conf_dir}:${lib_dir}/*:$hadoop_conf_dir";

cmd="java -Xmx1G -Dapp.config.file=$conf_dir/program.properties -cp $CLASS_PATH com.tree.finance.bigdata.hive.streaming.cli.CreateTools $@"

exec ${cmd}