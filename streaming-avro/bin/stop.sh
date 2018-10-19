#!/usr/bin/env bash

pid=`ps -ef | grep com.tree.finance.bigdata.kafka.connect.sink.fs.StreamingPioneer |grep -v "grep" |awk '{print($2)}'`
if [ "" == ${pid}"" ]; then
  echo "StreamingPioneer not started"
  exit
fi

nc_cmd='nc'
shutdown_port=8888

echo "ShutdownKey-StreamingPioneer" | ${nc_cmd} -v localhost ${shutdown_port}