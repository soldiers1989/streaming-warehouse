#!/usr/bin/env bash

pid=`ps -ef | grep com.tree.finance.bigdata.hive.streaming.StreamingWarehouse |grep -v "grep" |awk '{print($2)}'`
if [ "" == ${pid}"" ]; then
  echo "StreamingPioneer not started"
  exit
fi

nc_cmd='nc'
shutdown_port=34185

echo "ShutdownKey-StreamingWarehouse" | ${nc_cmd} -v localhost ${shutdown_port}