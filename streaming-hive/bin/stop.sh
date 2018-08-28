#!/usr/bin/env bash
pid=`ps -ef | grep com.tree.finance.bigdata.hive.streaming.StreamingWarehouse |grep -v "grep" |awk '{print($2)}'`
if [ "" == ${pid}"" ]; then
  echo "StreamingWarehouse not started"
  exit
fi

kill ${pid}
echo -e "stop StreamingWarehouse...\c"
while ( kill -0 ${pid} > /dev/null 2>&1 )
do
  echo -e ".\c"
  sleep 1
done
echo -e "\nstoped StreamingWarehouse"

