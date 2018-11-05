#!/usr/bin/env bash

db_user=hive2
db_pwd=hive2stream

if [ $@ > 1 ]; then
  ts=$1
else
  let ts=`date -d "1 hour ago" +%s`*1000
fi

delete_component_sql="delete from txn_components where tc_txnid in (select TXN_ID from txns where TXN_STARTED < ${ts} and TXN_STATE='o');"

delete_txn_sql="delete from txns where TXN_STARTED < ${ts} and TXN_STATE='o'"

delete_write_set_sql="delete from write_set where WS_TXNID < (select (NTXN_NEXT-10000) from next_txn_id limit 1);"

mysql -hhadoop3 -uhive2 -phive2stream -e "use hive2;${delete_component_sql}"
echo "cleaned txn_component"

mysql -hhadoop3 -uhive2 -phive2stream -e "use hive2;${delete_txn_sql}"
echo "cleand open txns"

mysql -hhadoop3 -uhive2 -phive2stream -e "use hive2;${delete_write_set_sql}"
echo "cleaned write set"