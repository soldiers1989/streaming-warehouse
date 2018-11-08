#!/usr/bin/env bash

#!/usr/bin/env bash

db_user=hive2
db_pwd=hive2stream
db_host=hadoop3
db_name=hive2

if [ $@ > 1 ]; then
  ts=$1
else
  let ts=`date -d "1 hour ago" +%s`*1000
fi

delete_component_sql="delete from TXN_COMPONENTS where tc_txnid in (select TXN_ID from TXNS where TXN_STARTED < ${ts} and TXN_STATE='o');"

delete_txn_sql="delete from TXNS where TXN_STARTED < ${ts} and TXN_STATE='o'"

delete_write_set_sql="delete from WRITE_SET where WS_TXNID < (select (NTXN_NEXT-10000) from NEXT_TXN_ID limit 1);"

mysql -h${db_host} -u${db_user} -p${db_pwd} -e "use ${db_name};${delete_component_sql}"
echo "cleaned txn_component"

mysql -h${db_host} -u${db_user} -p${db_pwd} -e "use ${db_name};${delete_txn_sql}"
echo "cleand open txns"

mysql -h${db_host} -u${db_user} -p${db_pwd} -e "use ${db_name};${delete_write_set_sql}"
echo "cleaned write set"