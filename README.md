
# 出于项目和个人隐私考虑，请您删除于我处(github账号：pecanNBU)fork的项目streaming-warehouse,谢谢配合，有任何问题请联系personalc@163.com
# 出于项目和个人隐私考虑，请您删除于我处(github账号：pecanNBU)fork的项目streaming-warehouse,谢谢配合,有任何问题请联系personalc@163.com

### Streaming-Warehouse
#### Introduction
2nd generation of Data Warehouse, with improved processing latency and data quality.
#### Data FLow
Mysql -> Kafka -> Intermediate Avro Files -> Hive ACID Files
#### Modules
##### 1. streaming-avro
cache DML binglog events(insert update delete), and write events as avro files every 5min.
##### 2. streaming-api
Customized hive streaming-mutation apis.
```
includes:
1. convert AVRO to ACID ORC Files.
2. adjust & convert compatible Data Types.
3. implement transaction fitrues.
4. batch put/get recordId to/from HBase.
```
used in streaming-mutation program and data repaiment.
##### 3. streaming-hive
convert and put intermediate avro files to warehouse.
```
1. insert intermediate avro files to hive.
2. process Data duplication and Data delay.
```
##### 4. streaming-tools
maintenance tools
```
includes
1. load recordId to HBase.
2. data duplication.
3. hive table compact.
4. HBase record insight.
5. create hive transaction table from mysql.
```
