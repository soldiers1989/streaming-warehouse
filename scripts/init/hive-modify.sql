 use hive2;
 alter table HIVE_LOCKS add UNIQUE KEY `hive_locks_uniq_idx` (`HL_DB`,`HL_TABLE`,`HL_PARTITION`);