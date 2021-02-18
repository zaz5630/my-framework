CREATE TABLE hive (
`num`      INT        COMMENT '计算数值' ,
`ts`       BIGINT     COMMENT '数据上报时间',
`vin`      STRING     COMMENT '唯一标识',
`f1`       STRING     COMMENT 'f1',
`f2`       INT        COMMENT 'f2',
`f3`       DOUBLE     COMMENT 'f3'
)
PARTITIONED BY (dt STRING, hr STRING) STORED AS orc TBLPROPERTIES (
'sink.partition-commit.trigger'='partition-time' ,
'transactional' = 'false' ,
'partition.time-extractor.timestamp-pattern'='$dt $hr:00:00' ,
'sink.partition-commit.delay'='20 min',
'sink.partition-commit.policy.kind'='metastore,success-file')
