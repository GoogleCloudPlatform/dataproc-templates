CREATE TABLE test_db.employee ( NAME VARCHAR(30), salary DECIMAL(20), destination CHAR(2)) PARTITIONED BY (eid INT) TBLPROPERTIES ('comment'='Test table for HIVE to GCS template');
INSERT INTO test_db.employee VALUES ('a', 5000, 'US', 1), ('b',5000, 'US', 2), ('c',5000, 'US', 3);
