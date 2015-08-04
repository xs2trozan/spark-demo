CREATE KEYSPACE IF NOT EXISTS  test_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };

CREATE TABLE IF NOT EXISTS test_keyspace.data (
  sensor_id int,
  collected_at timestamp,
  volts float,
  PRIMARY KEY (sensor_id, collected_at)
) WITH COMPACT STORAGE;

INSERT INTO test_keyspace.data (sensor_id,collected_at,volts) 
VALUES (1,dateof(now()),1.2);

select * from test_keyspace.data