
CREATE TABLE src_json_Tbl (
    a int,
    b bigint,
    c smallint,
    d tinyint,
    f float,
    g double,
    h boolean,
    e Timestamp,
    r ROW<x int, y float, z string>,
    y ARRAY<string>,
    m MAP<string, string>
 ) WITH (
    'connector'='kafka',
    'topic' = 'test_in_1',
    'properties.bootstrap.servers' = 'sg-test-kafka-conn1.bigdata.bigo.inner:9093,sg-test-kafka-conn2.bigdata.bigo.inner:9093,sg-test-kafka-conn3.bigdata.bigo.inner:9093',
    'properties.group.id' = 'abcd123',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
 );

 create table snk_json_Tbl(
    a int,
    b bigint,
    c smallint,
    d tinyint,
    f float,
    g double,
    h boolean,
    e Timestamp,
    r ROW<x int, y float, z string>,
    y ARRAY<string>,
    m MAP<string, string>
    ) with('connector' = 'print');

 insert into snk_json_Tbl select a,b,c,d,f,g,h,e,r,y,m from src_json_Tbl;

-- The test data as below
-- {"a":123, "b": 1234545678, "c":123, "d": 5, "f": 12.3448, "g":100.234455955, "h":false, "e":"2025-05-06 11:30:00", "r": {"x":111, "y":12.33, "z":"z1234"}, "y":["y123", "y124", "y125"], "m":{"cc":"c134", "dd":"d123", "ee":"e131"}}