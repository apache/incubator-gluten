CREATE TABLE srcTbl (id INT, price INT, name STRING) WITH ('connector'='datagen');
CREATE TABLE snkTbl (id INT, price INT) WITH ('connector'='blackhole');
INSERT INTO snkTbl SELECT id, price FROM srcTbl WHERE price > 10;
