DROP DATABASE IF EXISTS mydb1 CASCADE;
CREATE DATABASE mydb1;
USE mydb1;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1(a int, b int, c int, d int, e int) USING PARQUET;

INSERT INTO t1 VALUES(NULL,102,NULL,101,104);;

INSERT INTO t1 VALUES(107,106,108,109,105);;

INSERT INTO t1 VALUES(110,114,112,NULL,113);;

INSERT INTO t1 VALUES(116,119,117,115,NULL);;

INSERT INTO t1 VALUES(123,122,124,NULL,121);;

INSERT INTO t1 VALUES(127,128,129,126,125);;

INSERT INTO t1 VALUES(132,134,131,133,130);;

INSERT INTO t1 VALUES(138,136,139,135,137);;

INSERT INTO t1 VALUES(144,141,140,142,143);;

INSERT INTO t1 VALUES(145,149,146,NULL,147);;

INSERT INTO t1 VALUES(151,150,153,NULL,NULL);;

INSERT INTO t1 VALUES(155,157,159,NULL,158);;

INSERT INTO t1 VALUES(161,160,163,164,162);;

INSERT INTO t1 VALUES(167,NULL,168,165,166);;

INSERT INTO t1 VALUES(171,170,172,173,174);;

INSERT INTO t1 VALUES(177,176,179,NULL,175);;

INSERT INTO t1 VALUES(181,180,182,183,184);;

INSERT INTO t1 VALUES(187,188,186,189,185);;

INSERT INTO t1 VALUES(190,194,193,192,191);;

INSERT INTO t1 VALUES(199,197,198,196,195);;

INSERT INTO t1 VALUES(NULL,202,203,201,204);;

INSERT INTO t1 VALUES(208,NULL,NULL,206,207);;

INSERT INTO t1 VALUES(214,210,213,212,211);;

INSERT INTO t1 VALUES(218,215,216,217,219);;

INSERT INTO t1 VALUES(223,221,222,220,224);;

INSERT INTO t1 VALUES(226,227,228,229,225);;

INSERT INTO t1 VALUES(234,231,232,230,233);;

INSERT INTO t1 VALUES(237,236,239,NULL,238);;

INSERT INTO t1 VALUES(NULL,244,240,243,NULL);;

INSERT INTO t1 VALUES(246,248,247,249,245);;

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1;
----

SELECT e,
       abs(a),
       b,
       abs(b-c),
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a+b*2,
       a+b*2+c*3+d*4
  FROM t1
 WHERE a>b;
----

SELECT abs(b-c),
       b,
       a+b*2+c*3+d*4
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0
   AND (a>b-2 AND a<b+2);
----

SELECT a,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a+b*2+c*3+d*4+e*5,
       d
  FROM t1
 WHERE a IS NULL;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       d-e
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130
    OR (a>b-2 AND a<b+2);
----

SELECT a+b*2+c*3,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       b,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a+b*2+c*3+d*4+e*5
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150
    OR b IS NOT NULL;
----

SELECT abs(b-c)
  FROM t1;
----

SELECT a+b*2
  FROM t1
 WHERE (a>b-2 AND a<b+2)
   AND b IS NOT NULL
   AND a>b;
----

SELECT c,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       d-e,
       c-d,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a+b*2+c*3+d*4+e*5,
       abs(a)
  FROM t1
 WHERE (c<=d-2 OR c>=d+2)
   AND coalesce(a,b,c,d,e)<>0
   AND c>d;
----

SELECT d,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       abs(b-c),
       c-d,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a+b*2,
       b-c
  FROM t1
 WHERE b>c
   AND (e>c OR e<d);
----

SELECT e,
       b,
       a,
       a+b*2+c*3+d*4+e*5,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a-b,
       (a+b+c+d+e)/5
  FROM t1
 WHERE b IS NOT NULL
   AND coalesce(a,b,c,d,e)<>0;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a+b*2+c*3,
       a-b,
       d-e
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150
   AND c>d
   AND c BETWEEN b-2 AND d+2;
----

SELECT a+b*2+c*3+d*4+e*5,
       abs(a),
       d,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130
   AND (e>c OR e<d);
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1;
----

SELECT d-e,
       c-d,
       (a+b+c+d+e)/5
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0
    OR b>c
    OR b IS NOT NULL;
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       b,
       a+b*2+c*3+d*4,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       d-e,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a-b
  FROM t1;
----

SELECT a+b*2+c*3+d*4+e*5,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       e
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0
    OR b>c
    OR a>b;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       d-e,
       a+b*2+c*3+d*4+e*5
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
   AND EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
   AND b IS NOT NULL;
----

SELECT a+b*2+c*3,
       a+b*2+c*3+d*4+e*5,
       e,
       a-b,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE c>d;
----

SELECT e,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       d-e,
       d,
       a+b*2+c*3+d*4
  FROM t1;
----

SELECT e,
       a,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
   AND d NOT BETWEEN 110 AND 150;
----

SELECT a+b*2,
       abs(b-c),
       c,
       d-e,
       a+b*2+c*3+d*4,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1
 WHERE (e>c OR e<d)
    OR (e>a AND e<b)
    OR a IS NULL;
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130
    OR c>d;
----

SELECT abs(b-c),
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       d,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE a IS NULL
    OR d NOT BETWEEN 110 AND 150;
----

SELECT a,
       c-d,
       a+b*2+c*3,
       a+b*2+c*3+d*4,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1
 WHERE (a>b-2 AND a<b+2)
    OR b IS NOT NULL
    OR EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT a-b,
       a+b*2+c*3+d*4+e*5,
       b,
       abs(a),
       a
  FROM t1
 WHERE b>c
   AND coalesce(a,b,c,d,e)<>0
   AND (e>a AND e<b);
----

SELECT abs(b-c),
       a+b*2+c*3,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a-b,
       a+b*2+c*3+d*4+e*5,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1;
----

SELECT a-b,
       b-c,
       abs(a),
       abs(b-c),
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       b,
       d-e
  FROM t1
 WHERE b IS NOT NULL
   AND e+d BETWEEN a+b-10 AND c+130
   AND c BETWEEN b-2 AND d+2;
----

SELECT a+b*2+c*3+d*4,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130
   AND EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a+b*2+c*3+d*4
  FROM t1;
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       abs(a),
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a+b*2,
       a,
       d-e,
       d
  FROM t1
 WHERE a IS NULL
   AND d>e;
----

SELECT (a+b+c+d+e)/5,
       c-d,
       e,
       a
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150
    OR (a>b-2 AND a<b+2);
----

SELECT (a+b+c+d+e)/5,
       d
  FROM t1
 WHERE b IS NOT NULL;
----

SELECT a+b*2+c*3,
       c-d
  FROM t1
 WHERE b IS NOT NULL
    OR coalesce(a,b,c,d,e)<>0
    OR d NOT BETWEEN 110 AND 150;
----

SELECT e,
       a+b*2,
       c,
       b-c
  FROM t1
 WHERE a>b;
----

SELECT c,
       b-c,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a+b*2+c*3+d*4+e*5,
       a,
       (a+b+c+d+e)/5
  FROM t1
 WHERE b>c
   AND (e>a AND e<b);
----

SELECT (a+b+c+d+e)/5,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       c-d,
       abs(a),
       d-e,
       b-c
  FROM t1;
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       abs(a),
       a+b*2+c*3+d*4+e*5,
       a-b,
       a+b*2,
       c
  FROM t1;
----

SELECT c-d,
       b-c,
       a+b*2+c*3,
       abs(a),
       a+b*2+c*3+d*4
  FROM t1;
----

SELECT d
  FROM t1
 WHERE c>d
   AND (c<=d-2 OR c>=d+2)
   AND (e>c OR e<d);
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END
  FROM t1
 WHERE b>c;
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END
  FROM t1;
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a+b*2+c*3+d*4,
       abs(b-c),
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       c-d,
       a+b*2,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130
    OR coalesce(a,b,c,d,e)<>0;
----

SELECT (a+b+c+d+e)/5,
       e,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1
 WHERE (a>b-2 AND a<b+2)
    OR (e>a AND e<b);
----

SELECT b-c,
       a+b*2+c*3,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a+b*2,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END
  FROM t1
 WHERE a IS NULL;
----

SELECT c,
       a+b*2+c*3+d*4+e*5,
       c-d
  FROM t1;
----

SELECT a+b*2,
       d,
       abs(b-c),
       e
  FROM t1
 WHERE (c<=d-2 OR c>=d+2)
   AND d>e;
----

SELECT c-d
  FROM t1
 WHERE c>d;
----

SELECT a-b,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE (c<=d-2 OR c>=d+2)
    OR c>d
    OR (e>c OR e<d);
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END
  FROM t1
 WHERE a IS NULL;
----

SELECT a+b*2,
       d-e,
       a-b,
       abs(a),
       (a+b+c+d+e)/5,
       c
  FROM t1
 WHERE (c<=d-2 OR c>=d+2)
   AND c BETWEEN b-2 AND d+2
   AND coalesce(a,b,c,d,e)<>0;
----

SELECT a+b*2+c*3,
       b,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130
    OR a>b;
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END
  FROM t1
 WHERE (e>c OR e<d);
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       d-e
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130
   AND (e>a AND e<b);
----

SELECT b-c,
       c
  FROM t1
 WHERE (e>a AND e<b);
----

SELECT d-e,
       a+b*2+c*3+d*4+e*5,
       a+b*2,
       a,
       b-c
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
   AND EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a+b*2,
       a+b*2+c*3,
       (a+b+c+d+e)/5,
       d-e
  FROM t1
 WHERE (a>b-2 AND a<b+2)
    OR b>c;
----

SELECT c-d
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       (a+b+c+d+e)/5,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       c-d
  FROM t1
 WHERE d>e
    OR a>b
    OR EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT e
  FROM t1;
----

SELECT a+b*2
  FROM t1
 WHERE c>d;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1;
----

SELECT c-d,
       a+b*2,
       d-e,
       a+b*2+c*3+d*4
  FROM t1
 WHERE b>c
   AND b IS NOT NULL
   AND (a>b-2 AND a<b+2);
----

SELECT a-b,
       abs(b-c),
       abs(a)
  FROM t1
 WHERE a>b
    OR EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
    OR d>e;
----

SELECT d-e,
       abs(a)
  FROM t1
 WHERE (e>c OR e<d);
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       (a+b+c+d+e)/5,
       b-c
  FROM t1;
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a+b*2+c*3,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a-b
  FROM t1
 WHERE a>b
   AND b>c
   AND (c<=d-2 OR c>=d+2);
----

SELECT a+b*2+c*3+d*4+e*5,
       abs(b-c),
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       b-c,
       a+b*2+c*3,
       a+b*2+c*3+d*4
  FROM t1
 WHERE c>d;
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       b-c,
       a+b*2
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
   AND a>b;
----

SELECT a-b,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a+b*2+c*3+d*4,
       d-e
  FROM t1
 WHERE (e>c OR e<d);
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE a>b
   AND coalesce(a,b,c,d,e)<>0;
----

SELECT a+b*2+c*3+d*4+e*5,
       abs(b-c),
       a+b*2
  FROM t1
 WHERE (a>b-2 AND a<b+2);
----

SELECT d-e,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       b-c,
       e,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       abs(b-c)
  FROM t1
 WHERE a IS NULL;
----

SELECT d,
       c-d,
       e
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150
    OR (e>a AND e<b)
    OR a IS NULL;
----

SELECT a+b*2+c*3+d*4,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       b-c
  FROM t1;
----

SELECT b,
       d,
       abs(a),
       abs(b-c),
       d-e
  FROM t1
 WHERE (e>a AND e<b)
    OR e+d BETWEEN a+b-10 AND c+130
    OR b IS NOT NULL;
----

SELECT a+b*2+c*3+d*4+e*5,
       a+b*2+c*3,
       c
  FROM t1
 WHERE b>c
   AND a>b
   AND e+d BETWEEN a+b-10 AND c+130;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       c-d,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a+b*2+c*3+d*4+e*5,
       a-b,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       b
  FROM t1;
----

SELECT d-e,
       abs(b-c)
  FROM t1
 WHERE b IS NOT NULL;
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE (e>c OR e<d);
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a,
       d,
       b-c,
       b,
       a+b*2+c*3+d*4,
       a+b*2+c*3
  FROM t1;
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1
 WHERE (a>b-2 AND a<b+2)
    OR (e>a AND e<b);
----

SELECT a+b*2+c*3+d*4+e*5,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       abs(b-c)
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150
    OR c BETWEEN b-2 AND d+2;
----

SELECT c,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130
    OR a>b;
----

SELECT b-c,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a+b*2+c*3+d*4
  FROM t1
 WHERE (a>b-2 AND a<b+2);
----

SELECT abs(b-c),
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       b,
       d,
       e,
       c,
       (a+b+c+d+e)/5
  FROM t1
 WHERE d>e
   AND (c<=d-2 OR c>=d+2)
   AND a>b;
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       e,
       d-e,
       (a+b+c+d+e)/5,
       a-b
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
   AND b IS NOT NULL
   AND c>d;
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a+b*2+c*3,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       e,
       d,
       c-d,
       a
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT (a+b+c+d+e)/5,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a+b*2,
       c-d,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END
  FROM t1
 WHERE (c<=d-2 OR c>=d+2)
    OR d>e;
----

SELECT d-e
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
   AND coalesce(a,b,c,d,e)<>0
   AND c BETWEEN b-2 AND d+2;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       d,
       b-c,
       c-d,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       b,
       abs(b-c)
  FROM t1;
----

SELECT a+b*2+c*3+d*4+e*5,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       (a+b+c+d+e)/5
  FROM t1
 WHERE (a>b-2 AND a<b+2)
   AND (e>a AND e<b)
   AND a>b;
----

SELECT c-d,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a+b*2+c*3,
       b
  FROM t1
 WHERE (a>b-2 AND a<b+2)
   AND d>e;
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       b,
       d-e,
       a+b*2+c*3+d*4
  FROM t1;
----

SELECT d,
       b-c,
       abs(b-c),
       b
  FROM t1
 WHERE c BETWEEN b-2 AND d+2;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       abs(a)
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
    OR e+d BETWEEN a+b-10 AND c+130;
----

SELECT b-c
  FROM t1
 WHERE a IS NULL
   AND (e>c OR e<d);
----

SELECT abs(b-c),
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       b-c,
       c-d,
       a+b*2,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1
 WHERE a>b;
----

SELECT d
  FROM t1
 WHERE (e>c OR e<d);
----

SELECT c-d,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
   AND e+d BETWEEN a+b-10 AND c+130
   AND (c<=d-2 OR c>=d+2);
----

SELECT a+b*2+c*3,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a+b*2,
       d,
       c
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
    OR a>b
    OR (a>b-2 AND a<b+2);
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       abs(b-c),
       d,
       a-b,
       a+b*2+c*3+d*4+e*5,
       a+b*2,
       abs(a)
  FROM t1;
----

SELECT a+b*2,
       d,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       d-e,
       a-b,
       a+b*2+c*3+d*4+e*5
  FROM t1;
----

SELECT abs(b-c),
       a+b*2+c*3+d*4,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a+b*2+c*3,
       c,
       e,
       d
  FROM t1
 WHERE a>b
    OR (e>c OR e<d)
    OR b IS NOT NULL;
----

SELECT a+b*2,
       a+b*2+c*3+d*4
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150
    OR b>c
    OR (a>b-2 AND a<b+2);
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a+b*2
  FROM t1
 WHERE b>c;
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       e,
       a+b*2,
       abs(a),
       c-d
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0
   AND c BETWEEN b-2 AND d+2;
----

SELECT b-c,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       d-e
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
   AND c>d
   AND a>b;
----

SELECT d-e,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       e,
       b-c,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a+b*2+c*3
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0;
----

SELECT a
  FROM t1
 WHERE (e>c OR e<d);
----

SELECT a+b*2+c*3,
       abs(b-c),
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a
  FROM t1
 WHERE b IS NOT NULL;
----

SELECT a+b*2+c*3,
       d-e,
       (a+b+c+d+e)/5
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0
   AND (e>c OR e<d)
   AND e+d BETWEEN a+b-10 AND c+130;
----

SELECT abs(a),
       abs(b-c),
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150;
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a-b,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       (a+b+c+d+e)/5,
       c
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
    OR a>b
    OR b IS NOT NULL;
----

SELECT abs(b-c)
  FROM t1
 WHERE b IS NOT NULL
   AND e+d BETWEEN a+b-10 AND c+130;
----

SELECT a+b*2+c*3+d*4+e*5,
       a+b*2,
       d,
       a+b*2+c*3+d*4,
       a-b,
       b-c,
       a
  FROM t1
 WHERE b>c
    OR a IS NULL;
----

SELECT a+b*2+c*3
  FROM t1
 WHERE a>b;
----

SELECT a+b*2,
       a,
       b,
       c,
       abs(b-c),
       abs(a),
       d
  FROM t1;
----

SELECT b-c,
       abs(a)
  FROM t1
 WHERE b IS NOT NULL
    OR e+d BETWEEN a+b-10 AND c+130
    OR c BETWEEN b-2 AND d+2;
----

SELECT c-d,
       b-c,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       c
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150
   AND (a>b-2 AND a<b+2)
   AND a>b;
----

SELECT a+b*2+c*3+d*4+e*5,
       d-e,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a-b,
       abs(b-c),
       d
  FROM t1
 WHERE (c<=d-2 OR c>=d+2)
    OR e+d BETWEEN a+b-10 AND c+130
    OR a>b;
----

SELECT abs(a),
       d,
       e,
       a+b*2+c*3+d*4,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       b-c,
       a
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
   AND EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT d-e,
       abs(b-c),
       a+b*2+c*3,
       a+b*2,
       a-b
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0;
----

SELECT a+b*2+c*3+d*4,
       b,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       d-e
  FROM t1
 WHERE (c<=d-2 OR c>=d+2)
    OR (e>c OR e<d)
    OR c>d;
----

SELECT (a+b+c+d+e)/5,
       c-d,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a+b*2+c*3,
       d-e
  FROM t1
 WHERE a>b
   AND (e>c OR e<d)
   AND a IS NULL;
----

SELECT a+b*2+c*3,
       a+b*2+c*3+d*4+e*5,
       d,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       abs(b-c)
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
   AND a IS NULL
   AND d>e;
----

SELECT d,
       b-c,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a+b*2+c*3+d*4,
       c,
       a
  FROM t1
 WHERE (c<=d-2 OR c>=d+2);
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a+b*2+c*3+d*4+e*5,
       a+b*2+c*3,
       b-c,
       e
  FROM t1;
----

SELECT a+b*2+c*3+d*4,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150
    OR d>e
    OR coalesce(a,b,c,d,e)<>0;
----

SELECT a+b*2+c*3+d*4,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a-b,
       c-d
  FROM t1
 WHERE (a>b-2 AND a<b+2)
   AND (c<=d-2 OR c>=d+2);
----

SELECT a+b*2+c*3+d*4+e*5,
       d-e,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a+b*2+c*3,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1;
----

SELECT c,
       b,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a+b*2+c*3,
       e,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1;
----

SELECT (a+b+c+d+e)/5,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       b
  FROM t1
 WHERE c>d;
----

SELECT abs(b-c),
       (a+b+c+d+e)/5,
       d
  FROM t1
 WHERE (e>a AND e<b)
   AND a IS NULL;
----

SELECT b
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130
    OR (c<=d-2 OR c>=d+2);
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a+b*2+c*3+d*4+e*5,
       c-d,
       b-c,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a+b*2+c*3+d*4
  FROM t1;
----

SELECT a-b,
       a+b*2+c*3+d*4+e*5,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       d
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
   AND d>e
   AND coalesce(a,b,c,d,e)<>0;
----

SELECT a+b*2+c*3+d*4,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
   AND d>e;
----

SELECT abs(a),
       (a+b+c+d+e)/5,
       a+b*2,
       c,
       b,
       d
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130
   AND (c<=d-2 OR c>=d+2);
----

SELECT d-e,
       a
  FROM t1
 WHERE b>c
   AND (e>c OR e<d)
   AND (a>b-2 AND a<b+2);
----

SELECT a+b*2+c*3+d*4+e*5,
       a+b*2+c*3+d*4,
       a+b*2+c*3,
       d-e,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a-b
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0
   AND d>e
   AND d NOT BETWEEN 110 AND 150;
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1;
----

SELECT a+b*2+c*3+d*4,
       d,
       d-e,
       a-b
  FROM t1;
----

SELECT d-e,
       a-b
  FROM t1;
----

SELECT abs(a),
       a+b*2+c*3,
       c-d
  FROM t1
 WHERE (c<=d-2 OR c>=d+2)
   AND b IS NOT NULL
   AND c>d;
----

SELECT a,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       d-e
  FROM t1
 WHERE a>b;
----

SELECT c-d,
       d-e,
       a-b,
       (a+b+c+d+e)/5,
       b,
       a,
       abs(b-c)
  FROM t1
 WHERE d>e
   AND b IS NOT NULL
   AND coalesce(a,b,c,d,e)<>0;
----

SELECT a,
       d,
       a-b,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1;
----

SELECT d,
       c,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a+b*2+c*3+d*4+e*5
  FROM t1;
----

SELECT b,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a+b*2+c*3+d*4+e*5,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a+b*2
  FROM t1
 WHERE a>b;
----

SELECT (a+b+c+d+e)/5,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       b
  FROM t1
 WHERE a>b;
----

SELECT abs(a),
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150;
----

SELECT a-b,
       b-c,
       abs(b-c)
  FROM t1;
----

SELECT c
  FROM t1
 WHERE (e>c OR e<d)
    OR a>b;
----

SELECT (a+b+c+d+e)/5,
       c-d,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a,
       c
  FROM t1
 WHERE (e>c OR e<d);
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1
 WHERE (a>b-2 AND a<b+2)
   AND d>e
   AND b>c;
----

SELECT a+b*2
  FROM t1
 WHERE (e>c OR e<d);
----

SELECT a+b*2+c*3,
       c,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a+b*2
  FROM t1
 WHERE d>e;
----

SELECT d
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130
    OR b>c
    OR c>d;
----

SELECT c,
       e,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       d-e,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END
  FROM t1
 WHERE a>b;
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       b-c,
       e
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150
    OR a>b;
----

SELECT b,
       d,
       a,
       c,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a+b*2+c*3+d*4+e*5,
       abs(a)
  FROM t1
 WHERE d>e
    OR a IS NULL;
----

SELECT a+b*2+c*3+d*4,
       c,
       c-d,
       b-c
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
    OR a>b
    OR b IS NOT NULL;
----

SELECT abs(b-c),
       b-c,
       a+b*2,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       abs(a)
  FROM t1;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a+b*2,
       c,
       d,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a-b
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130
   AND (c<=d-2 OR c>=d+2);
----

SELECT d-e,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1
 WHERE (e>c OR e<d);
----

SELECT abs(b-c),
       b,
       e
  FROM t1
 WHERE (a>b-2 AND a<b+2);
----

SELECT abs(b-c)
  FROM t1
 WHERE (e>a AND e<b)
   AND c>d;
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       (a+b+c+d+e)/5,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       b,
       c-d
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0;
----

SELECT (a+b+c+d+e)/5,
       a+b*2+c*3+d*4+e*5,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       abs(b-c)
  FROM t1
 WHERE (e>c OR e<d)
    OR coalesce(a,b,c,d,e)<>0
    OR EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT a-b,
       b,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       abs(a),
       a+b*2
  FROM t1;
----

SELECT a+b*2+c*3+d*4,
       abs(b-c),
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a+b*2+c*3,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE (e>c OR e<d)
    OR b IS NOT NULL;
----

SELECT c
  FROM t1
 WHERE b>c
    OR b IS NOT NULL
    OR (e>c OR e<d);
----

SELECT d-e,
       c-d,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a+b*2
  FROM t1;
----

SELECT a+b*2+c*3+d*4+e*5,
       d,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a+b*2+c*3+d*4,
       c,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1
 WHERE a IS NULL;
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1;
----

SELECT d-e
  FROM t1;
----

SELECT b-c,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       c-d,
       e,
       b
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130
    OR a IS NULL
    OR c BETWEEN b-2 AND d+2;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       abs(a),
       a+b*2,
       a
  FROM t1;
----

SELECT d,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       b-c,
       a+b*2+c*3+d*4
  FROM t1
 WHERE (e>c OR e<d)
   AND EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
   AND b>c;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a+b*2+c*3+d*4+e*5
  FROM t1;
----

SELECT a+b*2+c*3
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0
   AND d>e;
----

SELECT d-e,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a+b*2+c*3,
       a-b,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       abs(b-c)
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0
    OR c BETWEEN b-2 AND d+2
    OR (c<=d-2 OR c>=d+2);
----

SELECT abs(a),
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       d-e,
       (a+b+c+d+e)/5,
       b,
       a+b*2+c*3,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130;
----

SELECT e,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       c-d,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       d,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END
  FROM t1;
----

SELECT c-d,
       a-b,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END
  FROM t1;
----

SELECT b,
       a+b*2+c*3+d*4,
       c-d,
       a,
       b-c,
       a+b*2,
       d-e
  FROM t1
 WHERE (e>a AND e<b)
    OR b IS NOT NULL
    OR EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT a,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       abs(a),
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1;
----

SELECT abs(b-c),
       c-d,
       c,
       d-e,
       abs(a),
       b-c
  FROM t1
 WHERE b IS NOT NULL
    OR (e>c OR e<d)
    OR d NOT BETWEEN 110 AND 150;
----

SELECT a+b*2+c*3+d*4
  FROM t1;
----

SELECT c-d
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0;
----

SELECT a+b*2+c*3+d*4,
       b,
       a-b,
       e,
       c,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1
 WHERE a>b
    OR b IS NOT NULL;
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       d,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a+b*2,
       e,
       a+b*2+c*3+d*4+e*5
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150
    OR e+d BETWEEN a+b-10 AND c+130
    OR a>b;
----

SELECT d,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END
  FROM t1
 WHERE b>c
    OR EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT abs(b-c),
       e,
       b,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a+b*2+c*3+d*4
  FROM t1
 WHERE a>b
    OR e+d BETWEEN a+b-10 AND c+130
    OR d NOT BETWEEN 110 AND 150;
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       e,
       b-c,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1
 WHERE (e>c OR e<d);
----

SELECT c-d,
       d,
       a-b,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE c>d
    OR EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
    OR (e>a AND e<b);
----

SELECT a-b
  FROM t1
 WHERE c BETWEEN b-2 AND d+2;
----

SELECT d,
       a-b,
       c-d,
       a+b*2+c*3,
       e,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1
 WHERE (e>a AND e<b)
    OR a IS NULL;
----

SELECT a
  FROM t1;
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a+b*2,
       abs(a),
       a+b*2+c*3+d*4+e*5,
       d-e
  FROM t1;
----

SELECT a+b*2+c*3+d*4+e*5,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a,
       abs(b-c),
       abs(a),
       c-d
  FROM t1
 WHERE c BETWEEN b-2 AND d+2;
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       d-e,
       c-d
  FROM t1;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a-b,
       (a+b+c+d+e)/5,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       d,
       c-d
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0;
----

SELECT c,
       a+b*2,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       c-d
  FROM t1
 WHERE b>c
    OR c>d
    OR a>b;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       b-c,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       c,
       e,
       abs(a),
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1
 WHERE a IS NULL
    OR (e>a AND e<b);
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a+b*2+c*3,
       b-c,
       abs(a),
       a-b,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1;
----

SELECT e,
       a+b*2+c*3+d*4+e*5,
       d
  FROM t1;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       e,
       b-c,
       d-e,
       abs(a)
  FROM t1
 WHERE (c<=d-2 OR c>=d+2);
----

SELECT d,
       abs(a),
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1
 WHERE a IS NULL
   AND a>b;
----

SELECT b-c,
       c-d,
       d,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a+b*2+c*3+d*4
  FROM t1
 WHERE (c<=d-2 OR c>=d+2);
----

SELECT a,
       a+b*2+c*3,
       a+b*2+c*3+d*4,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       b
  FROM t1
 WHERE b IS NOT NULL
    OR d>e;
----

SELECT a+b*2+c*3+d*4+e*5,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       b,
       abs(b-c)
  FROM t1
 WHERE (c<=d-2 OR c>=d+2)
   AND c>d
   AND a IS NULL;
----

SELECT a+b*2+c*3+d*4,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       e
  FROM t1;
----

SELECT d,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a,
       c,
       e,
       a+b*2+c*3
  FROM t1;
----

SELECT (a+b+c+d+e)/5,
       abs(a),
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a+b*2+c*3+d*4+e*5
  FROM t1;
----

SELECT c-d,
       abs(b-c),
       b,
       abs(a),
       a,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1;
----

SELECT a+b*2+c*3+d*4,
       e,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a-b,
       c-d,
       abs(a)
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130;
----

SELECT (a+b+c+d+e)/5
  FROM t1;
----

SELECT a+b*2,
       c-d,
       a+b*2+c*3+d*4+e*5,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE (e>a AND e<b)
    OR (c<=d-2 OR c>=d+2)
    OR c BETWEEN b-2 AND d+2;
----

SELECT e,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1;
----

SELECT c-d,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE a IS NULL;
----

SELECT d,
       c-d
  FROM t1;
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a
  FROM t1
 WHERE b>c
   AND a>b
   AND coalesce(a,b,c,d,e)<>0;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       (a+b+c+d+e)/5,
       e,
       a+b*2,
       c-d,
       a,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1;
----

SELECT e,
       d-e,
       a+b*2+c*3+d*4,
       c,
       d,
       a-b,
       abs(b-c)
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130;
----

SELECT abs(b-c),
       d,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1
 WHERE (c<=d-2 OR c>=d+2);
----

SELECT c-d,
       a+b*2+c*3,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       e
  FROM t1
 WHERE c>d
    OR a IS NULL
    OR (e>c OR e<d);
----

SELECT abs(b-c),
       (a+b+c+d+e)/5,
       b-c,
       a+b*2
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130
   AND d NOT BETWEEN 110 AND 150;
----

SELECT (a+b+c+d+e)/5,
       d,
       a+b*2+c*3+d*4+e*5,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a+b*2+c*3
  FROM t1
 WHERE b IS NOT NULL;
----

SELECT c,
       a+b*2+c*3,
       b-c,
       a-b
  FROM t1;
----

SELECT abs(a)
  FROM t1
 WHERE (e>c OR e<d)
   AND d>e;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a+b*2+c*3+d*4,
       a-b,
       c-d
  FROM t1;
----

SELECT a+b*2+c*3+d*4,
       abs(b-c),
       d-e,
       abs(a),
       a,
       e,
       c-d
  FROM t1;
----

SELECT c
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
   AND (c<=d-2 OR c>=d+2)
   AND (e>c OR e<d);
----

SELECT a+b*2,
       (a+b+c+d+e)/5,
       c,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       abs(b-c),
       e
  FROM t1
 WHERE a>b
    OR a IS NULL
    OR b IS NOT NULL;
----

SELECT a+b*2+c*3,
       d-e,
       (a+b+c+d+e)/5,
       a+b*2+c*3+d*4+e*5
  FROM t1;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a+b*2+c*3+d*4+e*5,
       abs(b-c),
       a+b*2,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1
 WHERE b>c;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1
 WHERE c BETWEEN b-2 AND d+2;
----

SELECT d-e
  FROM t1
 WHERE (e>a AND e<b);
----

SELECT b,
       c-d,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150;
----

SELECT d-e,
       d,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
   AND (e>a AND e<b)
   AND (a>b-2 AND a<b+2);
----

SELECT d-e,
       abs(b-c),
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       abs(a)
  FROM t1
 WHERE (c<=d-2 OR c>=d+2)
   AND d NOT BETWEEN 110 AND 150
   AND d>e;
----

SELECT b,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       (a+b+c+d+e)/5,
       a+b*2+c*3
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0
   AND (e>a AND e<b);
----

SELECT d,
       d-e,
       a,
       b-c,
       e
  FROM t1
 WHERE (e>c OR e<d)
    OR a IS NULL;
----

SELECT a+b*2+c*3+d*4,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a+b*2+c*3+d*4+e*5
  FROM t1;
----

SELECT d-e,
       d,
       c,
       b-c
  FROM t1
 WHERE a IS NULL
   AND b>c;
----

SELECT abs(a),
       a+b*2,
       a+b*2+c*3,
       c-d,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1
 WHERE c BETWEEN b-2 AND d+2;
----

SELECT a+b*2+c*3+d*4+e*5,
       a+b*2+c*3+d*4,
       abs(b-c)
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0;
----

SELECT c-d
  FROM t1;
----

SELECT abs(a),
       a+b*2+c*3,
       a+b*2,
       c,
       d,
       a-b,
       a+b*2+c*3+d*4+e*5
  FROM t1
 WHERE b IS NOT NULL;
----

SELECT a,
       a-b,
       a+b*2,
       a+b*2+c*3,
       (a+b+c+d+e)/5
  FROM t1;
----

SELECT d,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0;
----

SELECT a,
       a+b*2+c*3,
       d-e
  FROM t1
 WHERE d>e
   AND b IS NOT NULL;
----

SELECT b,
       d-e,
       (a+b+c+d+e)/5,
       d,
       a
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130;
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       c-d,
       abs(b-c),
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       e
  FROM t1
 WHERE a>b
    OR b>c
    OR c BETWEEN b-2 AND d+2;
----

SELECT d,
       b-c,
       a+b*2+c*3,
       abs(a),
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       b
  FROM t1
 WHERE (e>a AND e<b)
    OR d>e
    OR (c<=d-2 OR c>=d+2);
----

SELECT a+b*2
  FROM t1
 WHERE (c<=d-2 OR c>=d+2)
   AND e+d BETWEEN a+b-10 AND c+130;
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       c,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       abs(b-c),
       (a+b+c+d+e)/5
  FROM t1
 WHERE d>e
    OR (c<=d-2 OR c>=d+2);
----

SELECT d,
       a-b,
       b-c,
       c
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
   AND a>b;
----

SELECT a+b*2+c*3+d*4,
       e,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       c,
       abs(a),
       abs(b-c),
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
    OR b>c;
----

SELECT abs(b-c),
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1
 WHERE a>b;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1;
----

SELECT a+b*2+c*3+d*4,
       c-d
  FROM t1
 WHERE c>d;
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       e
  FROM t1
 WHERE c BETWEEN b-2 AND d+2;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       abs(b-c)
  FROM t1
 WHERE b IS NOT NULL
    OR a>b;
----

SELECT a-b,
       a+b*2+c*3+d*4+e*5,
       a+b*2
  FROM t1
 WHERE c>d
   AND b>c
   AND (c<=d-2 OR c>=d+2);
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       d-e,
       a+b*2+c*3+d*4,
       a+b*2+c*3,
       a-b
  FROM t1
 WHERE d>e
   AND a>b
   AND b>c;
----

SELECT abs(b-c)
  FROM t1;
----

SELECT abs(a),
       d-e,
       a+b*2+c*3
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
   AND (a>b-2 AND a<b+2)
   AND (e>c OR e<d);
----

SELECT b,
       a+b*2,
       c-d,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       abs(a)
  FROM t1
 WHERE b>c
    OR (c<=d-2 OR c>=d+2)
    OR d NOT BETWEEN 110 AND 150;
----

SELECT e,
       b,
       (a+b+c+d+e)/5
  FROM t1
 WHERE (e>a AND e<b)
    OR b>c;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       d-e,
       abs(a),
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a+b*2+c*3+d*4,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END
  FROM t1
 WHERE (e>a AND e<b)
    OR (e>c OR e<d);
----

SELECT d-e,
       a-b,
       b,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a+b*2,
       b-c,
       d
  FROM t1
 WHERE a>b
    OR (a>b-2 AND a<b+2);
----

SELECT abs(a),
       abs(b-c),
       c-d,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a+b*2+c*3+d*4+e*5
  FROM t1
 WHERE c>d
   AND (a>b-2 AND a<b+2)
   AND b>c;
----

SELECT a+b*2+c*3+d*4+e*5,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       abs(b-c),
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a+b*2,
       a-b
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150
   AND c>d;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       abs(b-c),
       e,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       b-c
  FROM t1;
----

SELECT a,
       c
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
   AND a IS NULL
   AND (a>b-2 AND a<b+2);
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1
 WHERE c>d;
----

SELECT a+b*2+c*3+d*4+e*5,
       b-c,
       b,
       a,
       c,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       (a+b+c+d+e)/5
  FROM t1;
----

SELECT abs(b-c),
       d,
       c-d,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       b-c
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1
 WHERE c>d
    OR c BETWEEN b-2 AND d+2;
----

SELECT b-c,
       c-d,
       d-e,
       (a+b+c+d+e)/5,
       d
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0;
----

SELECT b,
       e,
       a+b*2+c*3+d*4+e*5,
       d-e,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1;
----

SELECT a+b*2+c*3+d*4+e*5,
       c,
       c-d,
       a-b,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       (a+b+c+d+e)/5,
       a+b*2+c*3
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150
   AND d>e
   AND coalesce(a,b,c,d,e)<>0;
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       b,
       a+b*2,
       abs(b-c),
       abs(a),
       b-c,
       d
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
   AND (e>a AND e<b);
----

SELECT a
  FROM t1
 WHERE b IS NOT NULL
   AND d>e
   AND e+d BETWEEN a+b-10 AND c+130;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       d-e,
       b-c,
       a,
       c,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END
  FROM t1;
----

SELECT d-e,
       abs(b-c),
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a-b
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
    OR b IS NOT NULL
    OR a>b;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       d
  FROM t1
 WHERE d>e
    OR (e>c OR e<d)
    OR coalesce(a,b,c,d,e)<>0;
----

SELECT a-b
  FROM t1
 WHERE (e>a AND e<b);
----

SELECT a-b,
       (a+b+c+d+e)/5,
       a,
       a+b*2,
       e
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT c,
       c-d,
       a,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1;
----

SELECT (a+b+c+d+e)/5,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a+b*2+c*3,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       d
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0;
----

SELECT e,
       b,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       c-d,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1;
----

SELECT (a+b+c+d+e)/5,
       a,
       a+b*2+c*3,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       abs(b-c),
       d
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0
   AND (e>c OR e<d);
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1
 WHERE d>e
   AND a>b
   AND a IS NULL;
----

SELECT (a+b+c+d+e)/5,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a+b*2+c*3,
       d
  FROM t1
 WHERE (e>a AND e<b)
    OR EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
    OR a>b;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a+b*2+c*3,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       e,
       b-c,
       c-d
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
    OR c BETWEEN b-2 AND d+2;
----

SELECT abs(b-c),
       b-c
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
   AND coalesce(a,b,c,d,e)<>0
   AND (e>c OR e<d);
----

SELECT b-c,
       (a+b+c+d+e)/5
  FROM t1
 WHERE c BETWEEN b-2 AND d+2;
----

SELECT a-b,
       d-e,
       b-c,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a+b*2+c*3+d*4,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       b-c,
       d,
       abs(b-c),
       e,
       a+b*2+c*3+d*4+e*5
  FROM t1;
----

SELECT a+b*2
  FROM t1
 WHERE (e>a AND e<b);
----

SELECT a+b*2+c*3,
       c,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
   AND c BETWEEN b-2 AND d+2
   AND (c<=d-2 OR c>=d+2);
----

SELECT a,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       abs(b-c),
       c-d
  FROM t1
 WHERE a>b;
----

SELECT c-d,
       a+b*2+c*3,
       d-e,
       a+b*2+c*3+d*4,
       b-c,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1
 WHERE b IS NOT NULL
    OR (e>a AND e<b)
    OR (e>c OR e<d);
----

SELECT c-d,
       a,
       a+b*2+c*3,
       a-b
  FROM t1;
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a+b*2+c*3+d*4,
       a+b*2+c*3,
       e,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE a IS NULL
    OR c BETWEEN b-2 AND d+2
    OR a>b;
----

SELECT e
  FROM t1
 WHERE c>d
   AND coalesce(a,b,c,d,e)<>0
   AND a>b;
----

SELECT d-e,
       a,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       b-c,
       a+b*2+c*3,
       c
  FROM t1
 WHERE (a>b-2 AND a<b+2);
----

SELECT abs(a),
       d,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       e
  FROM t1
 WHERE (a>b-2 AND a<b+2)
    OR coalesce(a,b,c,d,e)<>0;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a+b*2+c*3+d*4,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END
  FROM t1
 WHERE (c<=d-2 OR c>=d+2)
    OR a>b;
----

SELECT a,
       e,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       abs(b-c)
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
    OR (e>a AND e<b);
----

SELECT d-e,
       c,
       (a+b+c+d+e)/5,
       a+b*2+c*3+d*4+e*5,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
   AND (c<=d-2 OR c>=d+2);
----

SELECT b,
       c-d,
       a+b*2+c*3+d*4+e*5
  FROM t1;
----

SELECT a,
       a+b*2,
       a-b
  FROM t1;
----

SELECT e,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       abs(a),
       a+b*2+c*3,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END
  FROM t1
 WHERE c>d
    OR e+d BETWEEN a+b-10 AND c+130
    OR d>e;
----

SELECT c-d,
       a-b,
       (a+b+c+d+e)/5,
       b-c,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1;
----

SELECT d-e,
       b-c,
       c
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130
   AND a>b
   AND a IS NULL;
----

SELECT a+b*2+c*3+d*4+e*5
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT (a+b+c+d+e)/5,
       a+b*2
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130;
----

SELECT (a+b+c+d+e)/5
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
   AND a IS NULL
   AND d NOT BETWEEN 110 AND 150;
----

SELECT b-c,
       e,
       c-d,
       a-b
  FROM t1
 WHERE b>c
    OR a IS NULL
    OR b IS NOT NULL;
----

SELECT (a+b+c+d+e)/5,
       abs(b-c),
       d
  FROM t1
 WHERE a>b
    OR a IS NULL
    OR b>c;
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       d,
       a-b,
       abs(b-c),
       (a+b+c+d+e)/5
  FROM t1
 WHERE b IS NOT NULL
    OR d NOT BETWEEN 110 AND 150;
----

SELECT d,
       d-e,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE b>c
    OR c BETWEEN b-2 AND d+2;
----

SELECT a+b*2,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a-b,
       c,
       (a+b+c+d+e)/5,
       a+b*2+c*3+d*4+e*5,
       abs(b-c)
  FROM t1;
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       abs(b-c)
  FROM t1
 WHERE a IS NULL;
----

SELECT a+b*2,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       (a+b+c+d+e)/5,
       a+b*2+c*3,
       b-c,
       a
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150;
----

SELECT abs(a),
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       e,
       abs(b-c),
       b,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END
  FROM t1
 WHERE (e>c OR e<d)
    OR c BETWEEN b-2 AND d+2;
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END
  FROM t1
 WHERE b>c;
----

SELECT c,
       b,
       a+b*2+c*3,
       a+b*2+c*3+d*4+e*5,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150
   AND coalesce(a,b,c,d,e)<>0
   AND e+d BETWEEN a+b-10 AND c+130;
----

SELECT c-d,
       a-b,
       a+b*2+c*3+d*4+e*5,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       abs(b-c),
       d,
       a+b*2+c*3
  FROM t1
 WHERE (a>b-2 AND a<b+2)
   AND b>c
   AND b IS NOT NULL;
----

SELECT a-b,
       b,
       a+b*2,
       a+b*2+c*3+d*4+e*5,
       d-e,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130
    OR d>e;
----

SELECT (a+b+c+d+e)/5,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1
 WHERE c>d;
----

SELECT a+b*2+c*3,
       a+b*2+c*3+d*4,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       abs(a),
       b,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1
 WHERE (a>b-2 AND a<b+2)
    OR d>e
    OR a>b;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a-b,
       d,
       a+b*2+c*3,
       c,
       (a+b+c+d+e)/5,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1
 WHERE (e>a AND e<b);
----

SELECT a+b*2+c*3+d*4,
       a+b*2,
       d-e,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       abs(a),
       a
  FROM t1;
----

SELECT a+b*2+c*3,
       (a+b+c+d+e)/5
  FROM t1
 WHERE (e>c OR e<d);
----

SELECT a+b*2+c*3+d*4+e*5,
       a,
       abs(b-c),
       a+b*2+c*3+d*4
  FROM t1
 WHERE (e>c OR e<d);
----

SELECT abs(b-c),
       abs(a),
       a+b*2+c*3+d*4+e*5,
       a+b*2+c*3,
       c-d
  FROM t1
 WHERE (e>c OR e<d)
    OR coalesce(a,b,c,d,e)<>0;
----

SELECT b,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       d,
       b-c,
       abs(b-c),
       a+b*2+c*3+d*4
  FROM t1
 WHERE (a>b-2 AND a<b+2)
   AND b IS NOT NULL
   AND d NOT BETWEEN 110 AND 150;
----

SELECT a+b*2,
       abs(b-c),
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a-b,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE (e>a AND e<b)
    OR a IS NULL;
----

SELECT a,
       b-c,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       d-e,
       d
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130
    OR d>e;
----

SELECT d
  FROM t1
 WHERE c>d
    OR b>c;
----

SELECT d-e,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a+b*2,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       c-d,
       c,
       a
  FROM t1
 WHERE (c<=d-2 OR c>=d+2);
----

SELECT e,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a+b*2,
       d-e,
       abs(a),
       a+b*2+c*3
  FROM t1
 WHERE b>c
    OR a>b;
----

SELECT a+b*2+c*3,
       (a+b+c+d+e)/5,
       b-c,
       e,
       abs(b-c),
       c-d,
       b
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130
   AND (e>c OR e<d)
   AND b>c;
----

SELECT a+b*2+c*3+d*4+e*5,
       c,
       abs(a),
       a+b*2+c*3+d*4,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
   AND b>c;
----

SELECT a+b*2+c*3,
       a+b*2+c*3+d*4+e*5,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END
  FROM t1
 WHERE d>e
   AND coalesce(a,b,c,d,e)<>0;
----

SELECT e,
       d-e,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a-b,
       b-c,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE d>e
    OR c BETWEEN b-2 AND d+2
    OR (e>a AND e<b);
----

SELECT a+b*2+c*3,
       a-b,
       d-e
  FROM t1
 WHERE a IS NULL
    OR b>c
    OR c BETWEEN b-2 AND d+2;
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1
 WHERE (e>c OR e<d);
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       e,
       a+b*2+c*3+d*4+e*5
  FROM t1
 WHERE d>e
    OR e+d BETWEEN a+b-10 AND c+130
    OR coalesce(a,b,c,d,e)<>0;
----

SELECT c-d,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       d,
       a-b
  FROM t1;
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a-b,
       d-e,
       c,
       a+b*2
  FROM t1
 WHERE (a>b-2 AND a<b+2)
   AND EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT a-b,
       a+b*2+c*3+d*4,
       c-d,
       b-c,
       abs(a),
       e
  FROM t1
 WHERE a IS NULL;
----

SELECT d,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END
  FROM t1
 WHERE d>e
    OR e+d BETWEEN a+b-10 AND c+130;
----

SELECT b,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a+b*2
  FROM t1;
----

SELECT b
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
   AND a IS NULL;
----

SELECT c-d,
       abs(a),
       a+b*2+c*3,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1;
----

SELECT abs(b-c),
       a-b,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       abs(a),
       b-c
  FROM t1;
----

SELECT d,
       c,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1;
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE (e>a AND e<b);
----

SELECT a+b*2+c*3,
       abs(b-c),
       a+b*2
  FROM t1
 WHERE c BETWEEN b-2 AND d+2;
----

SELECT d
  FROM t1
 WHERE (a>b-2 AND a<b+2);
----

SELECT a+b*2+c*3+d*4,
       c-d,
       a+b*2+c*3
  FROM t1;
----

SELECT (a+b+c+d+e)/5,
       abs(b-c),
       a+b*2,
       d-e,
       a+b*2+c*3+d*4
  FROM t1
 WHERE c>d;
----

SELECT a-b,
       abs(b-c),
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1
 WHERE (a>b-2 AND a<b+2);
----

SELECT (a+b+c+d+e)/5
  FROM t1;
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       (a+b+c+d+e)/5,
       a
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150
    OR d>e
    OR (a>b-2 AND a<b+2);
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1;
----

SELECT a+b*2+c*3,
       abs(a),
       c,
       a+b*2+c*3+d*4,
       e,
       a,
       a+b*2+c*3+d*4+e*5
  FROM t1
 WHERE a IS NULL
   AND coalesce(a,b,c,d,e)<>0
   AND (c<=d-2 OR c>=d+2);
----

SELECT c-d,
       b-c,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       c,
       a+b*2+c*3+d*4+e*5,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
   AND (a>b-2 AND a<b+2)
   AND d>e;
----

SELECT a+b*2+c*3+d*4+e*5,
       abs(a),
       a-b,
       a+b*2,
       b,
       c-d
  FROM t1
 WHERE a>b
   AND c BETWEEN b-2 AND d+2;
----

SELECT c-d,
       e,
       a+b*2+c*3+d*4,
       c,
       a,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1;
----

SELECT d,
       d-e,
       a+b*2+c*3+d*4+e*5,
       abs(a),
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a+b*2,
       b
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0
    OR b>c
    OR a>b;
----

SELECT d,
       c,
       a+b*2+c*3+d*4,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       abs(a),
       e
  FROM t1;
----

SELECT a+b*2+c*3+d*4+e*5,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       e,
       d,
       a+b*2+c*3+d*4
  FROM t1
 WHERE (c<=d-2 OR c>=d+2)
    OR (e>a AND e<b)
    OR b IS NOT NULL;
----

SELECT d,
       e,
       a+b*2+c*3+d*4+e*5,
       abs(a),
       c-d
  FROM t1;
----

SELECT b-c,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       c,
       d
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150;
----

SELECT e,
       a+b*2,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       (a+b+c+d+e)/5,
       a-b,
       abs(a),
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END
  FROM t1;
----

SELECT c-d,
       a+b*2+c*3+d*4
  FROM t1
 WHERE d>e
    OR a IS NULL
    OR EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT e,
       d,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       (a+b+c+d+e)/5,
       a+b*2+c*3+d*4+e*5
  FROM t1
 WHERE b>c
   AND d>e
   AND EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT d,
       e,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a+b*2+c*3+d*4+e*5,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       abs(b-c)
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0
   AND c>d;
----

SELECT a+b*2+c*3
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0;
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       (a+b+c+d+e)/5,
       abs(a),
       d-e,
       b-c,
       a
  FROM t1
 WHERE (a>b-2 AND a<b+2);
----

SELECT b-c,
       abs(b-c),
       a-b
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       abs(b-c),
       c,
       b,
       a-b
  FROM t1
 WHERE (c<=d-2 OR c>=d+2)
   AND c BETWEEN b-2 AND d+2
   AND d>e;
----

SELECT abs(b-c),
       a+b*2+c*3+d*4,
       e,
       b-c
  FROM t1
 WHERE a IS NULL
    OR coalesce(a,b,c,d,e)<>0
    OR b>c;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1;
----

SELECT c-d,
       a-b
  FROM t1
 WHERE d>e;
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a+b*2+c*3,
       c-d,
       d-e,
       a+b*2
  FROM t1
 WHERE d>e
    OR (e>c OR e<d)
    OR d NOT BETWEEN 110 AND 150;
----

SELECT b,
       a+b*2+c*3+d*4,
       a+b*2,
       d-e,
       abs(a),
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       d
  FROM t1
 WHERE c>d
    OR (c<=d-2 OR c>=d+2)
    OR EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT b,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a+b*2+c*3+d*4,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a+b*2+c*3,
       a
  FROM t1
 WHERE (e>c OR e<d)
   AND d>e;
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a+b*2+c*3+d*4
  FROM t1;
----

SELECT e,
       c-d
  FROM t1;
----

SELECT d
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
    OR c>d;
----

SELECT abs(a)
  FROM t1;
----

SELECT a+b*2+c*3+d*4+e*5,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       c,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a
  FROM t1
 WHERE (a>b-2 AND a<b+2)
   AND d NOT BETWEEN 110 AND 150
   AND d>e;
----

SELECT a,
       a-b
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       d-e,
       a,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a-b,
       abs(a)
  FROM t1;
----

SELECT d-e,
       b-c,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       abs(a),
       e
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150;
----

SELECT abs(a),
       b,
       abs(b-c),
       a+b*2+c*3,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a+b*2
  FROM t1
 WHERE (a>b-2 AND a<b+2);
----

SELECT c-d,
       a+b*2+c*3+d*4,
       a,
       b-c
  FROM t1;
----

SELECT a+b*2,
       abs(a),
       b,
       c,
       d,
       a+b*2+c*3
  FROM t1
 WHERE (c<=d-2 OR c>=d+2)
   AND c BETWEEN b-2 AND d+2
   AND b IS NOT NULL;
----

SELECT d-e,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a+b*2+c*3+d*4
  FROM t1;
----

SELECT e,
       a-b,
       b-c,
       b,
       (a+b+c+d+e)/5
  FROM t1
 WHERE a>b;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       d-e,
       c-d,
       a+b*2+c*3+d*4+e*5,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a
  FROM t1
 WHERE c BETWEEN b-2 AND d+2;
----

SELECT a+b*2,
       abs(a)
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150;
----

SELECT (a+b+c+d+e)/5,
       a+b*2+c*3+d*4+e*5,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       abs(a)
  FROM t1
 WHERE a>b;
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       (a+b+c+d+e)/5,
       d-e,
       a,
       a+b*2+c*3
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       b,
       a,
       d-e,
       e,
       c
  FROM t1
 WHERE c>d;
----

SELECT b,
       c,
       abs(b-c),
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a-b,
       e,
       a+b*2+c*3+d*4+e*5
  FROM t1
 WHERE (a>b-2 AND a<b+2)
    OR b>c;
----

SELECT (a+b+c+d+e)/5,
       a+b*2+c*3+d*4,
       abs(a),
       b-c,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1
 WHERE a IS NULL
   AND (e>c OR e<d);
----

SELECT a,
       c-d,
       b,
       a+b*2,
       b-c,
       a-b
  FROM t1
 WHERE b IS NOT NULL
   AND b>c;
----

SELECT b-c,
       a+b*2+c*3+d*4+e*5,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a-b,
       a,
       abs(b-c)
  FROM t1;
----

SELECT a+b*2,
       e
  FROM t1
 WHERE (c<=d-2 OR c>=d+2)
    OR d NOT BETWEEN 110 AND 150;
----

SELECT abs(a),
       a+b*2+c*3+d*4,
       d,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1;
----

SELECT e,
       abs(b-c)
  FROM t1;
----

SELECT d-e,
       a+b*2,
       c-d,
       e
  FROM t1;
----

SELECT c-d,
       abs(b-c),
       b-c,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       d
  FROM t1
 WHERE b>c
    OR c>d
    OR d NOT BETWEEN 110 AND 150;
----

SELECT a+b*2+c*3,
       (a+b+c+d+e)/5
  FROM t1
 WHERE (e>a AND e<b)
    OR e+d BETWEEN a+b-10 AND c+130;
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1
 WHERE (e>c OR e<d)
   AND EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
   AND (a>b-2 AND a<b+2);
----

SELECT a+b*2,
       d-e,
       d,
       abs(a),
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130;
----

SELECT b-c,
       c,
       a-b,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       c-d,
       abs(a)
  FROM t1;
----

SELECT a+b*2+c*3+d*4+e*5,
       c-d,
       abs(b-c)
  FROM t1;
----

SELECT a,
       c-d,
       b,
       a+b*2+c*3+d*4+e*5
  FROM t1
 WHERE a>b
   AND (c<=d-2 OR c>=d+2)
   AND b>c;
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a+b*2,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       (a+b+c+d+e)/5,
       abs(a),
       abs(b-c),
       c-d
  FROM t1;
----

SELECT a+b*2+c*3+d*4+e*5,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1;
----

SELECT b
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130
   AND a>b;
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       d,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       b-c
  FROM t1;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       c-d
  FROM t1
 WHERE d>e
    OR a IS NULL;
----

SELECT abs(b-c)
  FROM t1
 WHERE b IS NOT NULL
    OR (c<=d-2 OR c>=d+2)
    OR d>e;
----

SELECT a,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1;
----

SELECT a+b*2+c*3+d*4+e*5,
       a
  FROM t1;
----

SELECT (a+b+c+d+e)/5,
       a+b*2,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a
  FROM t1;
----

SELECT d,
       c,
       a-b
  FROM t1
 WHERE a IS NULL;
----

SELECT a,
       c,
       d-e,
       d
  FROM t1;
----

SELECT b,
       b-c,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       c-d,
       a+b*2+c*3+d*4
  FROM t1
 WHERE a IS NULL
   AND (e>c OR e<d)
   AND d NOT BETWEEN 110 AND 150;
----

SELECT abs(b-c),
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END
  FROM t1;
----

SELECT (a+b+c+d+e)/5,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130
   AND b IS NOT NULL
   AND (e>a AND e<b);
----

SELECT b-c,
       a-b,
       d-e
  FROM t1
 WHERE b IS NOT NULL
   AND (e>a AND e<b)
   AND (c<=d-2 OR c>=d+2);
----

SELECT a,
       b-c,
       c-d,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       e
  FROM t1
 WHERE d>e
   AND c BETWEEN b-2 AND d+2
   AND a>b;
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       c,
       a+b*2,
       a+b*2+c*3+d*4
  FROM t1;
----

SELECT abs(b-c)
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130
    OR (e>c OR e<d);
----

SELECT a+b*2,
       c,
       (a+b+c+d+e)/5
  FROM t1
 WHERE a>b
    OR b>c;
----

SELECT a-b,
       b,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1
 WHERE (c<=d-2 OR c>=d+2);
----

SELECT abs(b-c)
  FROM t1
 WHERE a IS NULL
   AND e+d BETWEEN a+b-10 AND c+130;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       c-d,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       abs(b-c),
       b
  FROM t1
 WHERE c>d
   AND EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT abs(b-c),
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a+b*2+c*3+d*4,
       b-c,
       d,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1
 WHERE (e>c OR e<d)
   AND coalesce(a,b,c,d,e)<>0
   AND e+d BETWEEN a+b-10 AND c+130;
----

SELECT e,
       a+b*2+c*3,
       abs(b-c),
       a+b*2+c*3+d*4
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0
   AND (a>b-2 AND a<b+2);
----

SELECT b-c,
       a+b*2+c*3+d*4
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0
    OR (e>c OR e<d)
    OR a IS NULL;
----

SELECT (a+b+c+d+e)/5,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1;
----

SELECT a+b*2+c*3,
       b-c
  FROM t1
 WHERE (e>c OR e<d);
----

SELECT e,
       a+b*2+c*3+d*4+e*5,
       d-e,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE b>c
   AND d NOT BETWEEN 110 AND 150
   AND coalesce(a,b,c,d,e)<>0;
----

SELECT a+b*2
  FROM t1;
----

SELECT d-e
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130
    OR (c<=d-2 OR c>=d+2);
----

SELECT (a+b+c+d+e)/5
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0
   AND (e>a AND e<b)
   AND b IS NOT NULL;
----

SELECT abs(b-c),
       c,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       e,
       b-c
  FROM t1
 WHERE (a>b-2 AND a<b+2)
   AND coalesce(a,b,c,d,e)<>0;
----

SELECT a+b*2,
       (a+b+c+d+e)/5
  FROM t1
 WHERE b IS NOT NULL
   AND c>d
   AND c BETWEEN b-2 AND d+2;
----

SELECT c,
       a+b*2+c*3+d*4,
       b,
       abs(b-c),
       (a+b+c+d+e)/5,
       abs(a)
  FROM t1;
----

SELECT c-d,
       abs(a)
  FROM t1;
----

SELECT a-b,
       a+b*2+c*3+d*4,
       c,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1
 WHERE (c<=d-2 OR c>=d+2)
   AND c>d;
----

SELECT d-e,
       (a+b+c+d+e)/5,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       abs(b-c),
       a+b*2+c*3,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1
 WHERE d>e
    OR d NOT BETWEEN 110 AND 150
    OR e+d BETWEEN a+b-10 AND c+130;
----

SELECT c,
       b-c,
       a+b*2+c*3
  FROM t1
 WHERE (c<=d-2 OR c>=d+2)
    OR d NOT BETWEEN 110 AND 150
    OR c BETWEEN b-2 AND d+2;
----

SELECT b,
       (a+b+c+d+e)/5
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130;
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       d-e,
       a+b*2,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1;
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       (a+b+c+d+e)/5
  FROM t1
 WHERE a IS NULL
    OR (a>b-2 AND a<b+2);
----

SELECT a+b*2+c*3,
       e,
       c-d,
       d,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a,
       a+b*2+c*3+d*4
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150
   AND b IS NOT NULL;
----

SELECT b-c,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       c-d
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
   AND c BETWEEN b-2 AND d+2;
----

SELECT d-e
  FROM t1;
----

SELECT abs(b-c),
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1
 WHERE b IS NOT NULL
    OR coalesce(a,b,c,d,e)<>0
    OR a IS NULL;
----

SELECT d-e,
       a+b*2+c*3+d*4+e*5,
       b-c,
       a,
       d,
       b
  FROM t1
 WHERE b IS NOT NULL
    OR d NOT BETWEEN 110 AND 150;
----

SELECT a,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       b,
       (a+b+c+d+e)/5,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a-b,
       d
  FROM t1
 WHERE b>c;
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a+b*2+c*3+d*4+e*5
  FROM t1;
----

SELECT a+b*2,
       c,
       d-e,
       a+b*2+c*3+d*4,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a-b
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130
    OR (c<=d-2 OR c>=d+2);
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
    OR a IS NULL
    OR (c<=d-2 OR c>=d+2);
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       b
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130;
----

SELECT a+b*2,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       e
  FROM t1;
----

SELECT e,
       d,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a+b*2+c*3+d*4
  FROM t1
 WHERE d>e
    OR c BETWEEN b-2 AND d+2;
----

SELECT c-d
  FROM t1
 WHERE a>b;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a+b*2,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       b-c,
       a,
       abs(a)
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
    OR e+d BETWEEN a+b-10 AND c+130
    OR d>e;
----

SELECT a+b*2,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       b-c,
       c-d,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       d-e,
       (a+b+c+d+e)/5
  FROM t1;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a
  FROM t1
 WHERE b>c
    OR c BETWEEN b-2 AND d+2;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       e,
       b-c,
       d
  FROM t1
 WHERE (a>b-2 AND a<b+2)
    OR d NOT BETWEEN 110 AND 150;
----

SELECT c,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
    OR d NOT BETWEEN 110 AND 150;
----

SELECT a,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a+b*2+c*3+d*4+e*5
  FROM t1
 WHERE c>d
    OR (e>c OR e<d)
    OR (a>b-2 AND a<b+2);
----

SELECT c,
       e
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130
   AND d NOT BETWEEN 110 AND 150;
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       c-d,
       abs(a),
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       b-c,
       a-b,
       b
  FROM t1
 WHERE a>b
    OR (a>b-2 AND a<b+2)
    OR d>e;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1;
----

SELECT d-e,
       (a+b+c+d+e)/5
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130
   AND (c<=d-2 OR c>=d+2);
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       b
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0
   AND d NOT BETWEEN 110 AND 150;
----

SELECT a+b*2+c*3+d*4
  FROM t1
 WHERE d>e
    OR e+d BETWEEN a+b-10 AND c+130;
----

SELECT a+b*2+c*3+d*4+e*5,
       e,
       a+b*2,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1;
----

SELECT a+b*2+c*3+d*4,
       c,
       a+b*2+c*3+d*4+e*5,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE b>c
   AND (e>c OR e<d);
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1
 WHERE b>c
   AND (e>c OR e<d)
   AND coalesce(a,b,c,d,e)<>0;
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       e,
       c-d,
       b,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a-b
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
   AND d NOT BETWEEN 110 AND 150
   AND b>c;
----

SELECT e,
       a+b*2+c*3,
       a+b*2,
       a,
       abs(b-c),
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       b
  FROM t1
 WHERE b>c;
----

SELECT e
  FROM t1
 WHERE b>c;
----

SELECT abs(b-c),
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a,
       a-b,
       a+b*2+c*3+d*4+e*5,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1
 WHERE d>e
    OR EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
    OR coalesce(a,b,c,d,e)<>0;
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a
  FROM t1;
----

SELECT c,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       abs(b-c),
       a+b*2+c*3+d*4+e*5,
       a
  FROM t1;
----

SELECT a+b*2,
       c-d,
       e,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       b,
       (a+b+c+d+e)/5
  FROM t1
 WHERE (e>c OR e<d);
----

SELECT b,
       (a+b+c+d+e)/5,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       c,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       d-e,
       a-b
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130
    OR (c<=d-2 OR c>=d+2);
----

SELECT c,
       a+b*2+c*3,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1;
----

SELECT a+b*2+c*3+d*4+e*5,
       (a+b+c+d+e)/5,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       c-d
  FROM t1
 WHERE (e>c OR e<d)
   AND b>c;
----

SELECT d-e,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a-b
  FROM t1
 WHERE a>b
    OR e+d BETWEEN a+b-10 AND c+130
    OR (e>c OR e<d);
----

SELECT a+b*2,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END
  FROM t1;
----

SELECT b,
       a-b
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150
   AND a>b
   AND d>e;
----

SELECT b
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT abs(a),
       abs(b-c),
       a
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130;
----

SELECT (a+b+c+d+e)/5,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       b-c,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a+b*2+c*3+d*4+e*5,
       a+b*2,
       a+b*2+c*3+d*4
  FROM t1;
----

SELECT a+b*2,
       a-b,
       b,
       d
  FROM t1
 WHERE d>e
    OR d NOT BETWEEN 110 AND 150
    OR (c<=d-2 OR c>=d+2);
----

SELECT a+b*2+c*3+d*4+e*5,
       a+b*2+c*3+d*4,
       a+b*2+c*3
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130
    OR (a>b-2 AND a<b+2);
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       abs(b-c),
       d,
       e,
       d-e
  FROM t1;
----

SELECT a+b*2,
       b-c,
       c,
       b,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130
   AND c>d
   AND b>c;
----

SELECT c-d,
       a,
       a+b*2,
       abs(a),
       (a+b+c+d+e)/5,
       c
  FROM t1
 WHERE a IS NULL
    OR (c<=d-2 OR c>=d+2)
    OR b>c;
----

SELECT b-c,
       a+b*2,
       a+b*2+c*3
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0
    OR a IS NULL
    OR d NOT BETWEEN 110 AND 150;
----

SELECT e
  FROM t1
 WHERE (e>a AND e<b)
    OR (e>c OR e<d);
----

SELECT a+b*2+c*3+d*4+e*5,
       a-b
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130;
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       (a+b+c+d+e)/5
  FROM t1
 WHERE b IS NOT NULL
   AND d NOT BETWEEN 110 AND 150;
----

SELECT a+b*2+c*3+d*4+e*5
  FROM t1;
----

SELECT a-b,
       c,
       (a+b+c+d+e)/5,
       b-c,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       b
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
    OR (c<=d-2 OR c>=d+2);
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       c,
       d,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a+b*2,
       c-d,
       a+b*2+c*3+d*4+e*5
  FROM t1
 WHERE a>b
    OR d>e;
----

SELECT a,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a-b,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END
  FROM t1
 WHERE a IS NULL;
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       d-e,
       a+b*2+c*3,
       c-d,
       (a+b+c+d+e)/5
  FROM t1;
----

SELECT abs(a),
       c,
       abs(b-c),
       b-c
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
    OR c>d
    OR EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       c,
       abs(a)
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0
   AND c>d
   AND (c<=d-2 OR c>=d+2);
----

SELECT a+b*2+c*3+d*4,
       a+b*2+c*3+d*4+e*5,
       b,
       a-b,
       a,
       a+b*2+c*3
  FROM t1
 WHERE a IS NULL
    OR b>c;
----

SELECT a+b*2+c*3+d*4,
       a-b,
       b-c,
       b,
       (a+b+c+d+e)/5,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1
 WHERE (c<=d-2 OR c>=d+2);
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       c,
       a+b*2+c*3+d*4,
       abs(b-c),
       d-e
  FROM t1;
----

SELECT abs(a),
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       c-d
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150;
----

SELECT d
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0
   AND d NOT BETWEEN 110 AND 150;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END
  FROM t1;
----

SELECT c-d,
       abs(a)
  FROM t1
 WHERE (c<=d-2 OR c>=d+2);
----

SELECT abs(b-c),
       c-d
  FROM t1
 WHERE b IS NOT NULL;
----

SELECT b,
       c,
       e,
       abs(a),
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a-b
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150;
----

SELECT a-b,
       c,
       b,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a+b*2+c*3
  FROM t1;
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       c-d,
       a+b*2+c*3+d*4+e*5,
       d
  FROM t1;
----

SELECT a-b,
       (a+b+c+d+e)/5,
       abs(a)
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
   AND b IS NOT NULL
   AND (a>b-2 AND a<b+2);
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       (a+b+c+d+e)/5
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130;
----

SELECT a+b*2+c*3+d*4,
       a+b*2+c*3,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       abs(b-c),
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1
 WHERE b IS NOT NULL;
----

SELECT a+b*2,
       a-b
  FROM t1
 WHERE a>b
   AND d>e
   AND (a>b-2 AND a<b+2);
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       c-d
  FROM t1
 WHERE (e>a AND e<b)
    OR coalesce(a,b,c,d,e)<>0;
----

SELECT d,
       c-d,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a+b*2+c*3+d*4,
       (a+b+c+d+e)/5,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1
 WHERE c>d
   AND b IS NOT NULL
   AND a>b;
----

SELECT a-b,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       c,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       e,
       (a+b+c+d+e)/5,
       d-e
  FROM t1;
----

SELECT a,
       abs(a),
       a+b*2+c*3+d*4+e*5,
       a+b*2
  FROM t1
 WHERE (c<=d-2 OR c>=d+2)
    OR c>d;
----

SELECT c,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END
  FROM t1;
----

SELECT b-c,
       a+b*2+c*3+d*4,
       b,
       c-d,
       a+b*2+c*3
  FROM t1
 WHERE d>e
   AND c>d
   AND coalesce(a,b,c,d,e)<>0;
----

SELECT b,
       (a+b+c+d+e)/5,
       a-b,
       a+b*2+c*3+d*4+e*5,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       d-e
  FROM t1
 WHERE d>e;
----

SELECT c,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a,
       a+b*2
  FROM t1
 WHERE a IS NULL;
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150;
----

SELECT a+b*2,
       b-c,
       c-d
  FROM t1;
----

SELECT (a+b+c+d+e)/5,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1;
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       d-e,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1
 WHERE d>e;
----

SELECT a,
       c-d,
       abs(a)
  FROM t1
 WHERE d>e;
----

SELECT abs(b-c),
       a-b,
       c-d,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       b-c
  FROM t1;
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       c-d,
       d-e,
       a+b*2+c*3
  FROM t1
 WHERE b>c
    OR d NOT BETWEEN 110 AND 150
    OR (a>b-2 AND a<b+2);
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150;
----

SELECT (a+b+c+d+e)/5,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       b,
       c-d,
       d,
       a+b*2+c*3+d*4,
       a+b*2+c*3
  FROM t1;
----

SELECT a,
       d-e,
       e,
       a+b*2+c*3,
       abs(b-c),
       c,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE (a>b-2 AND a<b+2)
    OR c>d;
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END
  FROM t1
 WHERE c BETWEEN b-2 AND d+2;
----

SELECT c-d,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a+b*2+c*3+d*4,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a-b,
       d-e
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150;
----

SELECT a+b*2+c*3
  FROM t1
 WHERE a IS NULL;
----

SELECT (a+b+c+d+e)/5,
       b-c,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       c-d
  FROM t1;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a+b*2+c*3+d*4
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0;
----

SELECT b,
       d-e,
       a+b*2+c*3+d*4,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1
 WHERE a IS NULL
   AND d>e;
----

SELECT e,
       a+b*2,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a+b*2+c*3+d*4+e*5
  FROM t1;
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       b,
       e,
       c-d,
       b-c,
       d
  FROM t1
 WHERE d>e
   AND a>b
   AND d NOT BETWEEN 110 AND 150;
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       b,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       c,
       (a+b+c+d+e)/5
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT c,
       a-b
  FROM t1
 WHERE a>b;
----

SELECT (a+b+c+d+e)/5,
       abs(b-c),
       c-d,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a,
       b,
       abs(a)
  FROM t1
 WHERE c>d
    OR c BETWEEN b-2 AND d+2;
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       b
  FROM t1
 WHERE a IS NULL;
----

SELECT (a+b+c+d+e)/5,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a+b*2+c*3+d*4+e*5,
       a
  FROM t1
 WHERE (a>b-2 AND a<b+2)
   AND (c<=d-2 OR c>=d+2)
   AND (e>a AND e<b);
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a+b*2,
       a+b*2+c*3,
       d-e,
       (a+b+c+d+e)/5,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1
 WHERE (e>a AND e<b)
    OR b IS NOT NULL;
----

SELECT d,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a,
       c-d,
       a-b,
       (a+b+c+d+e)/5
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
    OR a>b
    OR c BETWEEN b-2 AND d+2;
----

SELECT a-b,
       c-d,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       d-e
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150;
----

SELECT c-d,
       a+b*2,
       d-e
  FROM t1
 WHERE c>d;
----

SELECT (a+b+c+d+e)/5,
       abs(b-c),
       a+b*2+c*3,
       a+b*2+c*3+d*4
  FROM t1;
----

SELECT a-b,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE d>e;
----

SELECT a-b
  FROM t1
 WHERE (e>c OR e<d);
----

SELECT d-e,
       c-d
  FROM t1;
----

SELECT c-d,
       d,
       abs(b-c),
       d-e,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       b-c
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0
   AND (e>c OR e<d);
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a+b*2+c*3+d*4+e*5,
       b,
       abs(b-c),
       abs(a)
  FROM t1
 WHERE b IS NOT NULL
    OR (a>b-2 AND a<b+2)
    OR (e>a AND e<b);
----

SELECT a+b*2+c*3+d*4+e*5,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a+b*2+c*3+d*4,
       e,
       d
  FROM t1;
----

SELECT c-d,
       abs(a),
       a+b*2,
       a+b*2+c*3+d*4,
       d-e,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a
  FROM t1
 WHERE (c<=d-2 OR c>=d+2);
----

SELECT a,
       a+b*2+c*3+d*4,
       d
  FROM t1
 WHERE b IS NOT NULL;
----

SELECT a+b*2+c*3+d*4,
       (a+b+c+d+e)/5,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a-b,
       abs(b-c)
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0
   AND d>e;
----

SELECT e,
       abs(a),
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
    OR (c<=d-2 OR c>=d+2);
----

SELECT abs(b-c),
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       abs(a),
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a-b,
       c,
       a
  FROM t1;
----

SELECT abs(b-c),
       d-e,
       a+b*2+c*3,
       a+b*2+c*3+d*4+e*5,
       a,
       b,
       abs(a)
  FROM t1
 WHERE c>d;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a+b*2+c*3+d*4,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       d-e,
       abs(b-c),
       (a+b+c+d+e)/5
  FROM t1;
----

SELECT a+b*2+c*3+d*4,
       d-e,
       c-d,
       abs(a),
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
   AND (c<=d-2 OR c>=d+2);
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a+b*2+c*3,
       a,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       (a+b+c+d+e)/5,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1
 WHERE c>d
    OR b>c;
----

SELECT b,
       a+b*2,
       b-c,
       a,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       d-e
  FROM t1
 WHERE (c<=d-2 OR c>=d+2);
----

SELECT a+b*2+c*3+d*4
  FROM t1
 WHERE (a>b-2 AND a<b+2);
----

SELECT d-e,
       b-c,
       a-b,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1;
----

SELECT b
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
   AND d>e
   AND EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       abs(b-c)
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT a+b*2
  FROM t1
 WHERE (a>b-2 AND a<b+2);
----

SELECT d-e
  FROM t1
 WHERE c>d
   AND d NOT BETWEEN 110 AND 150;
----

SELECT (a+b+c+d+e)/5,
       c-d
  FROM t1
 WHERE (c<=d-2 OR c>=d+2)
   AND d NOT BETWEEN 110 AND 150;
----

SELECT a-b
  FROM t1
 WHERE b IS NOT NULL
   AND c>d;
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE a>b
    OR c>d;
----

SELECT a+b*2+c*3+d*4+e*5,
       abs(a),
       e,
       a+b*2+c*3,
       abs(b-c),
       a-b,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1
 WHERE (e>c OR e<d)
   AND EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT a+b*2+c*3+d*4,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       b,
       d,
       c-d,
       a+b*2
  FROM t1
 WHERE a>b
    OR (c<=d-2 OR c>=d+2)
    OR a IS NULL;
----

SELECT b-c,
       a+b*2
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT (a+b+c+d+e)/5,
       a,
       b-c,
       a+b*2,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       c-d
  FROM t1
 WHERE d>e
    OR c BETWEEN b-2 AND d+2
    OR (e>a AND e<b);
----

SELECT a+b*2+c*3,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       e,
       (a+b+c+d+e)/5
  FROM t1
 WHERE (e>a AND e<b)
    OR a>b;
----

SELECT b,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       d,
       a-b,
       a+b*2,
       a+b*2+c*3,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a+b*2+c*3,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       abs(b-c),
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END
  FROM t1
 WHERE (e>a AND e<b);
----

SELECT c,
       a+b*2+c*3,
       (a+b+c+d+e)/5,
       a-b,
       d
  FROM t1
 WHERE (e>a AND e<b)
    OR c BETWEEN b-2 AND d+2;
----

SELECT abs(b-c),
       d-e,
       b,
       c
  FROM t1;
----

SELECT abs(b-c),
       c-d,
       d-e,
       e,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1
 WHERE (a>b-2 AND a<b+2)
   AND d>e
   AND b IS NOT NULL;
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a,
       abs(a),
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0
   AND d NOT BETWEEN 110 AND 150
   AND b IS NOT NULL;
----

SELECT a+b*2+c*3,
       a+b*2+c*3+d*4+e*5,
       a+b*2+c*3+d*4,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       c-d,
       (a+b+c+d+e)/5
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0;
----

SELECT a+b*2+c*3+d*4+e*5,
       a-b,
       abs(a)
  FROM t1;
----

SELECT a+b*2+c*3+d*4,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT a-b,
       c,
       abs(a),
       a+b*2+c*3+d*4
  FROM t1;
----

SELECT a+b*2+c*3+d*4,
       a+b*2,
       b,
       b-c,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a-b
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150;
----

SELECT b-c,
       abs(b-c),
       a+b*2+c*3+d*4+e*5,
       d
  FROM t1
 WHERE d>e;
----

SELECT d,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       abs(b-c)
  FROM t1
 WHERE b>c
   AND d>e
   AND coalesce(a,b,c,d,e)<>0;
----

SELECT d-e,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a+b*2+c*3+d*4+e*5,
       a+b*2+c*3,
       abs(b-c),
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       b
  FROM t1
 WHERE (c<=d-2 OR c>=d+2)
    OR a IS NULL
    OR d NOT BETWEEN 110 AND 150;
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       c-d,
       d-e,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1;
----

SELECT a+b*2+c*3+d*4,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a+b*2+c*3+d*4+e*5
  FROM t1
 WHERE (e>a AND e<b)
   AND d NOT BETWEEN 110 AND 150;
----

SELECT d-e,
       e,
       b-c,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1
 WHERE (a>b-2 AND a<b+2)
    OR EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1
 WHERE (e>c OR e<d)
    OR a IS NULL;
----

SELECT a+b*2+c*3,
       abs(b-c)
  FROM t1;
----

SELECT d
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       c,
       b,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a+b*2+c*3+d*4+e*5,
       (a+b+c+d+e)/5
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT b-c,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       c-d,
       b,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1
 WHERE c>d;
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       c,
       a+b*2,
       a+b*2+c*3
  FROM t1
 WHERE (e>c OR e<d);
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a+b*2+c*3
  FROM t1
 WHERE a IS NULL
    OR (c<=d-2 OR c>=d+2)
    OR (e>a AND e<b);
----

SELECT a+b*2+c*3+d*4+e*5,
       a+b*2+c*3
  FROM t1;
----

SELECT b
  FROM t1;
----

SELECT a+b*2+c*3+d*4+e*5,
       c,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       c-d,
       a+b*2+c*3+d*4
  FROM t1
 WHERE a IS NULL
   AND c>d
   AND (e>a AND e<b);
----

SELECT c-d
  FROM t1
 WHERE (e>c OR e<d)
   AND d NOT BETWEEN 110 AND 150;
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       b-c
  FROM t1
 WHERE a>b
    OR a IS NULL;
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a,
       c-d
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
   AND b>c;
----

SELECT e,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a,
       (a+b+c+d+e)/5
  FROM t1
 WHERE d>e;
----

SELECT a-b,
       (a+b+c+d+e)/5,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       abs(b-c),
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a+b*2,
       b
  FROM t1
 WHERE c BETWEEN b-2 AND d+2;
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a+b*2,
       b,
       d-e
  FROM t1
 WHERE b IS NOT NULL;
----

SELECT e,
       a+b*2+c*3,
       a+b*2+c*3+d*4,
       b-c,
       d-e
  FROM t1;
----

SELECT c,
       abs(b-c),
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       c-d,
       a+b*2
  FROM t1
 WHERE (c<=d-2 OR c>=d+2)
    OR c>d
    OR a IS NULL;
----

SELECT b-c,
       abs(a)
  FROM t1;
----

SELECT d,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       abs(b-c),
       a-b,
       a
  FROM t1
 WHERE b>c
    OR d NOT BETWEEN 110 AND 150;
----

SELECT a,
       c
  FROM t1
 WHERE d>e
   AND b>c;
----

SELECT b-c
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
    OR c BETWEEN b-2 AND d+2
    OR (c<=d-2 OR c>=d+2);
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1
 WHERE c BETWEEN b-2 AND d+2;
----

SELECT a+b*2+c*3+d*4,
       a-b,
       a+b*2+c*3,
       e
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
    OR c BETWEEN b-2 AND d+2;
----

SELECT d-e,
       a+b*2+c*3+d*4,
       a+b*2+c*3+d*4+e*5,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       d,
       abs(b-c)
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150
    OR (e>c OR e<d);
----

SELECT (a+b+c+d+e)/5,
       d,
       c
  FROM t1
 WHERE b>c;
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a+b*2
  FROM t1;
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a+b*2+c*3+d*4+e*5,
       d,
       e
  FROM t1
 WHERE a IS NULL;
----

SELECT a+b*2+c*3+d*4
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0
    OR a IS NULL
    OR EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT abs(a)
  FROM t1
 WHERE d>e
   AND a IS NULL;
----

SELECT a+b*2+c*3,
       a+b*2+c*3+d*4,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       abs(a)
  FROM t1;
----

SELECT b-c,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       c-d,
       a+b*2+c*3+d*4,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1;
----

SELECT a+b*2+c*3+d*4
  FROM t1;
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       abs(a),
       a,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a-b,
       d
  FROM t1
 WHERE (e>a AND e<b)
    OR (a>b-2 AND a<b+2)
    OR c BETWEEN b-2 AND d+2;
----

SELECT d-e,
       (a+b+c+d+e)/5,
       b,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       abs(a),
       a+b*2+c*3+d*4,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1;
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       b-c,
       a+b*2+c*3
  FROM t1;
----

SELECT c-d,
       c,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END
  FROM t1;
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       (a+b+c+d+e)/5
  FROM t1
 WHERE b>c;
----

SELECT a+b*2+c*3
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
    OR coalesce(a,b,c,d,e)<>0
    OR (e>c OR e<d);
----

SELECT a+b*2,
       abs(b-c),
       a
  FROM t1
 WHERE a IS NULL
   AND (e>c OR e<d)
   AND c>d;
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       c,
       a+b*2+c*3,
       d,
       d-e
  FROM t1
 WHERE (e>a AND e<b);
----

SELECT d-e
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
   AND b>c;
----

SELECT abs(a),
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130
   AND c BETWEEN b-2 AND d+2;
----

SELECT a+b*2+c*3+d*4,
       a,
       b,
       abs(b-c)
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0;
----

SELECT a
  FROM t1;
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE b IS NOT NULL
    OR c BETWEEN b-2 AND d+2
    OR (e>c OR e<d);
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a+b*2+c*3,
       a+b*2
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0
   AND a>b
   AND b IS NOT NULL;
----

SELECT a-b,
       a+b*2+c*3,
       c-d,
       a+b*2+c*3+d*4,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       c
  FROM t1;
----

SELECT c
  FROM t1
 WHERE a IS NULL
   AND (c<=d-2 OR c>=d+2)
   AND c>d;
----

SELECT e,
       b-c,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       d-e,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       b
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
    OR e+d BETWEEN a+b-10 AND c+130
    OR EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       b,
       d
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
   AND c BETWEEN b-2 AND d+2;
----

SELECT c-d,
       a+b*2+c*3+d*4+e*5,
       b,
       d
  FROM t1
 WHERE (c<=d-2 OR c>=d+2)
   AND b IS NOT NULL;
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE c BETWEEN b-2 AND d+2;
----

SELECT c-d,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE b>c;
----

SELECT a+b*2+c*3+d*4,
       a,
       abs(b-c),
       d-e,
       a+b*2
  FROM t1
 WHERE b IS NOT NULL
   AND a>b
   AND c BETWEEN b-2 AND d+2;
----

SELECT a+b*2+c*3
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
    OR d>e;
----

SELECT c-d,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a+b*2+c*3+d*4,
       a+b*2,
       c,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
    OR coalesce(a,b,c,d,e)<>0;
----

SELECT a+b*2
  FROM t1
 WHERE (e>a AND e<b)
    OR (a>b-2 AND a<b+2);
----

SELECT a+b*2+c*3+d*4+e*5,
       e
  FROM t1
 WHERE (e>a AND e<b)
   AND (e>c OR e<d);
----

SELECT d-e,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE c>d
    OR (c<=d-2 OR c>=d+2)
    OR e+d BETWEEN a+b-10 AND c+130;
----

SELECT (a+b+c+d+e)/5,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       b
  FROM t1;
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a+b*2+c*3+d*4+e*5,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a+b*2,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a+b*2+c*3+d*4
  FROM t1
 WHERE a IS NULL
   AND b IS NOT NULL
   AND c BETWEEN b-2 AND d+2;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       c,
       c-d,
       a+b*2+c*3,
       abs(b-c)
  FROM t1
 WHERE (e>a AND e<b);
----

SELECT b-c,
       a+b*2,
       b,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1
 WHERE (e>c OR e<d);
----

SELECT c,
       a+b*2+c*3+d*4
  FROM t1
 WHERE d>e
   AND b IS NOT NULL
   AND (e>a AND e<b);
----

SELECT a+b*2+c*3+d*4
  FROM t1;
----

SELECT d-e,
       b
  FROM t1
 WHERE a>b;
----

SELECT d-e,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a-b,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       b,
       a+b*2+c*3+d*4+e*5
  FROM t1
 WHERE a>b
    OR b>c
    OR c BETWEEN b-2 AND d+2;
----

SELECT a-b
  FROM t1;
----

SELECT c-d,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       d-e,
       (a+b+c+d+e)/5
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
   AND b>c
   AND a IS NULL;
----

SELECT abs(b-c),
       c,
       a+b*2+c*3+d*4+e*5
  FROM t1;
----

SELECT abs(a),
       d
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
   AND d NOT BETWEEN 110 AND 150
   AND c>d;
----

SELECT d-e,
       c-d,
       a+b*2+c*3+d*4+e*5,
       a+b*2+c*3+d*4,
       a
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130;
----

SELECT a+b*2+c*3+d*4+e*5,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a+b*2+c*3,
       b-c,
       e,
       a-b
  FROM t1;
----

SELECT c-d,
       d
  FROM t1
 WHERE (a>b-2 AND a<b+2)
   AND b>c;
----

SELECT a+b*2+c*3+d*4+e*5,
       abs(a)
  FROM t1
 WHERE a>b
    OR (e>c OR e<d);
----

SELECT d,
       (a+b+c+d+e)/5,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END
  FROM t1;
----

SELECT abs(b-c),
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       b
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0
   AND (e>c OR e<d)
   AND (a>b-2 AND a<b+2);
----

SELECT d,
       b-c,
       a-b,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a,
       a+b*2
  FROM t1
 WHERE c BETWEEN b-2 AND d+2;
----

SELECT a+b*2,
       d,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       abs(b-c),
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a,
       a+b*2+c*3+d*4
  FROM t1;
----

SELECT abs(a),
       a+b*2+c*3,
       d
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0
   AND (a>b-2 AND a<b+2);
----

SELECT a,
       a+b*2,
       a+b*2+c*3,
       b,
       abs(b-c),
       c-d,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1;
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1
 WHERE (e>a AND e<b)
    OR c BETWEEN b-2 AND d+2;
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       c-d,
       a-b,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1;
----

SELECT a,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130
    OR (a>b-2 AND a<b+2)
    OR a>b;
----

SELECT abs(a),
       a+b*2,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a+b*2+c*3+d*4
  FROM t1;
----

SELECT c-d
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0;
----

SELECT b,
       b-c,
       a+b*2+c*3+d*4+e*5,
       abs(a),
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE b IS NOT NULL
   AND coalesce(a,b,c,d,e)<>0;
----

SELECT e
  FROM t1
 WHERE (c<=d-2 OR c>=d+2)
    OR b IS NOT NULL;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a,
       a+b*2+c*3,
       c-d,
       e,
       a+b*2+c*3+d*4+e*5,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE c BETWEEN b-2 AND d+2;
----

SELECT abs(a),
       a-b,
       a+b*2+c*3+d*4,
       a+b*2
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a,
       (a+b+c+d+e)/5,
       a+b*2+c*3+d*4+e*5,
       a-b
  FROM t1
 WHERE a>b
    OR d NOT BETWEEN 110 AND 150;
----

SELECT abs(a),
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a
  FROM t1
 WHERE (e>a AND e<b)
    OR (e>c OR e<d);
----

SELECT d,
       b,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a-b,
       (a+b+c+d+e)/5
  FROM t1
 WHERE b>c
    OR d>e;
----

SELECT e,
       b-c,
       abs(b-c),
       c,
       c-d
  FROM t1
 WHERE a>b;
----

SELECT b,
       a-b,
       d,
       a+b*2,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       b-c,
       a+b*2+c*3
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150;
----

SELECT d-e,
       a-b,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a+b*2,
       abs(b-c),
       c-d
  FROM t1
 WHERE (e>a AND e<b)
    OR a IS NULL
    OR c>d;
----

SELECT a+b*2
  FROM t1
 WHERE c>d
    OR a>b
    OR d>e;
----

SELECT c-d,
       b-c,
       e,
       a+b*2+c*3+d*4+e*5
  FROM t1
 WHERE d>e;
----

SELECT e
  FROM t1
 WHERE a>b
    OR coalesce(a,b,c,d,e)<>0;
----

SELECT a+b*2+c*3+d*4,
       a+b*2+c*3,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       b
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
   AND a>b
   AND e+d BETWEEN a+b-10 AND c+130;
----

SELECT c
  FROM t1
 WHERE c>d
    OR EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
    OR (c<=d-2 OR c>=d+2);
----

SELECT e,
       a+b*2+c*3+d*4+e*5,
       (a+b+c+d+e)/5,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a,
       b-c
  FROM t1
 WHERE a>b
   AND EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
   AND d>e;
----

SELECT e,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       abs(b-c),
       a-b
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130
   AND (c<=d-2 OR c>=d+2)
   AND (e>a AND e<b);
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       abs(b-c),
       abs(a)
  FROM t1
 WHERE a>b
    OR c>d;
----

SELECT c,
       b,
       abs(b-c),
       e,
       d-e,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1;
----

SELECT c-d,
       d,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE (a>b-2 AND a<b+2)
   AND d NOT BETWEEN 110 AND 150;
----

SELECT a-b,
       a+b*2,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1
 WHERE (e>c OR e<d)
    OR d NOT BETWEEN 110 AND 150
    OR (e>a AND e<b);
----

SELECT (a+b+c+d+e)/5,
       c,
       a+b*2
  FROM t1;
----

SELECT a+b*2+c*3+d*4,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a-b,
       abs(b-c),
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
    OR EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
    OR e+d BETWEEN a+b-10 AND c+130;
----

SELECT a+b*2,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1
 WHERE b IS NOT NULL;
----

SELECT a,
       e,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       abs(a),
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       (a+b+c+d+e)/5
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
    OR e+d BETWEEN a+b-10 AND c+130;
----

SELECT d-e,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a+b*2,
       abs(b-c),
       a+b*2+c*3+d*4
  FROM t1
 WHERE (a>b-2 AND a<b+2)
    OR (e>c OR e<d)
    OR b>c;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       c,
       d,
       b-c,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a+b*2+c*3+d*4
  FROM t1
 WHERE d>e
    OR b IS NOT NULL
    OR d NOT BETWEEN 110 AND 150;
----

SELECT a+b*2+c*3+d*4+e*5,
       c-d,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130
   AND (c<=d-2 OR c>=d+2);
----

SELECT (a+b+c+d+e)/5,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a+b*2,
       b
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0
    OR b IS NOT NULL;
----

SELECT c,
       a+b*2+c*3+d*4+e*5,
       b,
       a+b*2
  FROM t1;
----

SELECT b-c,
       b,
       a+b*2,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       c,
       d-e
  FROM t1
 WHERE (c<=d-2 OR c>=d+2);
----

SELECT a,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       (a+b+c+d+e)/5,
       a+b*2+c*3+d*4,
       abs(b-c)
  FROM t1
 WHERE a IS NULL
   AND coalesce(a,b,c,d,e)<>0;
----

SELECT b,
       b-c,
       e,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a
  FROM t1
 WHERE a IS NULL
    OR (c<=d-2 OR c>=d+2);
----

SELECT a-b,
       d-e
  FROM t1;
----

SELECT a+b*2+c*3+d*4+e*5
  FROM t1
 WHERE (a>b-2 AND a<b+2);
----

SELECT d-e,
       a,
       b-c,
       e,
       d
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130;
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a+b*2
  FROM t1
 WHERE d>e
    OR (c<=d-2 OR c>=d+2);
----

SELECT a-b,
       a+b*2+c*3,
       abs(b-c),
       d,
       (a+b+c+d+e)/5,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE b IS NOT NULL;
----

SELECT a+b*2+c*3+d*4
  FROM t1
 WHERE (e>c OR e<d)
   AND (a>b-2 AND a<b+2);
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a+b*2+c*3+d*4,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       (a+b+c+d+e)/5,
       abs(b-c),
       a+b*2
  FROM t1
 WHERE a>b;
----

SELECT abs(a)
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
   AND coalesce(a,b,c,d,e)<>0
   AND a IS NULL;
----

SELECT a+b*2+c*3+d*4,
       c-d,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       e,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       (a+b+c+d+e)/5,
       b
  FROM t1
 WHERE c>d;
----

SELECT a+b*2,
       a-b,
       a+b*2+c*3+d*4,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130
    OR d NOT BETWEEN 110 AND 150;
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       b-c,
       a+b*2+c*3+d*4,
       d-e,
       a+b*2+c*3
  FROM t1
 WHERE a>b
   AND c BETWEEN b-2 AND d+2;
----

SELECT a-b,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       c-d,
       b
  FROM t1;
----

SELECT c-d,
       abs(b-c),
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150
   AND a>b;
----

SELECT b,
       a
  FROM t1
 WHERE (a>b-2 AND a<b+2);
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a+b*2,
       a,
       b,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       d-e
  FROM t1
 WHERE b>c
    OR (e>a AND e<b);
----

SELECT (a+b+c+d+e)/5,
       abs(a),
       d-e,
       e,
       b
  FROM t1;
----

SELECT d,
       abs(a),
       c,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       e
  FROM t1
 WHERE (c<=d-2 OR c>=d+2)
   AND a>b
   AND (e>c OR e<d);
----

SELECT d-e,
       a+b*2,
       b-c,
       abs(b-c),
       c-d,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a+b*2+c*3
  FROM t1
 WHERE b>c;
----

SELECT a+b*2+c*3
  FROM t1
 WHERE b IS NOT NULL
   AND EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       c-d
  FROM t1;
----

SELECT abs(a),
       b-c,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       (a+b+c+d+e)/5,
       a-b,
       c,
       c-d
  FROM t1
 WHERE a>b
   AND (a>b-2 AND a<b+2)
   AND c>d;
----

SELECT (a+b+c+d+e)/5,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       d-e
  FROM t1;
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       e,
       a
  FROM t1;
----

SELECT b-c,
       e,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE (c<=d-2 OR c>=d+2)
    OR c>d;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a+b*2+c*3,
       a+b*2,
       a,
       a-b,
       b,
       e
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130;
----

SELECT a+b*2+c*3,
       a+b*2+c*3+d*4,
       a+b*2,
       d-e,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
   AND EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
   AND b IS NOT NULL;
----

SELECT d,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       c,
       a+b*2+c*3,
       b
  FROM t1
 WHERE b>c
    OR c>d;
----

SELECT e,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       abs(b-c),
       d
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150
   AND b>c
   AND d>e;
----

SELECT a+b*2
  FROM t1
 WHERE a>b
   AND coalesce(a,b,c,d,e)<>0
   AND a IS NULL;
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a,
       e,
       a+b*2+c*3
  FROM t1
 WHERE (a>b-2 AND a<b+2);
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       e,
       a+b*2+c*3+d*4+e*5,
       a+b*2,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150
    OR coalesce(a,b,c,d,e)<>0
    OR (a>b-2 AND a<b+2);
----

SELECT c-d,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1;
----

SELECT c-d,
       b-c
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
   AND b>c;
----

SELECT b,
       a+b*2+c*3,
       (a+b+c+d+e)/5,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a+b*2+c*3+d*4,
       a+b*2,
       c
  FROM t1
 WHERE d>e;
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       b,
       c-d
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
    OR b IS NOT NULL;
----

SELECT b-c,
       abs(b-c),
       d-e,
       a+b*2,
       c-d
  FROM t1
 WHERE a>b
   AND (e>a AND e<b);
----

SELECT c-d,
       b
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
    OR a IS NULL;
----

SELECT b
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT abs(a)
  FROM t1
 WHERE b IS NOT NULL
    OR a>b;
----

SELECT c,
       b-c,
       a+b*2+c*3+d*4,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a+b*2+c*3,
       a,
       c-d
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0
   AND b IS NOT NULL;
----

SELECT a+b*2+c*3+d*4,
       abs(b-c),
       b-c,
       d-e,
       a+b*2,
       a+b*2+c*3+d*4+e*5
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130
    OR b>c
    OR c BETWEEN b-2 AND d+2;
----

SELECT c-d,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a-b,
       c,
       a+b*2+c*3+d*4
  FROM t1;
----

SELECT d,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a+b*2+c*3+d*4+e*5,
       c-d,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1
 WHERE c BETWEEN b-2 AND d+2;
----

SELECT b,
       a+b*2+c*3+d*4,
       c,
       a+b*2,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1;
----

SELECT (a+b+c+d+e)/5
  FROM t1
 WHERE b IS NOT NULL
   AND b>c;
----

SELECT d-e
  FROM t1
 WHERE (e>a AND e<b);
----

SELECT a+b*2+c*3,
       c,
       a,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a-b
  FROM t1
 WHERE (c<=d-2 OR c>=d+2)
   AND (a>b-2 AND a<b+2);
----

SELECT e,
       c-d,
       (a+b+c+d+e)/5,
       a+b*2,
       b,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       c
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150
   AND (a>b-2 AND a<b+2);
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a-b,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
    OR a>b;
----

SELECT a+b*2+c*3+d*4
  FROM t1;
----

SELECT c-d,
       a,
       b,
       d,
       b-c
  FROM t1;
----

SELECT a,
       c,
       a+b*2+c*3,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0
   AND (c<=d-2 OR c>=d+2);
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       c-d,
       (a+b+c+d+e)/5,
       abs(a)
  FROM t1
 WHERE b IS NOT NULL
   AND (a>b-2 AND a<b+2);
----

SELECT a+b*2,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a-b,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0
   AND e+d BETWEEN a+b-10 AND c+130;
----

SELECT a+b*2+c*3+d*4+e*5,
       abs(a),
       c,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       d
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0
    OR (c<=d-2 OR c>=d+2);
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a+b*2+c*3+d*4,
       c,
       d,
       b
  FROM t1
 WHERE b IS NOT NULL
   AND c BETWEEN b-2 AND d+2;
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a+b*2+c*3,
       d,
       abs(a),
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       b-c
  FROM t1
 WHERE a IS NULL;
----

SELECT a+b*2+c*3+d*4,
       a-b,
       (a+b+c+d+e)/5,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       b-c,
       a,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150;
----

SELECT abs(a)
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
    OR a IS NULL;
----

SELECT abs(a),
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       c,
       e
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0
    OR EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT (a+b+c+d+e)/5,
       a+b*2+c*3,
       a+b*2+c*3+d*4+e*5,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a-b
  FROM t1
 WHERE (c<=d-2 OR c>=d+2);
----

SELECT abs(a),
       (a+b+c+d+e)/5,
       a,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a-b
  FROM t1
 WHERE (a>b-2 AND a<b+2)
   AND c BETWEEN b-2 AND d+2
   AND b>c;
----

SELECT b-c,
       a+b*2,
       d,
       abs(b-c)
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
   AND b>c
   AND (c<=d-2 OR c>=d+2);
----

SELECT d-e,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       abs(a),
       a+b*2+c*3+d*4+e*5,
       b-c
  FROM t1
 WHERE b>c;
----

SELECT b-c
  FROM t1
 WHERE c>d
    OR d>e
    OR EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT c,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a+b*2,
       a-b
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150
    OR e+d BETWEEN a+b-10 AND c+130;
----

SELECT abs(b-c),
       d,
       a+b*2+c*3+d*4+e*5,
       b,
       a
  FROM t1
 WHERE (c<=d-2 OR c>=d+2);
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a,
       a-b,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1
 WHERE a IS NULL
    OR EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT (a+b+c+d+e)/5,
       a+b*2+c*3
  FROM t1;
----

SELECT b,
       abs(a),
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       abs(b-c),
       d-e
  FROM t1
 WHERE (e>a AND e<b)
    OR d>e;
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       c-d,
       e,
       abs(a),
       a+b*2+c*3,
       abs(b-c)
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150
   AND (e>a AND e<b);
----

SELECT d-e,
       a+b*2+c*3+d*4+e*5,
       a
  FROM t1
 WHERE (e>c OR e<d)
   AND a IS NULL
   AND a>b;
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a+b*2,
       c-d,
       b-c,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a+b*2,
       a+b*2+c*3+d*4+e*5,
       (a+b+c+d+e)/5,
       d-e,
       abs(b-c)
  FROM t1
 WHERE b IS NOT NULL;
----

SELECT abs(b-c)
  FROM t1;
----

SELECT b-c
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       b
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0
    OR c>d;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       c,
       a,
       a+b*2+c*3+d*4+e*5,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE (e>c OR e<d)
   AND a IS NULL;
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a+b*2+c*3,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       (a+b+c+d+e)/5,
       a+b*2+c*3+d*4+e*5,
       b
  FROM t1
 WHERE c>d;
----

SELECT d-e,
       a+b*2+c*3+d*4
  FROM t1
 WHERE (c<=d-2 OR c>=d+2)
    OR (e>c OR e<d)
    OR b>c;
----

SELECT abs(b-c),
       a-b,
       d-e,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a+b*2+c*3+d*4,
       a
  FROM t1
 WHERE c>d
    OR b>c;
----

SELECT b,
       a-b,
       a+b*2+c*3+d*4+e*5,
       a+b*2+c*3,
       c,
       c-d
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130;
----

SELECT a,
       a+b*2,
       a-b,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
    OR a IS NULL
    OR d>e;
----

SELECT a+b*2+c*3,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a+b*2+c*3+d*4,
       b,
       b-c,
       d-e
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150;
----

SELECT abs(a),
       (a+b+c+d+e)/5,
       c,
       d-e,
       a+b*2,
       e,
       b
  FROM t1
 WHERE a>b;
----

SELECT a-b,
       abs(b-c),
       a+b*2+c*3
  FROM t1
 WHERE (a>b-2 AND a<b+2)
    OR (e>a AND e<b)
    OR c>d;
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       (a+b+c+d+e)/5,
       a+b*2+c*3+d*4+e*5
  FROM t1;
----

SELECT a+b*2,
       e,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       b,
       c-d
  FROM t1;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       (a+b+c+d+e)/5,
       b
  FROM t1;
----

SELECT d,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
    OR coalesce(a,b,c,d,e)<>0;
----

SELECT a+b*2+c*3+d*4+e*5,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       abs(b-c),
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       (a+b+c+d+e)/5,
       a+b*2+c*3+d*4
  FROM t1;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       c-d,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       d,
       b
  FROM t1;
----

SELECT d
  FROM t1;
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       c-d,
       a,
       b-c,
       a+b*2+c*3+d*4+e*5,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1;
----

SELECT a+b*2+c*3+d*4+e*5
  FROM t1
 WHERE a>b;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a+b*2+c*3+d*4+e*5
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
   AND d>e;
----

SELECT c,
       b-c,
       a+b*2+c*3
  FROM t1
 WHERE b>c
   AND coalesce(a,b,c,d,e)<>0;
----

SELECT a
  FROM t1
 WHERE a IS NULL
    OR b IS NOT NULL;
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a-b,
       b-c,
       c-d,
       abs(b-c),
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
   AND d>e
   AND a>b;
----

SELECT b,
       (a+b+c+d+e)/5
  FROM t1
 WHERE a>b;
----

SELECT (a+b+c+d+e)/5
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a-b,
       c
  FROM t1;
----

SELECT a-b
  FROM t1
 WHERE a>b
   AND c BETWEEN b-2 AND d+2
   AND (e>c OR e<d);
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       c-d,
       b,
       a+b*2+c*3+d*4+e*5,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       abs(b-c),
       a-b
  FROM t1
 WHERE d>e
    OR (a>b-2 AND a<b+2);
----

SELECT a+b*2+c*3+d*4,
       e,
       a+b*2+c*3
  FROM t1
 WHERE a>b;
----

SELECT b,
       d,
       b-c
  FROM t1
 WHERE a IS NULL
    OR EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a+b*2+c*3,
       a+b*2+c*3+d*4,
       a-b,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       abs(b-c),
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130
    OR (c<=d-2 OR c>=d+2);
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE (a>b-2 AND a<b+2)
   AND (e>c OR e<d);
----

SELECT c-d,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       (a+b+c+d+e)/5,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       c,
       a,
       a+b*2+c*3+d*4+e*5
  FROM t1
 WHERE (e>a AND e<b)
    OR b>c
    OR (c<=d-2 OR c>=d+2);
----

SELECT d-e
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0
   AND e+d BETWEEN a+b-10 AND c+130
   AND a IS NULL;
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a+b*2+c*3+d*4,
       a+b*2+c*3+d*4+e*5,
       a,
       (a+b+c+d+e)/5,
       e,
       d
  FROM t1;
----

SELECT a
  FROM t1;
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE c>d
    OR EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
    OR (a>b-2 AND a<b+2);
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       c,
       b
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130
    OR b IS NOT NULL;
----

SELECT d,
       c-d,
       a+b*2
  FROM t1
 WHERE (e>c OR e<d)
    OR d>e
    OR a>b;
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       c,
       abs(a),
       a+b*2+c*3+d*4,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a-b
  FROM t1;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       e,
       a+b*2+c*3+d*4+e*5,
       a+b*2+c*3,
       a-b,
       b-c,
       d
  FROM t1
 WHERE b>c;
----

SELECT a+b*2,
       abs(b-c),
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       d,
       (a+b+c+d+e)/5
  FROM t1;
----

SELECT a,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE (e>c OR e<d)
   AND (a>b-2 AND a<b+2);
----

SELECT e,
       b-c,
       a+b*2,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       d-e,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       (a+b+c+d+e)/5
  FROM t1
 WHERE a>b
   AND e+d BETWEEN a+b-10 AND c+130
   AND d>e;
----

SELECT d,
       b-c
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
   AND d>e;
----

SELECT b-c,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       c,
       a+b*2,
       d-e,
       a+b*2+c*3+d*4,
       c-d
  FROM t1
 WHERE a IS NULL
   AND c>d;
----

SELECT b,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a+b*2
  FROM t1;
----

SELECT e,
       b-c,
       abs(b-c),
       abs(a),
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       d
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130
    OR (e>a AND e<b);
----

SELECT d,
       c-d,
       abs(b-c),
       b-c,
       a+b*2+c*3
  FROM t1
 WHERE b>c;
----

SELECT a-b,
       a,
       a+b*2+c*3,
       abs(a),
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       b-c
  FROM t1
 WHERE b IS NOT NULL
   AND (e>c OR e<d)
   AND c>d;
----

SELECT abs(b-c),
       e,
       a+b*2,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       abs(a),
       c-d,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1;
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       d,
       e,
       c-d,
       a,
       abs(a),
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1;
----

SELECT a+b*2+c*3+d*4+e*5,
       a+b*2+c*3,
       b,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       abs(b-c),
       (a+b+c+d+e)/5
  FROM t1;
----

SELECT b,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       abs(b-c),
       a+b*2+c*3
  FROM t1
 WHERE (c<=d-2 OR c>=d+2)
    OR coalesce(a,b,c,d,e)<>0;
----

SELECT abs(a),
       c,
       (a+b+c+d+e)/5,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1
 WHERE a IS NULL;
----

SELECT abs(b-c),
       d,
       a+b*2+c*3+d*4+e*5,
       a-b,
       c-d,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       (a+b+c+d+e)/5
  FROM t1
 WHERE d>e;
----

SELECT c,
       a-b,
       a+b*2+c*3+d*4+e*5,
       e,
       abs(a)
  FROM t1;
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       b,
       c,
       a+b*2
  FROM t1;
----

SELECT a-b,
       b-c,
       a+b*2,
       c-d
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
    OR d NOT BETWEEN 110 AND 150;
----

SELECT a+b*2+c*3,
       a+b*2,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       d-e
  FROM t1
 WHERE (e>c OR e<d);
----

SELECT (a+b+c+d+e)/5,
       c-d
  FROM t1
 WHERE d>e
   AND c>d
   AND a IS NULL;
----

SELECT b,
       a-b,
       d-e,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1
 WHERE c BETWEEN b-2 AND d+2;
----

SELECT a+b*2+c*3+d*4,
       b,
       a,
       e,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0
    OR b>c;
----

SELECT b-c,
       a,
       (a+b+c+d+e)/5,
       c-d,
       a+b*2+c*3,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
    OR d NOT BETWEEN 110 AND 150
    OR (e>c OR e<d);
----

SELECT a-b,
       c-d,
       (a+b+c+d+e)/5,
       a+b*2+c*3+d*4+e*5,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       d
  FROM t1;
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a+b*2+c*3+d*4+e*5,
       a+b*2+c*3+d*4
  FROM t1;
----

SELECT c
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
    OR c>d
    OR a>b;
----

SELECT a+b*2+c*3+d*4,
       b,
       abs(a)
  FROM t1;
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END
  FROM t1
 WHERE b>c
   AND c>d
   AND EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       d,
       (a+b+c+d+e)/5,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE b>c;
----

SELECT (a+b+c+d+e)/5,
       d-e,
       a+b*2+c*3+d*4,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1
 WHERE (a>b-2 AND a<b+2)
    OR d>e
    OR b>c;
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a+b*2+c*3+d*4,
       a+b*2
  FROM t1
 WHERE b>c;
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE (e>c OR e<d)
    OR b>c
    OR e+d BETWEEN a+b-10 AND c+130;
----

SELECT d-e,
       a,
       d
  FROM t1
 WHERE (e>c OR e<d)
   AND a IS NULL;
----

SELECT a,
       (a+b+c+d+e)/5,
       b-c,
       a+b*2,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1
 WHERE (e>c OR e<d);
----

SELECT abs(a),
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       e
  FROM t1
 WHERE b IS NOT NULL;
----

SELECT d-e,
       a-b,
       c,
       c-d,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END
  FROM t1;
----

SELECT abs(b-c),
       a+b*2+c*3,
       a+b*2+c*3+d*4,
       abs(a),
       a,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130;
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       b,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1;
----

SELECT a+b*2,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       (a+b+c+d+e)/5,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
    OR EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       abs(a),
       a+b*2+c*3+d*4,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       b-c
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
   AND c>d;
----

SELECT d-e,
       a,
       a+b*2+c*3+d*4+e*5,
       abs(a),
       b
  FROM t1
 WHERE (c<=d-2 OR c>=d+2)
   AND c>d
   AND d NOT BETWEEN 110 AND 150;
----

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a+b*2+c*3+d*4+e*5,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150
   AND c>d;
----

SELECT c-d,
       e
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0;
----

SELECT a+b*2+c*3+d*4,
       d-e,
       abs(a),
       a+b*2,
       c
  FROM t1;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0
    OR (c<=d-2 OR c>=d+2);
----

SELECT a-b,
       a+b*2+c*3+d*4+e*5,
       a+b*2,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       c-d,
       b-c
  FROM t1;
----

SELECT abs(a),
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       (a+b+c+d+e)/5
  FROM t1
 WHERE (e>c OR e<d)
   AND a IS NULL
   AND e+d BETWEEN a+b-10 AND c+130;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1;
----

SELECT a+b*2+c*3,
       c-d,
       a+b*2+c*3+d*4+e*5,
       a+b*2,
       abs(a),
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       e
  FROM t1;
----

SELECT abs(a),
       c,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1
 WHERE (e>c OR e<d)
    OR c>d;
----

SELECT d,
       b-c,
       c-d,
       a+b*2+c*3+d*4,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE (e>c OR e<d);
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       b,
       abs(a),
       a+b*2+c*3+d*4
  FROM t1
 WHERE b>c
   AND (e>a AND e<b)
   AND b IS NOT NULL;
----

SELECT a+b*2,
       abs(a)
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150
   AND a IS NULL;
----

SELECT c-d,
       abs(b-c),
       a-b,
       a
  FROM t1;
----

SELECT a,
       a+b*2+c*3+d*4+e*5,
       e,
       a+b*2,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       abs(b-c)
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0;
----

SELECT abs(b-c),
       d,
       abs(a),
       a-b,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       c-d,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE b>c
    OR c>d;
----

SELECT a+b*2+c*3+d*4+e*5,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       b-c,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       e,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1;
----

SELECT a,
       a+b*2+c*3+d*4,
       c,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1
 WHERE a>b
   AND c>d
   AND b IS NOT NULL;
----

SELECT c
  FROM t1
 WHERE (e>a AND e<b);
----

SELECT (a+b+c+d+e)/5,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
   AND d>e
   AND c>d;
----

SELECT b-c
  FROM t1
 WHERE (c<=d-2 OR c>=d+2)
   AND d NOT BETWEEN 110 AND 150
   AND a>b;
----

SELECT e,
       a-b,
       b-c,
       d-e,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1
 WHERE a IS NULL
    OR (e>a AND e<b);
----

SELECT a+b*2,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       c-d,
       a+b*2+c*3+d*4+e*5
  FROM t1
 WHERE (c<=d-2 OR c>=d+2)
    OR c BETWEEN b-2 AND d+2;
----

SELECT d-e,
       a-b,
       b-c
  FROM t1;
----

SELECT c
  FROM t1
 WHERE b>c
    OR b IS NOT NULL
    OR EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT a+b*2+c*3+d*4+e*5,
       e,
       d-e,
       a-b,
       a+b*2+c*3,
       a
  FROM t1
 WHERE c>d
    OR d>e;
----

SELECT a+b*2+c*3,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1
 WHERE (a>b-2 AND a<b+2)
    OR c>d;
----

SELECT c,
       a+b*2+c*3,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       b-c,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a-b,
       d
  FROM t1
 WHERE (c<=d-2 OR c>=d+2)
    OR d NOT BETWEEN 110 AND 150;
----

SELECT a+b*2+c*3+d*4,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       abs(b-c),
       e,
       a+b*2+c*3+d*4+e*5
  FROM t1
 WHERE (e>c OR e<d);
----

SELECT (a+b+c+d+e)/5,
       d,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1
 WHERE (c<=d-2 OR c>=d+2)
    OR c BETWEEN b-2 AND d+2
    OR a>b;
----

SELECT a+b*2+c*3+d*4,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       (a+b+c+d+e)/5,
       e
  FROM t1
 WHERE (a>b-2 AND a<b+2)
   AND b>c;
----

SELECT a+b*2+c*3,
       a+b*2,
       e,
       c,
       abs(a),
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1
 WHERE (e>c OR e<d)
   AND c>d;
----

SELECT d,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       c,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       e,
       a+b*2+c*3+d*4+e*5
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130;
----

SELECT a+b*2+c*3+d*4,
       a-b
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130
    OR (e>c OR e<d)
    OR c>d;
----

SELECT (a+b+c+d+e)/5
  FROM t1
 WHERE (e>c OR e<d);
----

SELECT a+b*2,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       abs(b-c),
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END
  FROM t1;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       (a+b+c+d+e)/5,
       c,
       a,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE c>d;
----

SELECT a+b*2+c*3,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a-b,
       a+b*2+c*3+d*4+e*5,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       b
  FROM t1
 WHERE d>e
   AND EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT a+b*2+c*3,
       b,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       (a+b+c+d+e)/5,
       a,
       d
  FROM t1;
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a+b*2,
       (a+b+c+d+e)/5,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a-b,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE a>b
    OR c BETWEEN b-2 AND d+2
    OR e+d BETWEEN a+b-10 AND c+130;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d)
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
   AND d>e;
----

SELECT a+b*2+c*3+d*4+e*5,
       a+b*2+c*3,
       d
  FROM t1
 WHERE b>c
   AND (c<=d-2 OR c>=d+2);
----

SELECT a-b
  FROM t1;
----

SELECT c-d,
       abs(a),
       a,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       b
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
    OR b>c
    OR c>d;
----

SELECT c-d,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a+b*2+c*3+d*4+e*5,
       d
  FROM t1
 WHERE (a>b-2 AND a<b+2)
    OR EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
    OR b>c;
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       b-c,
       abs(b-c),
       e,
       a+b*2+c*3,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       (a+b+c+d+e)/5
  FROM t1
 WHERE (a>b-2 AND a<b+2)
    OR a IS NULL
    OR d NOT BETWEEN 110 AND 150;
----

SELECT a+b*2+c*3+d*4,
       (a+b+c+d+e)/5,
       abs(b-c)
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
    OR (e>a AND e<b)
    OR d NOT BETWEEN 110 AND 150;
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       abs(a),
       c-d
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130
    OR (c<=d-2 OR c>=d+2)
    OR b>c;
----

SELECT a-b,
       d-e,
       abs(b-c),
       e,
       a+b*2,
       b-c
  FROM t1
 WHERE a IS NULL
   AND b>c
   AND (a>b-2 AND a<b+2);
----

SELECT b-c,
       a+b*2+c*3,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END
  FROM t1
 WHERE b>c
    OR e+d BETWEEN a+b-10 AND c+130
    OR (a>b-2 AND a<b+2);
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       c-d,
       e
  FROM t1
 WHERE c BETWEEN b-2 AND d+2;
----

SELECT d,
       a,
       b
  FROM t1;
----

SELECT b,
       a,
       a+b*2+c*3+d*4,
       (a+b+c+d+e)/5
  FROM t1
 WHERE b>c
   AND (a>b-2 AND a<b+2)
   AND e+d BETWEEN a+b-10 AND c+130;
----

SELECT a,
       a+b*2,
       b-c,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       b,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END
  FROM t1
 WHERE (e>a AND e<b);
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       b-c,
       c-d,
       a+b*2+c*3,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE d>e
    OR a IS NULL
    OR d NOT BETWEEN 110 AND 150;
----

SELECT abs(a),
       d,
       a+b*2+c*3,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       c
  FROM t1
 WHERE a IS NULL
   AND c>d
   AND a>b;
----

SELECT abs(b-c),
       (a+b+c+d+e)/5,
       a-b,
       c,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       b-c,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1
 WHERE a IS NULL
   AND coalesce(a,b,c,d,e)<>0;
----

SELECT b-c,
       abs(a)
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0
    OR (e>a AND e<b);
----

SELECT b-c
  FROM t1
 WHERE (c<=d-2 OR c>=d+2)
    OR (e>a AND e<b)
    OR coalesce(a,b,c,d,e)<>0;
----

SELECT a+b*2+c*3+d*4+e*5,
       a,
       a+b*2+c*3+d*4,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 WHERE (c<=d-2 OR c>=d+2)
    OR EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT a+b*2+c*3
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
   AND a IS NULL
   AND coalesce(a,b,c,d,e)<>0;
----

SELECT e,
       a+b*2,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       b
  FROM t1;
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       abs(a),
       a+b*2,
       a-b,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       d-e
  FROM t1;
----

SELECT (a+b+c+d+e)/5,
       a+b*2+c*3,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       a-b,
       abs(b-c)
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150
   AND c>d;
----

SELECT abs(a)
  FROM t1
 WHERE c>d;
----

SELECT e,
       a+b*2,
       c,
       d-e
  FROM t1;
----

SELECT d-e,
       d,
       a,
       a+b*2
  FROM t1;
----

SELECT a
  FROM t1
 WHERE c>d;
----

SELECT c-d,
       a+b*2+c*3,
       a+b*2+c*3+d*4+e*5,
       a,
       b,
       a+b*2+c*3+d*4
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150;
----

SELECT CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       abs(a),
       a+b*2+c*3
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0;
----

SELECT (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       abs(a),
       b,
       a-b,
       a+b*2+c*3+d*4
  FROM t1
 WHERE (a>b-2 AND a<b+2)
   AND (e>c OR e<d)
   AND a>b;
----

SELECT a+b*2+c*3+d*4,
       a+b*2+c*3+d*4+e*5,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d),
       (a+b+c+d+e)/5
  FROM t1
 WHERE a>b
    OR (a>b-2 AND a<b+2);
----

SELECT d,
       abs(b-c),
       b-c,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       b
  FROM t1
 WHERE (c<=d-2 OR c>=d+2)
   AND a IS NULL
   AND c>d;
----

SELECT b,
       d-e,
       a+b*2,
       b-c
  FROM t1;
----

SELECT c,
       e,
       a+b*2,
       abs(b-c),
       b-c,
       a
  FROM t1
 WHERE a IS NULL
   AND (e>a AND e<b)
   AND b IS NOT NULL;
----

SELECT (a+b+c+d+e)/5,
       a+b*2,
       a+b*2+c*3+d*4+e*5,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       a+b*2+c*3,
       a-b,
       a
  FROM t1
 WHERE b>c;
----

SELECT e,
       b-c,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a-b,
       abs(b-c),
       abs(a)
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150
    OR b>c;
----

SELECT a-b,
       c,
       b,
       b-c,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       (a+b+c+d+e)/5,
       d
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0
    OR (c<=d-2 OR c>=d+2);
----

SELECT abs(b-c),
       d-e,
       c-d,
       a+b*2+c*3+d*4+e*5,
       d
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0
    OR c>d
    OR d>e;
----

SELECT a-b,
       (a+b+c+d+e)/5,
       abs(b-c),
       c,
       abs(a),
       e,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1
 WHERE d NOT BETWEEN 110 AND 150
    OR EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
    OR a IS NULL;
----

SELECT a+b*2,
       c,
       e,
       b
  FROM t1
 WHERE (e>a AND e<b)
    OR coalesce(a,b,c,d,e)<>0;
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       d,
       (a+b+c+d+e)/5,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)
  FROM t1
 WHERE a>b
   AND (e>a AND e<b);
----

SELECT a+b*2+c*3+d*4+e*5
  FROM t1
 WHERE (a>b-2 AND a<b+2)
    OR d NOT BETWEEN 110 AND 150
    OR EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT b-c,
       a+b*2+c*3+d*4+e*5
  FROM t1;
----

SELECT CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b);
----

SELECT a-b,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a
  FROM t1;
----

SELECT abs(b-c)
  FROM t1
 WHERE coalesce(a,b,c,d,e)<>0
   AND b IS NOT NULL;
----

SELECT c-d,
       abs(b-c)
  FROM t1
 WHERE (c<=d-2 OR c>=d+2)
   AND e+d BETWEEN a+b-10 AND c+130
   AND b IS NOT NULL;
----

SELECT (a+b+c+d+e)/5,
       e,
       c-d,
       a+b*2+c*3,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       a-b
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
    OR coalesce(a,b,c,d,e)<>0
    OR a IS NULL;
----

SELECT a-b,
       a
  FROM t1
 WHERE c>d;
----

SELECT (a+b+c+d+e)/5,
       a+b*2+c*3+d*4+e*5,
       d-e,
       a+b*2,
       CASE a+1 WHEN b THEN 111 WHEN c THEN 222
        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,
       c
  FROM t1
 WHERE c BETWEEN b-2 AND d+2
   AND b>c
   AND a>b;
----

SELECT a+b*2+c*3
  FROM t1
 WHERE e+d BETWEEN a+b-10 AND c+130
    OR c>d
    OR a IS NULL;
----

