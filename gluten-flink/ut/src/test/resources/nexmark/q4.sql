CREATE TABLE nexmark_q4 (
  id BIGINT,
  final BIGINT
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q4
SELECT
    Q.category,
    AVG(Q.final)
FROM (
    SELECT MAX(B.price) AS final, A.category
    FROM auction A, bid B
    WHERE A.id = B.auction AND B.`dateTime` BETWEEN A.`dateTime` AND A.expires
    GROUP BY A.id, A.category
) Q
GROUP BY Q.category;
