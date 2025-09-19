CREATE TABLE nexmark_q19 (
    auction  BIGINT,
    bidder  BIGINT,
    price  BIGINT,
    channel  VARCHAR,
    url  VARCHAR,
    `dateTime`  TIMESTAMP(3),
    extra  VARCHAR,
    rank_number  BIGINT
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q19
SELECT * FROM
(SELECT *, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) AS rank_number FROM bid)
WHERE rank_number <= 10;
