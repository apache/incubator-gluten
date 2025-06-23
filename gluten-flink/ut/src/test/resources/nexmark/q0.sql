CREATE TABLE nexmark_q0 (
  auction  BIGINT,
  bidder  BIGINT,
  price  BIGINT,
  `dateTime`  TIMESTAMP(3),
  extra  VARCHAR
) WITH (
  'connector' = 'blackhole'
);


INSERT INTO nexmark_q0
SELECT auction, bidder, price, `dateTime`, extra FROM bid;
