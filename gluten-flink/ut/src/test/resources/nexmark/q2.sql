CREATE TABLE nexmark_q2 (
  auction  BIGINT,
  price  BIGINT
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q2
SELECT auction, price FROM bid WHERE MOD(auction, 123) = 0;
