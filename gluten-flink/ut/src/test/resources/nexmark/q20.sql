CREATE TABLE nexmark_q20 (
    auction  BIGINT,
    bidder  BIGINT,
    price  BIGINT,
    channel  VARCHAR,
    url  VARCHAR,
    bid_dateTime  TIMESTAMP(3),
    bid_extra  VARCHAR,

    itemName  VARCHAR,
    description  VARCHAR,
    initialBid  BIGINT,
    reserve  BIGINT,
    auction_dateTime  TIMESTAMP(3),
    expires  TIMESTAMP(3),
    seller  BIGINT,
    category  BIGINT,
    auction_extra  VARCHAR
) WITH (
    'connector' = 'blackhole'
);

INSERT INTO nexmark_q20
SELECT
    auction, bidder, price, channel, url, B.`dateTime`, B.extra,
    itemName, description, initialBid, reserve, A.`dateTime`, expires, seller, category, A.extra
FROM
    bid AS B INNER JOIN auction AS A on B.auction = A.id
WHERE A.category = 10;
