CREATE TABLE bid_source (
    auction BIGINT,
    bidder BIGINT,
    price BIGINT,
    dateTime TIMESTAMP(3),
    extra STRING,
    WATERMARK FOR dateTime AS dateTime - INTERVAL '4' SECOND
) WITH (
    'connector' = 'datagen',
    'number-of-rows' = '5'
)