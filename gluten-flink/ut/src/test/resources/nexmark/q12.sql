-- -------------------------------------------------------------------------------------------------
-- Query 12: Processing Time Windows (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- How many bids does a user make within a fixed processing time limit?
-- Illustrates working in processing time window.
--
-- Group bids by the same user into processing time windows of 10 seconds.
-- Emit the count of bids per window.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE nexmark_q12 (
  bidder BIGINT,
  bid_count BIGINT,
  starttime TIMESTAMP(3),
  endtime TIMESTAMP(3)
) WITH (
  'connector' = 'blackhole'
);

CREATE VIEW B AS SELECT *, PROCTIME() as p_time FROM bid;

INSERT INTO nexmark_q12
SELECT
    bidder,
    count(*) as bid_count,
    window_start AS starttime,
    window_end AS endtime
FROM TABLE(
        TUMBLE(TABLE B, DESCRIPTOR(p_time), INTERVAL '10' SECOND))
GROUP BY bidder, window_start, window_end;