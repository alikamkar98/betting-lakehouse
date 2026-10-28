-- Business questions on the gold layer, answered with window functions.
-- Run them in the Databricks SQL editor or with the notebook 05_analysis.

-- 1. Top 3 customers by turnover per partner (DENSE_RANK: ties share a rank, no gaps)
WITH customer_turnover AS (
    SELECT partner_id, customer_id, SUM(stake) AS turnover
    FROM workspace.betting.fact_bet
    GROUP BY partner_id, customer_id
)
SELECT partner_id, customer_id, turnover, turnover_rank
FROM (
    SELECT *, DENSE_RANK() OVER (PARTITION BY partner_id ORDER BY turnover DESC) AS turnover_rank
    FROM customer_turnover
)
WHERE turnover_rank <= 3
ORDER BY partner_id, turnover_rank;

-- 2. Daily turnover and change versus the previous day (LAG)
WITH daily AS (
    SELECT bet_date, SUM(turnover) AS turnover
    FROM workspace.betting.gold_partner_daily_kpis
    GROUP BY bet_date
)
SELECT bet_date,
       turnover,
       turnover - LAG(turnover) OVER (ORDER BY bet_date) AS change_vs_previous_day
FROM daily
ORDER BY bet_date;

-- 3. Running stake per customer over time (SUM OVER with ORDER BY = running total)
SELECT customer_id,
       placed_at,
       stake,
       SUM(stake) OVER (PARTITION BY customer_id ORDER BY placed_at, bet_id) AS running_stake
FROM workspace.betting.fact_bet
WHERE customer_id = 'C-001'
ORDER BY placed_at;

-- 4. Customers with no bet on the last day of data (NOT EXISTS, safe with NULLs unlike NOT IN)
SELECT c.customer_id, c.partner_id
FROM workspace.betting.dim_customer AS c
WHERE c.is_current
  AND NOT EXISTS (
      SELECT 1
      FROM workspace.betting.fact_bet AS b
      WHERE b.customer_id = c.customer_id
        AND b.bet_date = (SELECT MAX(bet_date) FROM workspace.betting.fact_bet)
  )
ORDER BY c.customer_id;

-- 5. GGR per event, settled bets only, biggest losers for the book first
SELECT e.event_id,
       e.sport,
       e.home_team || ' vs ' || e.away_team AS match_name,
       SUM(b.stake) AS settled_turnover,
       SUM(b.payout) AS payouts,
       SUM(b.stake) - SUM(b.payout) AS ggr
FROM workspace.betting.fact_bet AS b
JOIN workspace.betting.dim_event AS e ON b.event_id = e.event_id
WHERE b.is_settled
GROUP BY e.event_id, e.sport, e.home_team, e.away_team
ORDER BY ggr ASC
LIMIT 10;
