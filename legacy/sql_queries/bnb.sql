WITH parameters AS (
  SELECT
    CURRENT_TIMESTAMP AS today,
    CURRENT_TIMESTAMP - INTERVAL '{{perf_window}}' HOUR AS perf_window_start,
    CURRENT_TIMESTAMP - INTERVAL '{{early_window}}' HOUR AS early_window_start,
    FROM_HEX('{{token_address}}') AS token_address
),

early_buyers AS (
  SELECT
    evt.to AS wallet,
    MIN(evt.evt_block_time) AS first_buy_date,
    COUNT(*) AS number_of_trades_early,
    SUM(evt.value / POWER(10, tok.decimals)) AS total_tokens_received_early
  FROM erc20_bnb.evt_Transfer AS evt
  JOIN tokens.erc20 AS tok
    ON evt.contract_address = tok.contract_address
  JOIN parameters AS p
    ON evt.contract_address = p.token_address
  WHERE
    evt.evt_block_time BETWEEN p.early_window_start AND p.perf_window_start
  GROUP BY evt.to
),

wallet_total_tx AS (
  SELECT
    address,
    COUNT(*) AS nb_total_transfers
  FROM (
    SELECT evt.to AS address
    FROM erc20_bnb.evt_Transfer AS evt
    JOIN parameters AS p
      ON evt.contract_address = p.token_address
    UNION ALL
    SELECT evt."from" AS address
    FROM erc20_bnb.evt_Transfer AS evt
    JOIN parameters AS p
      ON evt.contract_address = p.token_address
  ) AS t
  GROUP BY address
),

full_balances AS (
  SELECT
    wallet,
    SUM(received) AS total_bought,
    SUM(sent) AS total_sold,
    SUM(received) - SUM(sent) AS current_balance
  FROM (
    SELECT
      evt.to AS wallet,
      SUM(evt.value / POWER(10, tok.decimals)) AS received,
      0 AS sent
    FROM erc20_bnb.evt_Transfer AS evt
    JOIN tokens.erc20 AS tok
      ON evt.contract_address = tok.contract_address
    JOIN parameters AS p
      ON evt.contract_address = p.token_address
    GROUP BY evt.to

    UNION ALL

    SELECT
      evt."from" AS wallet,
      0 AS received,
      SUM(evt.value / POWER(10, tok.decimals)) AS sent
    FROM erc20_bnb.evt_Transfer AS evt
    JOIN tokens.erc20 AS tok
      ON evt.contract_address = tok.contract_address
    JOIN parameters AS p
      ON evt.contract_address = p.token_address
    GROUP BY evt."from"
  ) AS agg
  GROUP BY wallet
),

final AS (
  SELECT
    eb.wallet,
    eb.first_buy_date,
    eb.number_of_trades_early,
    wt.nb_total_transfers,
    eb.total_tokens_received_early,
    fb.total_bought,
    fb.total_sold,
    fb.current_balance,
    -- Calcul du profit réalisé et potentiel
    CASE
      WHEN fb.total_bought > 0 THEN fb.total_sold / fb.total_bought
      ELSE 0
    END AS sell_ratio,
    fb.current_balance + fb.total_sold AS total_exposure
  FROM early_buyers AS eb
  JOIN full_balances AS fb
    ON eb.wallet = fb.wallet
  JOIN wallet_total_tx AS wt
    ON eb.wallet = wt.address
  WHERE
    wt.nb_total_transfers <= 200
    AND (
      fb.current_balance > 0.1  -- Wallets qui détiennent encore
      OR (fb.total_sold > 0 AND fb.total_sold / fb.total_bought >= 1.05)  -- OU wallets qui ont vendu avec 5%+ profit
    )
)

SELECT
  wallet,
  first_buy_date,
  number_of_trades_early,
  nb_total_transfers,
  total_tokens_received_early,
  total_bought,
  total_sold,
  current_balance,
  sell_ratio,
  total_exposure
FROM final
ORDER BY total_exposure DESC  -- Tri par exposition totale (position + profits réalisés)
LIMIT 1000  -- Augmenté de 300 à 1000
