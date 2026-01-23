-- TRANSFORM FROM KYRIBA_POSITION_DATA_HISTORY
-- MIGRATED FROM GEN2 VIEW COMPONENT

WITH LATEST AS (
  SELECT
  "account",
  "accountnumber",
  "currency",
  "amount",
  "importdate"
FROM
  {{ ref("kyriba_position_data_history") }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY "account", "importdate"::DATE ORDER BY "importdate" ASC)=1   
)
SELECT
  k."account",
  k."accountnumber",
  k."currency",
  k."amount",
  k."importdate"
FROM
  {{ ref("kyriba_position_data_history") }} AS k
  JOIN LATEST l ON k."account" = l."account" AND k."importdate" = l."importdate"
  ORDER BY k."importdate" DESC, k."account"