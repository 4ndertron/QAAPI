-- Calls over X running days
WITH
    rawd AS
    (
        SELECT
            TO_DATE(call_start) AS dt
          , COUNT(call_session_id) AS tally
        FROM
            cjp.t_calls
        WHERE
            dt IS NOT NULL
        GROUP BY
            dt
        ORDER BY
            dt
    )

SELECT
    *
  , SUM(tally) OVER (ORDER BY dt ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS running_tally
FROM
    rawd
ORDER BY
    running_tally DESC
;