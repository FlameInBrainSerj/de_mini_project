INSERT INTO 
    public.sskr_rep_fraud (
        event_dt
        , passport 
        , fio
        , phone
        , event_type
        , report_dt
    )
WITH common_table AS (
    SELECT 
        t.trans_date                                                    AS event_dt
        , c.passport_num                                                AS passport
        , CONCAT(c.last_name, ' ', c.first_name, ' ', c.patronymic)     AS fio
        , c.phone                                                       AS phone
        , t.trans_id                                                    AS trans_id
        , c.client_id                                                   AS client_id
        , ter.terminal_city                                             AS terminal_city
    FROM
        public.sskr_stg_transactions t
    LEFT JOIN
        public.sskr_dwh_dim_terminals ter
    ON
        t.terminal = ter.terminal_id
    LEFT JOIN 
        public.sskr_dwh_dim_cards cr 
    ON 
        t.card_num = cr.card_num
    LEFT JOIN 
        public.sskr_dwh_dim_accounts acc 
    ON 
        cr.account_num = acc.account_num
    LEFT JOIN 
        public.sskr_dwh_dim_clients c 
    ON 
        acc.client = c.client_id
)
SELECT
    t1.event_dt
    , t1.passport
    , t1.fio
    , t1.phone
    , 'Операции в разных городах за короткое время'                      AS event_type
    , t1.event_dt::DATE                                                AS report_dt
FROM 
    common_table t1
INNER JOIN
    common_table t2
ON
    1 = 1
    AND t1.client_id = t2.client_id 
    AND t1.trans_id <> t2.trans_id
    AND t1.terminal_city <> t2.terminal_city
WHERE 
    ABS(EXTRACT(EPOCH FROM (t1.event_dt - t2.event_dt))) <= 3600
;

COMMIT;
