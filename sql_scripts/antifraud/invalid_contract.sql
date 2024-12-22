INSERT INTO 
    public.sskr_rep_fraud (
        event_dt
        , passport 
        , fio
        , phone
        , event_type
        , report_dt
    )
SELECT 
    t.trans_date                                                    AS event_dt
    , c.passport_num                                                AS passport
    , CONCAT(c.last_name, ' ', c.first_name, ' ', c.patronymic)     AS fio
    , c.phone                                                       AS phone
    , 'Недействующий договор'                                       AS event_type
    , t.trans_date::DATE                                            AS report_dt
FROM
    public.sskr_stg_transactions t
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
WHERE 
    1 = 1
    AND acc.valid_to < t.trans_date
;

COMMIT;
