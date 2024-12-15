-------------------- Fact: Transactions -------------------- 
INSERT INTO 
	public.sskr_dwh_fact_transactions(
        trans_id
        , trans_date
        , card_num
        , oper_type
        , amt
        , oper_result
        , terminal
    )
SELECT 
	trans_id
    , trans_date
    , card_num
    , oper_type
    , amt
    , oper_result
    , terminal
FROM
	public.sskr_stg_transactions
;

COMMIT;
