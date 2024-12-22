-------------------- SCD1 Increment Load: Cards -------------------- 

-- 1. For further deletion from target table

INSERT INTO
	public.sskr_stg_del_cards ( card_num )
SELECT
	sc.card_num
FROM 
	public.sskr_stg_cards sc
LEFT JOIN 
	public.sskr_stg_del_cards sdc
ON
  	sc.card_num = sdc.card_num
WHERE 
	1 = 1
	AND sdc.card_num IS NULL
;

-- 2. Remove unnecessary data

DELETE FROM 
	public.sskr_stg_cards
WHERE
	1 = 1
	AND update_dt < ( 
		SELECT 
			max_update_dt 
		FROM 
			public.sskr_meta_info 
		WHERE 
			1 = 1
			AND schema_name='info'
			AND table_name='cards_scd1' 
	)
;

-- 3. Insert new data to target

INSERT INTO 
	public.sskr_dwh_dim_cards( 
		card_num
		, account_num
		, create_dt
		, update_dt 
	)
SELECT 
	stg.card_num
	, stg.account_num
	, stg.create_dt	
	, NULL 
FROM public.sskr_stg_cards stg
LEFT JOIN 
	public.sskr_dwh_dim_cards tgt
ON 
	stg.card_num = tgt.card_num
WHERE
	1 = 1
	AND tgt.card_num IS NULL
;

-- 4. Update values

UPDATE 
	public.sskr_dwh_dim_cards
SET 
	account_num = tmp.account_num
	, update_dt = tmp.update_dt
FROM (
	SELECT 
		stg.card_num
		, stg.account_num 
		, stg.update_dt	
	FROM 
		public.sskr_stg_cards stg
	INNER JOIN 
		public.sskr_dwh_dim_cards tgt
	ON 
		stg.card_num = tgt.card_num
	WHERE 
		1 = 0
		OR stg.account_num <> tgt.account_num
) tmp
WHERE
	1 = 1
	AND public.sskr_dwh_dim_cards.card_num = tmp.card_num
;

-- 5. Delete rows that no longer exist in source

DELETE FROM 
	public.sskr_dwh_dim_cards
WHERE
	1 = 1
	AND card_num IN (
		SELECT
			tgt.card_num
		FROM
			public.sskr_dwh_dim_cards tgt
		LEFT JOIN
			public.sskr_stg_del_cards stg
		ON
			stg.card_num = tgt.card_num
		WHERE
			1 = 1
			AND stg.card_num IS NULL
	)
;

-- 6. Metadata update

UPDATE 
	public.sskr_meta_info
SET
	max_update_dt = COALESCE( 
		(
			SELECT MAX( update_dt ) 
			FROM public.sskr_stg_cards 
		)
		, (
			SELECT max_update_dt 
			FROM public.sskr_meta_info 
			WHERE 
				1 = 1
				AND schema_name = 'info' 
				AND table_name = 'cards_scd1' 
		)
	)
WHERE 
	1 = 1
	AND schema_name = 'info' 
	AND table_name = 'cards_scd1'
;

COMMIT;
