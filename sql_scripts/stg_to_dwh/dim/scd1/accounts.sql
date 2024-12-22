-------------------- SCD1 Increment Load: Accounts -------------------- 

-- 1. For further deletion from target table

INSERT INTO
	public.sskr_stg_del_accounts ( account_num )
SELECT
	sa.account_num
FROM
	public.sskr_stg_accounts sa
LEFT JOIN 
	public.sskr_stg_del_accounts sda
ON 
  	sa.account_num = sda.account_num
WHERE 
	1 = 1
	AND sda.account_num IS NULL
;

-- 2. Remove unnecessary data

DELETE FROM 
	public.sskr_stg_accounts
WHERE
	1 = 1
	AND update_dt < ( 
		SELECT
			max_update_dt
		FROM 
			public.sskr_meta_info 
		WHERE 
			1 = 1
			AND schema_name = 'info'
			AND table_name = 'accounts_scd1' 
	)
;

-- 3. Insert new data to target

INSERT INTO 
	public.sskr_dwh_dim_accounts( 
		account_num
		, valid_to
        , client
		, create_dt
		, update_dt 
	)
SELECT 
	stg.account_num
	, stg.valid_to
    , stg.client
	, stg.create_dt	
	, NULL 
FROM public.sskr_stg_accounts stg
LEFT JOIN 
	public.sskr_dwh_dim_accounts tgt
ON 
	stg.account_num = tgt.account_num
WHERE
	1 = 1
	AND tgt.account_num IS NULL
;

-- 4. Update values

UPDATE 
	public.sskr_dwh_dim_accounts
SET 
	valid_to = tmp.valid_to
    , client = tmp.client
	, update_dt = tmp.update_dt
FROM (
	SELECT 
		stg.account_num
		, stg.valid_to 
        , stg.client
		, stg.update_dt	
	FROM 
		public.sskr_stg_accounts stg
	INNER JOIN 
		public.sskr_dwh_dim_accounts tgt
	ON 
		stg.account_num = tgt.account_num
	WHERE 
		1 = 0
		OR stg.valid_to <> tgt.valid_to
        OR stg.client <> tgt.client
) tmp
WHERE
	1 = 1
	AND public.sskr_dwh_dim_accounts.account_num = tmp.account_num
;

-- 5. Delete rows that no longer exist in source

DELETE FROM 
	public.sskr_dwh_dim_accounts
WHERE
	1 = 1
	AND account_num IN (
		SELECT
			tgt.account_num
		FROM
			public.sskr_dwh_dim_accounts tgt
		LEFT JOIN
			public.sskr_stg_del_accounts stg
		ON
			stg.account_num = tgt.account_num
		WHERE
			1 = 1
			AND stg.account_num IS NULL
	)
;

-- 6. Metadata update

UPDATE 
	public.sskr_meta_info
SET
	max_update_dt = COALESCE( 
		(
			SELECT MAX( update_dt ) 
			FROM public.sskr_stg_accounts 
		)
		, (
			SELECT max_update_dt 
			FROM public.sskr_meta_info 
			WHERE 
				1 = 1
				AND schema_name = 'info' 
				AND table_name = 'accounts_scd1'
		)
	)
WHERE 
	1 = 1
	AND schema_name = 'info' 
	AND table_name = 'accounts_sd1'
;

COMMIT;
