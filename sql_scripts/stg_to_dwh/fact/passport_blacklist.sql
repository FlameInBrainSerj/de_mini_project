-------------------- Fact: Passport Blacklist -------------------- 

-- 1. Insert new data to target

INSERT INTO 
	public.sskr_dwh_fact_passport_blacklist(
        passport_num
        , entry_dt
    )
SELECT 
	passport_num
    , entry_dt
FROM
	public.sskr_stg_blacklist
WHERE
	entry_dt > ( 
		SELECT 
			max_update_dt 
		FROM 
			public.sskr_meta_info 
		WHERE
			1 = 1
			AND schema_name = 'local'
			AND table_name = 'blacklist' 
	)
;

-- 2. Metadata update

UPDATE 
	public.sskr_meta_info
SET
	max_update_dt = COALESCE( 
		(
			SELECT MAX( entry_dt ) 
			FROM public.sskr_stg_blacklist 
		)
		, (
			SELECT max_update_dt 
			FROM public.sskr_meta_info 
			WHERE 
				1 = 1
				AND schema_name = 'local' 
				AND table_name = 'blacklist' 
		)
	)
WHERE 
	1 = 1
	AND schema_name = 'local' 
	AND table_name = 'blacklist'
;

COMMIT;
