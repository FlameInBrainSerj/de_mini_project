-------------------- SCD1 Increment Load: Terminals -------------------- 

-- 1. Insert new data to target

INSERT INTO 
	public.sskr_dwh_dim_terminals( 
		terminal_id
		, terminal_type 
		, terminal_city 
		, terminal_address
		, create_dt
		, update_dt 
	)
SELECT 
	stg.terminal_id
	, stg.terminal_type 
	, stg.terminal_city 
	, stg.terminal_address
	, stg.file_date	
	, NULL 
FROM public.sskr_stg_terminals stg
LEFT JOIN
	public.sskr_dwh_dim_terminals tgt
ON 
	stg.terminal_id = tgt.terminal_id
WHERE
	tgt.terminal_id IS NULL
;

-- 2. Update values

UPDATE
	public.sskr_dwh_dim_terminals
SET
	terminal_type = tmp.terminal_type
	, terminal_city = tmp.terminal_city
	, terminal_address = tmp.terminal_address
	, update_dt = tmp.file_date
FROM (
	SELECT 
		stg.terminal_id
        , stg.terminal_type 
        , stg.terminal_city 
        , stg.terminal_address
        , stg.file_date	
	FROM 
		public.sskr_stg_terminals stg
	INNER JOIN 
		public.sskr_dwh_dim_terminals tgt
	ON 
		stg.terminal_id = tgt.terminal_id
	WHERE 
		stg.terminal_type <> tgt.terminal_type
		OR stg.terminal_city <> tgt.terminal_city
		OR stg.terminal_address <> tgt.terminal_address
) tmp
WHERE
	public.sskr_dwh_dim_terminals.terminal_id = tmp.terminal_id
;

-- 3. Delete rows that no longer exist in source

DELETE FROM 
	public.sskr_dwh_dim_terminals
WHERE
	terminal_id IN (
		SELECT 
			tgt.terminal_id
		FROM 
			public.sskr_dwh_dim_terminals tgt
		LEFT JOIN
			public.sskr_stg_terminals stg
		ON 
			stg.terminal_id = tgt.terminal_id
		WHERE 
			stg.terminal_id IS NULL
	)
;

COMMIT;
