-------------------- SCD1 Increment Load: Clients -------------------- 

-- 1. For further deletion from target table

INSERT INTO 
	public.sskr_stg_del_clients( client_id )
SELECT 
	client_id
FROM 
	public.sskr_stg_clients
;

-- 2. Remove unnecessary data

DELETE FROM 
	public.sskr_stg_clients
WHERE 
	update_dt < ( 
		SELECT 
			max_update_dt 
		FROM 
			public.sskr_meta_info 
		WHERE 
			1 = 1
			AND schema_name='info'
			AND table_name='clients' 
	)
	OR update_dt IS NOT NULL
;

-- 3. Insert new data to target

INSERT INTO 
	public.sskr_dwh_dim_clients( 
		client_id
		, last_name 
		, first_name 
		, patronymic
		, date_of_birth 
		, passport_num
		, passport_valid_to 
		, phone
		, create_dt
		, update_dt 
	)
SELECT 
	stg.client_id
	, stg.last_name 
	, stg.first_name 
	, stg.patronymic
	, stg.date_of_birth 
	, stg.passport_num
	, stg.passport_valid_to 
	, stg.phone
	, stg.create_dt	
	, NULL 
FROM public.sskr_stg_clients stg
LEFT JOIN
	public.sskr_dwh_dim_clients tgt
ON 
	stg.client_id = tgt.client_id
WHERE
	tgt.client_id IS NULL
;

-- 4. Update values

UPDATE
	public.sskr_dwh_dim_clients
SET 
	last_name = tmp.last_name
	, first_name = tmp.first_name
	, patronymic = tmp.patronymic
	, date_of_birth = tmp.date_of_birth
	, passport_num = tmp.passport_num
	, passport_valid_to = tmp.passport_valid_to
	, phone = tmp.phone
	, update_dt = tmp.update_dt
FROM (
	SELECT 
		stg.client_id
		, stg.last_name 
		, stg.first_name 
		, stg.patronymic
		, stg.date_of_birth 
		, stg.passport_num
		, stg.passport_valid_to 
		, stg.phone
		, stg.update_dt	
	FROM 
		public.sskr_stg_clients stg
	INNER JOIN 
		public.sskr_dwh_dim_clients tgt
	ON 
		stg.client_id = tgt.client_id
	WHERE 
		stg.last_name <> tgt.last_name
		OR stg.first_name <> tgt.first_name
		OR stg.patronymic <> tgt.patronymic
		OR stg.date_of_birth <> tgt.date_of_birth
		OR stg.passport_num <> tgt.passport_num
		OR stg.phone <> tgt.phone
		OR ( stg.passport_valid_to is null and tgt.passport_valid_to is not null ) 
		OR ( stg.passport_valid_to is not null and tgt.passport_valid_to is null )
) tmp
WHERE
	public.sskr_dwh_dim_clients.client_id = tmp.client_id
; 

-- 5. Delete rows that no longer exist in source

DELETE FROM 
	public.sskr_dwh_dim_clients
WHERE
	client_id IN (
		SELECT 
			tgt.client_id
		FROM 
			public.sskr_dwh_dim_clients tgt
		LEFT JOIN
			public.sskr_stg_del_clients stg
		ON 
			stg.client_id = tgt.client_id
		WHERE 
			stg.client_id IS NULL
	)
;

-- 6. Metadata update

UPDATE 
	public.sskr_meta_info
SET 
	max_update_dt = COALESCE( 
		(
			SELECT MAX( update_dt ) 
			from public.sskr_stg_clients 
		)
		, (
			SELECT max_update_dt 
			FROM public.sskr_meta_info 
			where 
				1 = 1
				AND schema_name = 'info' 
				AND table_name = 'clients' 
		)
	)
where 
	1 = 1
	AND schema_name = 'info' 
	AND table_name = 'clients'
;

COMMIT;
