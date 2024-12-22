-------------------- Meta -------------------- 
DROP TABLE IF EXISTS public.sskr_meta_info CASCADE;
-------------------- Staging -------------------- 
DROP TABLE IF EXISTS public.sskr_stg_terminals CASCADE;
DROP TABLE IF EXISTS public.sskr_stg_clients CASCADE;
DROP TABLE IF EXISTS public.sskr_stg_accounts CASCADE;
DROP TABLE IF EXISTS public.sskr_stg_cards CASCADE;
DROP TABLE IF EXISTS public.sskr_stg_transactions CASCADE;
DROP TABLE IF EXISTS public.sskr_stg_blacklist CASCADE;
-------------------- Staging - del -------------------- 
DROP TABLE IF EXISTS public.sskr_stg_del_clients CASCADE;
DROP TABLE IF EXISTS public.sskr_stg_del_cards CASCADE;
DROP TABLE IF EXISTS public.sskr_stg_del_accounts CASCADE;
-------------------- DWH: FACT -------------------- 
DROP TABLE IF EXISTS public.sskr_dwh_fact_transactions CASCADE;
DROP TABLE IF EXISTS public.sskr_dwh_fact_passport_blacklist CASCADE;
-------------------- DWH: DIM - SCD1 --------------------
DROP TABLE IF EXISTS public.sskr_dwh_dim_terminals CASCADE;
DROP TABLE IF EXISTS public.sskr_dwh_dim_clients CASCADE;
DROP TABLE IF EXISTS public.sskr_dwh_dim_accounts CASCADE;
DROP TABLE IF EXISTS public.sskr_dwh_dim_cards CASCADE;
-------------------- Report -------------------- 
DROP TABLE IF EXISTS public.sskr_rep_fraud CASCADE;

COMMIT;
