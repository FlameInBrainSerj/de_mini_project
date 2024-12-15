-------------------- Staging -------------------- 
DROP TABLE IF EXISTS public.sskr_meta_info CASCADE;
-------------------- Staging -------------------- 
DROP TABLE IF EXISTS public.sskr_stg_terminals CASCADE;
DROP TABLE IF EXISTS public.sskr_stg_clients CASCADE;
DROP TABLE IF EXISTS public.sskr_stg_accounts CASCADE;
DROP TABLE IF EXISTS public.sskr_stg_cards CASCADE;
DROP TABLE IF EXISTS public.sskr_stg_transactions CASCADE;
DROP TABLE IF EXISTS public.sskr_stg_blacklist CASCADE;
-------------------- FACT & DIM -------------------- 
DROP TABLE IF EXISTS public.sskr_dwh_dim_terminals CASCADE;
DROP TABLE IF EXISTS public.sskr_dwh_dim_clients CASCADE;
DROP TABLE IF EXISTS public.sskr_dwh_dim_accounts CASCADE;
DROP TABLE IF EXISTS public.sskr_dwh_dim_cards CASCADE;
DROP TABLE IF EXISTS public.sskr_dwh_fact_transactions CASCADE;
DROP TABLE IF EXISTS public.sskr_dwh_fact_passport_blacklist CASCADE;
-------------------- Report -------------------- 
DROP TABLE IF EXISTS public.sskr_rep_fraud CASCADE;

COMMIT;
