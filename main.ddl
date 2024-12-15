-------------------- Meta -------------------- 

CREATE TABLE IF NOT EXISTS public.sskr_meta_info (
    schema_name         VARCHAR(30)
    , table_name        VARCHAR(30)
    , max_update_dt     TIMESTAMP(0)
);

-------------------- Staging -------------------- 

CREATE TABLE IF NOT EXISTS public.sskr_stg_terminals (
    terminal_id         VARCHAR(6)                      PRIMARY KEY
    , terminal_type     VARCHAR(3)                      NOT NULL
    , terminal_city     VARCHAR(25)                     NOT NULL
    , terminal_address  VARCHAR(60)                     NOT NULL
)
;

CREATE TABLE IF NOT EXISTS public.sskr_stg_clients (
    client_id           VARCHAR(10)                     PRIMARY KEY
    , last_name         VARCHAR(20)                     NOT NULL
    , first_name        VARCHAR(20)                     NOT NULL
    , patronymic        VARCHAR(20)                     NOT NULL
    , date_of_birth     DATE                            NOT NULL
    , passport_num      VARCHAR(15)                     NOT NULL
    , passport_valid_to DATE
    , phone             VARCHAR(16)                     NOT NULL
    , create_dt         TIMESTAMP WITHOUT TIME ZONE     NOT NULL
    , update_dt         TIMESTAMP WITHOUT TIME ZONE
)
;

CREATE TABLE IF NOT EXISTS public.sskr_stg_accounts (
    account_num         VARCHAR(20)                     PRIMARY KEY
    , valid_to          DATE                            NOT NULL
    , client            VARCHAR(10)                     NOT NULL
    , create_dt         TIMESTAMP WITHOUT TIME ZONE     NOT NULL
    , update_dt         TIMESTAMP WITHOUT TIME ZONE
)
;

CREATE TABLE IF NOT EXISTS public.sskr_stg_cards (
    card_num            VARCHAR(20)                     PRIMARY KEY
    , account_num       VARCHAR(20)                     NOT NULL
    , create_dt         TIMESTAMP WITHOUT TIME ZONE     NOT NULL
    , update_dt         TIMESTAMP WITHOUT TIME ZONE
)
;

CREATE TABLE IF NOT EXISTS public.sskr_stg_transactions (
    trans_id            VARCHAR(11)                     PRIMARY KEY
    , trans_date        DATE                            NOT NULL
    , card_num          VARCHAR(20)                     NOT NULL
    , oper_type         VARCHAR(10)                     NOT NULL
    , amt               DECIMAL                         NOT NULL
    , oper_result       VARCHAR(10)                     NOT NULL
    , terminal          VARCHAR(6)                      NOT NULL
)
;

CREATE TABLE IF NOT EXISTS public.sskr_stg_blacklist (
    passport_num        VARCHAR(15)                     PRIMARY KEY
    , entry_dt          DATE                            NOT NULL
)
;

-------------------- FACT & DIM -------------------- 

CREATE TABLE IF NOT EXISTS public.sskr_dwh_dim_terminals (
    terminal_id         VARCHAR(6)                      PRIMARY KEY
    , terminal_type     VARCHAR(3)                      NOT NULL
    , terminal_city     VARCHAR(25)                     NOT NULL
    , terminal_address  VARCHAR(60)                     NOT NULL
    , create_dt         TIMESTAMP WITHOUT TIME ZONE     NOT NULL
    , update_dt         TIMESTAMP WITHOUT TIME ZONE     NOT NULL
)
;

CREATE TABLE IF NOT EXISTS public.sskr_dwh_dim_clients (
    client_id           VARCHAR(10)                     PRIMARY KEY
    , last_name         VARCHAR(20)                     NOT NULL
    , first_name        VARCHAR(20)                     NOT NULL
    , patronymic        VARCHAR(20)                     NOT NULL
    , date_of_birth     DATE                            NOT NULL
    , passport_num      VARCHAR(15)                     NOT NULL
    , passport_valid_to DATE
    , phone             VARCHAR(16)                     NOT NULL
    , create_dt         TIMESTAMP WITHOUT TIME ZONE     NOT NULL
    , update_dt         TIMESTAMP WITHOUT TIME ZONE     NOT NULL
)
;

CREATE TABLE IF NOT EXISTS public.sskr_dwh_dim_accounts (
    account_num         VARCHAR(20)                     PRIMARY KEY
    , valid_to          DATE                            NOT NULL
    , client            VARCHAR(10)                     NOT NULL
    , create_dt         TIMESTAMP WITHOUT TIME ZONE     NOT NULL
    , update_dt         TIMESTAMP WITHOUT TIME ZONE     NOT NULL

    , FOREIGN KEY (client) REFERENCES public.sskr_dwh_dim_clients (client_id)
)
;

CREATE TABLE IF NOT EXISTS public.sskr_dwh_dim_cards (
    card_num            VARCHAR(20)                     PRIMARY KEY
    , account_num       VARCHAR(20)                     NOT NULL
    , create_dt         TIMESTAMP WITHOUT TIME ZONE     NOT NULL
    , update_dt         TIMESTAMP WITHOUT TIME ZONE     NOT NULL

    , FOREIGN KEY (account_num) REFERENCES public.sskr_dwh_dim_accounts (account_num)
)
;

CREATE TABLE IF NOT EXISTS public.sskr_dwh_fact_transactions (
    trans_id            VARCHAR(11)                     PRIMARY KEY
    , trans_date        DATE                            NOT NULL
    , card_num          VARCHAR(20)                     NOT NULL
    , oper_type         VARCHAR(10)                     NOT NULL
    , amt               DECIMAL                         NOT NULL
    , oper_result       VARCHAR(10)                     NOT NULL
    , terminal          VARCHAR(6)                      NOT NULL

    , FOREIGN KEY (card_num) REFERENCES public.sskr_dwh_dim_cards (card_num)
    , FOREIGN KEY (terminal) REFERENCES public.sskr_dwh_dim_terminals (terminal_id)
)
;

CREATE TABLE IF NOT EXISTS public.sskr_dwh_fact_passport_blacklist (
    passport_num        VARCHAR(15)                     PRIMARY KEY
    , entry_dt          DATE                            NOT NULL
)
;

-------------------- Report -------------------- 

CREATE TABLE IF NOT EXISTS public.sskr_rep_fraud (
    event_dt            TIMESTAMP WITHOUT TIME ZONE     NOT NULL
    , passport          VARCHAR(15)                     NOT NULL
    , fio               VARCHAR(70)                     NOT NULL
    , phone             VARCHAR(16)                     NOT NULL
    , event_type        VARCHAR(100)                    NOT NULL
    , report_dt         DATE                            NOT NULL
)
;

COMMIT;

