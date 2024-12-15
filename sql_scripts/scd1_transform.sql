----------  staging ---------- 



-- добавление
insert into dwh.XXXX_source ( id, val, update_dt ) values ( 1, 'A', now() );
insert into dwh.XXXX_source ( id, val, update_dt ) values ( 2, 'B', now() );
insert into dwh.XXXX_source ( id, val, update_dt ) values ( 3, 'C', now() );
insert into dwh.XXXX_source ( id, val, update_dt ) values ( 4, 'D', now() );

-- обновление
update dwh.XXXX_source set val = 'X', update_dt = now() where id = 3;

-- удаление
delete from dwh.XXXX_source where id = 3;

-- выборки
select * from dwh.XXXX_source;
select * from dwh.XXXX_stg;
select * from dwh.XXXX_target;

-- Подготовка таблиц

create table dwh.XXXX_source( 
	id integer,
	val varchar(50),
	update_dt timestamp(0)
);


create table dwh.XXXX_stg( 
	id integer,
	val varchar(50),
	update_dt timestamp(0)
);

create table dwh.XXXX_target (
	id integer,
	val varchar(50),
	create_dt timestamp(0),
	update_dt timestamp(0)
);

create table dwh.XXXX_meta (
    schema_name varchar(30),
    table_name varchar(30),
    max_update_dt timestamp(0)
);

insert into dwh.XXXX_meta( schema_name, table_name, max_update_dt )
values( 'dwh','XXXX_SOURCE', to_timestamp('1900-01-01','YYYY-MM-DD') );

create table dwh.XXXX_stg_del( 
	id integer
);


----------------------------------------------------------------------------
-- Инкрементальная загрузка SCD1

-- 1. Очистка стейджинговых таблиц

delete from dwh.XXXX_stg;
delete from dwh.XXXX_stg_del;

-- 2. Захват данных из источника (измененных с момента последней загрузки) в стейджинг

insert into dwh.XXXX_stg( id, val, update_dt )
select id, val, update_dt from dwh.XXXX_source
where update_dt > ( select max_update_dt from dwh.XXXX_meta where schema_name='dwh' and table_name='XXXX_SOURCE' );

-- 3. Захват в стейджинг ключей из источника полным срезом для вычисления удалений.

insert into dwh.XXXX_stg_del( id )
select id from dwh.XXXX_source;

-- 4. Загрузка в приемник "вставок" на источнике (формат SCD1).

insert into dwh.XXXX_target( id, val, create_dt, update_dt )
select 
	stg.id, 
	stg.val, 
	stg.update_dt, 
	null 
from dwh.XXXX_stg stg
left join dwh.XXXX_target tgt
on stg.id = tgt.id
where tgt.id is null;

-- 5. Обновление в приемнике "обновлений" на источнике (формат SCD1).

update dwh.XXXX_target
set 
	val = tmp.val,
	update_dt = tmp.update_dt
from (
	select 
		stg.id, 
		stg.val, 
		stg.update_dt, 
		null 
	from dwh.XXXX_stg stg
	inner join dwh.XXXX_target tgt
	on stg.id = tgt.id
	where stg.val <> tgt.val or ( stg.val is null and tgt.val is not null ) or ( stg.val is not null and tgt.val is null )
) tmp
where XXXX_target.id = tmp.id; 

-- 6. Удаление в приемнике удаленных в источнике записей (формат SCD1).

delete from dwh.XXXX_target
where id in (
	select tgt.id
	from dwh.XXXX_target tgt
	left join dwh.XXXX_stg_del stg
	on stg.id = tgt.id
	where stg.id is null
);

-- 7. Обновление метаданных.

update dwh.XXXX_meta
set max_update_dt = coalesce( (select max( update_dt ) from dwh.XXXX_stg ), ( select max_update_dt from dwh.XXXX_meta where schema_name='dwh' and table_name='XXXX_SOURCE' ) )
where schema_name='dwh' and table_name = 'XXXX_SOURCE';

-- 8. Фиксация транзакции.

commit;