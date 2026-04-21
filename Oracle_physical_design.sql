select index_name,
       table_name,
       uniqueness,
       status
from user_indexes
where table_name in ('CUSTOMER', 'STAFF', 'ORDERITEM', 'Order')
order by table_name, index_name;

BEGIN
  EXECUTE IMMEDIATE 'DROP INDEX IX_ORDER_ORDER_DATETIME';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

BEGIN
  EXECUTE IMMEDIATE 'DROP INDEX IX_STAFF_RESIDENCE_PRAHA';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

BEGIN
  EXECUTE IMMEDIATE 'DROP INDEX IX_ORDER_COMPOSITE';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

BEGIN
  EXECUTE IMMEDIATE 'DROP INDEX IX_ORDERITEM_IDO_QTY';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/



create or replace view vsql_user AS
  select 
    sql_id,
    plan_hash_value,
    sum(executions) as executions,
    round(sum(buffer_gets)/sum(executions),0) as buffer_gets_per_exec,
    round(sum(cpu_time)/sum(executions),0) as cpu_time_per_exec,
    round(sum(elapsed_time)/sum(executions),0) as elapsed_time_per_exec,
    round(sum(rows_processed)/sum(executions),0) as rows_processed_per_exec,
    round(sum(elapsed_time)/1000,0) as total_elapsed_time_ms,
    substr(max(sql_text),1,1000) sql_text
  from v$sql
  where parsing_schema_name = sys_context('USERENV','SESSION_USER')
  group by sql_id, plan_hash_value
  having sum(executions) <> 0;

grant select on vsql_user to public;

create public synonym vsql_user for SYS.vsql_user;

create or replace procedure PrintQueryStat(p_sql_id varchar2, p_plan_hash_value int)
as
begin
  for rec in (
    select *
    from vsql_user
    where sql_id = p_sql_id
      and plan_hash_value = p_plan_hash_value
  )
  loop
    dbms_output.put_line('---- Query Processing Statistics ----');
    dbms_output.put_line('executions:               ' || rec.executions);
    dbms_output.put_line('rows_processed_per_exec:  ' || rec.rows_processed_per_exec);
    dbms_output.put_line('buffer_gets_per_exec:     ' || rec.buffer_gets_per_exec);
    dbms_output.put_line('cpu_time_per_exec:        ' || rec.cpu_time_per_exec);
    dbms_output.put_line('cpu_time_per_exec_ms:     ' || round(rec.cpu_time_per_exec/1000, 0));
    dbms_output.put_line('elapsed_time_per_exec:    ' || rec.elapsed_time_per_exec);
    dbms_output.put_line('elapsed_time_per_exec_ms: ' || round(rec.elapsed_time_per_exec/1000, 0));
    dbms_output.put_line('total_elapsed_time_ms:    ' || rec.total_elapsed_time_ms);
    dbms_output.put_line('sql_text: ' || rec.sql_text);
  end loop;
end;
/

set serveroutput on;


-- Iteration 0: baseline without additional indexes
-- Parallelism is disabled using the NO_PARALLEL hint.


select /*+ NO_PARALLEL */ count(*)as cnt, sum(oi.quantity)as total
from "Order" o 
join Staff s on s.idsa = o.idsa
join orderItem oi on oi.ido = o.ido
join customer c on c.idc = o.idc
where o.order_datetime between 
      to_date('2025-01-01','YYYY-MM-DD') 
  and to_date('2025-01-31','YYYY-MM-DD')
and s.residence = 'Praha' and c.residence = 'Praha';

-- Expected Oracle result: cnt = 19, total = 103

explain plan for

select /*+ NO_PARALLEL */ count(*)as cnt, sum(oi.quantity)as total
from "Order" o 
join Staff s on s.idsa = o.idsa
join orderItem oi on oi.ido = o.ido
join customer c on c.idc = o.idc
where o.order_datetime between 
      to_date('2025-01-01','YYYY-MM-DD') 
  and to_date('2025-01-31','YYYY-MM-DD')
and s.residence = 'Praha' and c.residence = 'Praha';


select * from table(dbms_xplan.display);

---Plan hash value: 1389829209


set feedback on SQL_ID;
select /*+ NO_PARALLEL */ count(*)as cnt, sum(oi.quantity)as total
from "Order" o 
join Staff s on s.idsa = o.idsa
join orderItem oi on oi.ido = o.ido
join customer c on c.idc = o.idc
where o.order_datetime between 
      to_date('2025-01-01','YYYY-MM-DD') 
  and to_date('2025-01-31','YYYY-MM-DD')
and s.residence = 'Praha' and c.residence = 'Praha';
set feedback off SQL_ID;

exec PrintQueryStat('56xntf3s3yzbq', 1389829209);

-- Iteration 0: baseline without additional indexes.
-- The query performs selection on Order.order_datetime and on the residence
-- attributes of Staff and Customer, then joins Order with Staff, Customer,
-- and OrderItem, and finally computes COUNT(*) and SUM(quantity).
--
-- Because no suitable indexes support these operations, Oracle performs
-- full table scans on STAFF, "Order", and CUSTOMER.
--
-- CUSTOMER is also scanned completely because there is no access path that
-- directly supports the predicate residence = 'Praha' together with the join by idc.
--
-- ORDERITEM already uses the primary-key index for the join by ido, but Oracle
-- still has to access complete ORDERITEM rows by ROWID in order to read quantity.
--
-- Therefore, the baseline plan is scan-based and uses hash joins on large inputs.
-- This means that filtering and joining are not sufficiently supported by indexes,
-- and complete table rows still have to be accessed.

-- Iteration 1: index on Order(order_datetime)

create index IX_ORDER_ORDER_DATETIME
on "Order"(order_datetime);


explain plan for

select /*+ NO_PARALLEL */ count(*)as cnt, sum(oi.quantity)as total
from "Order" o 
join Staff s on s.idsa = o.idsa
join orderItem oi on oi.ido = o.ido
join customer c on c.idc = o.idc
where o.order_datetime between 
      to_date('2025-01-01','YYYY-MM-DD') 
  and to_date('2025-01-31','YYYY-MM-DD')
and s.residence = 'Praha' and c.residence = 'Praha';


select * from table(dbms_xplan.display);

-- Iteration 1: index on "Order"(order_datetime).
-- This index is created to support the selection on Order.order_datetime,
-- because the query restricts orders to January 2025.
-- However, the query also needs idsa, idc, and ido for the subsequent joins.
-- Therefore, a single-column index on order_datetime does not support enough
-- operations to avoid access to complete Order rows.
--
-- The QEP remains unchanged compared to the baseline.
-- Oracle still performs full table scans on STAFF, "Order", and CUSTOMER,
-- and still uses hash joins on the large scanned inputs.
-- ORDERITEM continues to use the primary-key index for the join by ido,
-- but complete ORDERITEM rows still have to be accessed by ROWID.
--
-- Therefore, the single-column index on Order.order_datetime is not sufficient,
-- and a composite index is required later to support both the selection
-- and the joins on Order.

---Iteration - 2 -function based index on Customer

create index IX_STAFF_RESIDENCE_PRAHA
on Staff (
  case when residence = 'Praha' then idsa end
);

explain plan for

select /*+ NO_PARALLEL */ count(*)as cnt, sum(oi.quantity)as total
from "Order" o 
join Staff s on s.idsa = o.idsa
join orderItem oi on oi.ido = o.ido
join customer c on c.idc = o.idc
where o.order_datetime between 
      to_date('2025-01-01','YYYY-MM-DD') 
  and to_date('2025-01-31','YYYY-MM-DD')
and s.residence = 'Praha' and c.residence = 'Praha';


select * from table(dbms_xplan.display);

-- Iteration 2: function-based index on Staff.
-- Oracle does not support filtered indexes directly, so a function-based index
-- is created to represent only rows where residence = 'Praha'.
-- The purpose of this index is to support the selection on Staff.residence
-- and the join from Order to Staff by idsa.
--
-- However, the QEP is unchanged compared to Iteration 1.
-- Oracle still performs full table scans on STAFF, "Order", and CUSTOMER,
-- and still uses hash joins on the large scanned inputs.
-- ORDERITEM still uses the primary-key index for the join by ido, but complete
-- ORDERITEM rows still have to be accessed by ROWID.
--
-- Therefore, the function-based index on Staff is not matched by Oracle to
-- the current query predicate, so it does not improve the plan.

-- Iteration 3: composite index on "Order"(order_datetime, idsa, idc, ido)

select * from "Order";


create index IX_ORDER_COMPOSITE
on "Order"(order_datetime, idsa, idc, ido);

explain plan for

select /*+ NO_PARALLEL */ count(*)as cnt, sum(oi.quantity)as total
from "Order" o 
join Staff s on s.idsa = o.idsa
join orderItem oi on oi.ido = o.ido
join customer c on c.idc = o.idc
where o.order_datetime between 
      to_date('2025-01-01','YYYY-MM-DD') 
  and to_date('2025-01-31','YYYY-MM-DD')
and s.residence = 'Praha' and c.residence = 'Praha';


select * from table(dbms_xplan.display);

-- Iteration 3: composite index on "Order"(order_datetime, idsa, idc, ido).
-- order_datetime is the leading key because the query first applies the
-- date-range selection for January 2025.
-- idsa is included to support the join from Order to Staff.
-- idc is included to support the join from Order to Customer.
-- ido is included to support the join from Order to OrderItem.
-- Therefore, this index supports both the selection and all subsequent joins
-- on Order in a single access path, without requiring a full scan of Order.
--
-- In this iteration, Oracle replaces TABLE ACCESS FULL on "Order" with
-- INDEX RANGE SCAN on IX_ORDER_COMPOSITE.
-- This significantly improves the plan and reduces the estimated cost
-- compared to the previous iterations.
--
-- However, STAFF and CUSTOMER are still accessed by full table scans,
-- and ORDERITEM still requires TABLE ACCESS BY INDEX ROWID after the
-- index range scan on PK_ORDERITEM.
-- Therefore, the plan is improved, but it is not yet fully index-driven.

-- Iteration 4: composite index on OrderItem(ido, quantity)

create index IX_ORDERITEM_IDO_QTY
on OrderItem(ido, quantity);

-- Iteration 4: composite index on OrderItem(ido, quantity).
-- ido is the join attribute from Order to OrderItem.
-- quantity is included because it is required for SUM(oi.quantity).
-- Therefore, this index supports both the join and the aggregation without
-- accessing complete OrderItem rows.
--
-- In this iteration, Oracle uses INDEX RANGE SCAN on IX_ORDERITEM_IDO_QTY,
-- and the previous TABLE ACCESS BY INDEX ROWID on ORDERITEM is eliminated.
-- This reduces the estimated cost further and improves the access path for OrderItem.
--
-- However, STAFF and CUSTOMER are still accessed by full table scans,
-- and the joins involving these tables remain hash joins.
-- Therefore, the plan is improved further, but it is still not fully index-driven.

CREATE INDEX IX_CUSTOMER_RESIDENCE_IDC
ON Customer(residence, idc);

CREATE INDEX IX_STAFF_RESIDENCE_IDSA
ON Staff(residence, idsa);


-- Additional Oracle iteration: composite index on Staff(residence, idsa)
-- residence is the leading key because the query first applies the predicate
-- Staff.residence = 'Praha'.
-- idsa is included to support the join from Order to Staff.
-- Therefore, this index supports both the selection and the join on Staff
-- in a single access path.

-- Additional Oracle iteration: composite index on Customer(residence, idc)
-- residence is the leading key because the query first applies the predicate
-- Customer.residence = 'Praha'.
-- idc is included to support the join from Order to Customer.
-- Therefore, this index supports both the selection and the join on Customer
-- in a single access path.


DROP INDEX IX_ORDER_ORDER_DATETIME;
DROP INDEX IX_STAFF_RESIDENCE_PRAHA;
DROP INDEX IX_ORDER_COMPOSITE;
DROP INDEX IX_ORDERITEM_IDO_QTY;



