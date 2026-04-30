col sql_text format a30;

-- Task for a sys user
create or replace view vsql_user AS
  select 
    sql_id, plan_hash_value, 
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

-----------------------

select * from vsql_user;

-----------------------

create or replace procedure PrintQueryStat(p_sql_id varchar2, p_plan_hash_value int)
as
begin
  -- report the statistics of the query processing
  for rec in (
    select * from vsql_user  
    where sql_id=p_sql_id and plan_hash_value=p_plan_hash_value
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

-=========================================================================
-- Selectivity Analysis
--=========================================================================

select count(*) from Customer;                -- 300000
select count(*) from Customer
where residence = 'Berlin';                   -- 15731
-- Selectivity: 15731/300000 = 5.2%          → THIRD priority

select count(*) from "Order";                 -- 499894
select count(*) from "Order"
where order_datetime = DATE '2025-05-01';     -- 88
-- Selectivity: 88/499894 = 0.00017%         → FIRST priority (most selective)

select count(*) from OrderItem;               -- 5000005
select count(*) from OrderItem
where unit_price >= 100000
and unit_price <= 200000;                     -- 230802
-- Selectivity: 230802/5000005 = 4.6%        → SECOND priority


--=========================================================================
-- Baseline
--=========================================================================

-- Problems identified from QEP:
-- 1. TABLE ACCESS FULL on Order
--    → no index on order_datetime
--    → reads all 499894 rows to find only 88
--    → cost = 658 out of 748 total → most expensive ❌
-- 2. TABLE ACCESS BY INDEX ROWID on Customer
--    → PK finds idc for join ✅
--    → residence not in PK → must fetch full row ❌
-- 3. TABLE ACCESS BY INDEX ROWID on OrderItem
--    → PK finds ido for join ✅
--    → unit_price not in PK → must fetch full row ❌

explain plan for
--query
SELECT COUNT(*) AS record_count
FROM "Order" o
JOIN Customer c ON o.idc = c.idc
JOIN OrderItem oi ON o.ido = oi.ido
WHERE c.residence = 'Berlin'
  AND o.order_datetime = DATE '2025-05-01'
  AND oi.unit_price BETWEEN 100000 AND 200000;
  
select * from table(dbms_xplan.display);

--Plan hash value: 147360924
--SQL_ID: 4ajg1qjhj5zcr


set feedback on SQL_ID;
SELECT COUNT(*) AS record_count
FROM "Order" o
JOIN Customer c ON o.idc = c.idc
JOIN OrderItem oi ON o.ido = oi.ido
WHERE c.residence = 'Berlin'
  AND o.order_datetime = DATE '2025-05-01'
  AND oi.unit_price BETWEEN 100000 AND 200000;
set feedback off SQL_ID;


exec PrintQueryStat('4ajg1qjhj5zcr',147360924);

select * from vsql_user WHERE sql_id='4ajg1qjhj5zcr';

-- buffer gets : 2671
-- cpu time    : 30443 µs

------------------------------------------------------------------------------------------------
| Id  | Operation                       | Name         | Rows  | Bytes | Cost (%CPU)| Time     |
------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT                |              |     1 |    42 |   748   (2)| 00:00:01 |
|   1 |  SORT AGGREGATE                 |              |     1 |    42 |            |          |
|   2 |   NESTED LOOPS                  |              |    22 |   924 |   748   (2)| 00:00:01 |
|   3 |    NESTED LOOPS                 |              |   180 |   924 |   748   (2)| 00:00:01 |
|   4 |     NESTED LOOPS                |              |    18 |   558 |   694   (2)| 00:00:01 |
|*  5 |      TABLE ACCESS FULL          | Order        |    18 |   324 |   658   (2)| 00:00:01 |
|*  6 |      TABLE ACCESS BY INDEX ROWID| CUSTOMER     |     1 |    13 |     2   (0)| 00:00:01 |
|*  7 |       INDEX UNIQUE SCAN         | SYS_C0085575 |     1 |       |     1   (0)| 00:00:01 |
|*  8 |     INDEX RANGE SCAN            | PK_ORDERITEM |    10 |       |     2   (0)| 00:00:01 |
|*  9 |    TABLE ACCESS BY INDEX ROWID  | ORDERITEM    |     1 |    11 |     3   (0)| 00:00:01 |
------------------------------------------------------------------------------------------------

-- buffer gets : 283
-- cpu time    : 16563 µs



--=========================================================================
-- Iteration 1: Composite Index on Order
--=========================================================================

-- Goal: Eliminate TABLE ACCESS FULL on Order
--
-- Columns identified from QEP:
--   TABLE ACCESS FULL WHERE order_datetime → FILTER → FIRST
--   JOIN o.idc = c.idc                    → JOIN Customer → SECOND
--   JOIN o.ido = oi.ido                   → JOIN OrderItem → THIRD
--
-- order_datetime → FIRST  : most selective (0.00017%), WHERE filter
-- idc            → SECOND : JOIN column to Customer (point query =)
-- ido            → THIRD  : JOIN column to OrderItem (point query =)


create index idx_order_date_idc_ido  on "Order"(order_datetime,idc,ido);

exec PrintQueryStat('4ajg1qjhj5zcr',1373759162);
select * from vsql_user WHERE sql_id='4ajg1qjhj5zcr';
--Plan hash value:1373759162
--SQL_ID:4ajg1qjhj5zcr

--buffer gets: 283
--cpu time: 16563
--plan:
----------------------------------------------------------------------------------------------------------
| Id  | Operation                       | Name                   | Rows  | Bytes | Cost (%CPU)| Time     |
----------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT                |                        |     1 |    42 |    76   (0)| 00:00:01 |
|   1 |  SORT AGGREGATE                 |                        |     1 |    42 |            |          |
|   2 |   NESTED LOOPS                  |                        |    22 |   924 |    76   (0)| 00:00:01 |
|   3 |    NESTED LOOPS                 |                        |   180 |   924 |    76   (0)| 00:00:01 |
|   4 |     NESTED LOOPS                |                        |    18 |   558 |    22   (0)| 00:00:01 |
|*  5 |      INDEX RANGE SCAN           | IDX_ORDER_DATE_IDC_IDO |    18 |   324 |     3   (0)| 00:00:01 |
|*  6 |      TABLE ACCESS BY INDEX ROWID| CUSTOMER               |     1 |    13 |     2   (0)| 00:00:01 |
|*  7 |       INDEX UNIQUE SCAN         | SYS_C0085575           |     1 |       |     1   (0)| 00:00:01 |
|*  8 |     INDEX RANGE SCAN            | PK_ORDERITEM           |    10 |       |     2   (0)| 00:00:01 |
|*  9 |    TABLE ACCESS BY INDEX ROWID  | ORDERITEM              |     1 |    11 |     3   (0)| 00:00:01 |

-- buffer gets : 283
-- cpu time    : 16563 µs

-- QEP Analysis:
-- FIXED    : TABLE ACCESS FULL → INDEX RANGE SCAN on Order ✅
--            cost dropped from 658 → 3
-- REMAINING: TABLE ACCESS BY INDEX ROWID on Customer ❌
--            TABLE ACCESS BY INDEX ROWID on OrderItem ❌


--=========================================================================
-- Iteration 2: Composite Index on Customer
--=========================================================================

-- Goal: Eliminate TABLE ACCESS BY INDEX ROWID on Customer
--
-- Columns identified from QEP:
--   INDEX UNIQUE SCAN SEEK: idc = [value from Order] → JOIN column → FIRST
--   TABLE ACCESS BY ROWID WHERE residence = 'Berlin' → FILTER → SECOND
--
-- idc       → FIRST  : JOIN column from Order (point query =)
--             Order passes idc values one by one via Nested Loop
--             index must be seekable by idc to enable Nested Loop join
-- residence → SECOND : WHERE filter (point query =)
--             checked together with idc in same index seek

create index idx_customer_res_idc
on Customer(idc,residence);

exec PrintQueryStat('4ajg1qjhj5zcr',1210758390);
select * from vsql_user WHERE sql_id='4ajg1qjhj5zcr';

--Plan hash value: 1210758390
--SQL_ID:4ajg1qjhj5zcr

--buffer gets: 194
--cpu time: 16526
--plan:
--------------------------------------------------------------------------------------------------------
| Id  | Operation                     | Name                   | Rows  | Bytes | Cost (%CPU)| Time     |
--------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT              |                        |     1 |    42 |    76   (0)| 00:00:01 |
|   1 |  SORT AGGREGATE               |                        |     1 |    42 |            |          |
|   2 |   NESTED LOOPS                |                        |    22 |   924 |    76   (0)| 00:00:01 |
|   3 |    NESTED LOOPS               |                        |   180 |   924 |    76   (0)| 00:00:01 |
|   4 |     NESTED LOOPS              |                        |    18 |   558 |    22   (0)| 00:00:01 |
|*  5 |      INDEX RANGE SCAN         | IDX_ORDER_DATE_IDC_IDO |    18 |   324 |     3   (0)| 00:00:01 |
|*  6 |      INDEX RANGE SCAN         | IDX_CUSTOMER_RES_IDC   |     1 |    13 |     2   (0)| 00:00:01 |
|*  7 |     INDEX RANGE SCAN          | PK_ORDERITEM           |    10 |       |     2   (0)| 00:00:01 |
|*  8 |    TABLE ACCESS BY INDEX ROWID| ORDERITEM              |     1 |    11 |     3   (0)| 00:00:01 |


-- buffer gets : 194
-- cpu time    : 16526 µs

-- QEP Analysis:
-- FIXED    : TABLE ACCESS BY INDEX ROWID(Customer) → INDEX RANGE SCAN ✅
--            seeks by idc AND residence = 'Berlin' together
--            no full Customer row fetch needed
-- REMAINING: TABLE ACCESS BY INDEX ROWID on OrderItem ❌


--=========================================================================
-- Iteration 3: Composite Index on OrderItem
--=========================================================================

-- Goal: Eliminate TABLE ACCESS BY INDEX ROWID on OrderItem
--
-- Columns identified from QEP:
--   INDEX RANGE SCAN SEEK: ido = [value from Order] → JOIN column → FIRST
--   TABLE ACCESS BY ROWID to check unit_price range → FILTER → SECOND
--
-- ido        → FIRST  : JOIN column from Order (point query =)
--              must be leading column for Nested Loop join
-- unit_price → SECOND : range filter (BETWEEN 100000 AND 200000)
--              range query goes LAST after point query ✅
--
-- NOTE: Oracle does not support INCLUDE syntax like SQL Server
--       unit_price added as key column instead
--       both ido (seek) and unit_price (range filter) in same index

create index idx_orderitem_ido_include_price on OrderItem(ido,unit_price);

exec PrintQueryStat('4ajg1qjhj5zcr',318315449);
select * from vsql_user WHERE sql_id='4ajg1qjhj5zcr';

--Plan hash value: 318315449
--SQL_ID:4ajg1qjhj5zcr

--buffer gets: 193
--cpu time: 9973
--plan:
 
-------------------------------------------------------------------------------------------------------
| Id  | Operation           | Name                            | Rows  | Bytes | Cost (%CPU)| Time     |
-------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT    |                                 |     1 |    42 |    58   (0)| 00:00:01 |
|   1 |  SORT AGGREGATE     |                                 |     1 |    42 |            |          |
|   2 |   NESTED LOOPS      |                                 |    22 |   924 |    58   (0)| 00:00:01 |
|   3 |    NESTED LOOPS     |                                 |    18 |   558 |    22   (0)| 00:00:01 |
|*  4 |     INDEX RANGE SCAN| IDX_ORDER_DATE_IDC_IDO          |    18 |   324 |     3   (0)| 00:00:01 |
|*  5 |     INDEX RANGE SCAN| IDX_CUSTOMER_RES_IDC            |     1 |    13 |     2   (0)| 00:00:01 |
|*  6 |    INDEX RANGE SCAN | IDX_ORDERITEM_IDO_INCLUDE_PRICE |     1 |    11 |     2   (0)| 00:00:01 |
-------------------------------------------------------------------------------------------------------

-- buffer gets : 193
-- cpu time    : 9973 µs

-- QEP Analysis:
-- FIXED : TABLE ACCESS BY INDEX ROWID(OrderItem) → INDEX RANGE SCAN ✅
--         seeks by ido then filters unit_price range within index
--         no full OrderItem row fetch needed

-- FINAL RESULT — Fully Optimized:
-- ✅ No TABLE ACCESS FULL
-- ✅ No TABLE ACCESS BY INDEX ROWID
-- ✅ All tables use INDEX RANGE SCAN
-- ✅ All joins use Nested Loop

-- Iteration | Index                           | Buffer Gets | CPU(µs) | Fixed
-- ----------|---------------------------------|-------------|---------|------------------
-- Baseline  | None                            | 2671        | 30443   | -
-- 1         | idx_order_date_idc_ido          | 283         | 16563   | TABLE ACCESS FULL
-- 2         | idx_customer_res_idc            | 194         | 16526   | Customer ROWID
-- 3         | idx_orderitem_ido_include_price | 193         | 9973    | OrderItem ROWID

-- WHY PHYSICAL DESIGN IS CORRECT AND OPTIMAL:
-- ✅ No TABLE ACCESS FULL     → all tables use INDEX RANGE SCAN
-- ✅ No TABLE ACCESS BY ROWID → all columns covered by indexes
-- ✅ All Nested Loop Joins    → enabled by JOIN columns as leading keys
-- ✅ Buffer gets : 2671 → 193  (93% reduction)
-- ✅ CPU time    : 30443 → 9973 µs (67% reduction)

--One Line Answer
--
--(ido, residence) leads to Nested Loop Join because ido is the leading column matching the join condition, allowing direct index seek. (residence, ido) leads to Hash Join because residence is the leading column, so the optimizer cannot seek by ido and must build a hash table instead.

--=========================================================================
-- Drop Indexes
--=========================================================================
drop index idx_order_date_idc_ido;
drop index idx_customer_res_idc;
drop index idx_orderitem_ido_include_price;