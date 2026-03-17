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

------------------------------------

explain plan for select * from Customer 
where birthday = TO_DATE('01.01.2000', 'DD.MM.YYYY');

select * from table(dbms_xplan.display);
-- Plan hash value: 2844954298

---------------------------------------------------

col fname format a15;
col lname format a15;
col residence format a15;

set feedback on SQL_ID;

select count(*) from Customer
where fname = 'Jana' and lname='PokornĂˇ' and residence = 'Praha';

set feedback off SQL_ID;

-- SQL_ID: 2r8n26ha61779

explain plan for select * from Customer
where fname = 'Jana' and lname='PokornĂˇ' and residence = 'Praha';

select * from table(dbms_xplan.display);

-- Plan hash value: 2844954298

exec PrintQueryStat('2r8n26ha61779', 2844954298);

-----------------------

--Week 3 - task

set feedback on SQL_ID;
select * from OrderItem where orderitem.unit_price between 1 and 300; 
set feedback off SQL_ID;
-- 59
-- SQL_ID: 56xntf3s3yzbq

explain plan for select * from OrderItem where orderitem.unit_price between 1 and 300; 

select * from table(dbms_xplan.display);
-- Plan hash value: 4294024870

exec PrintQueryStat('56xntf3s3yzbq', 4294024870);

-----------------------

-- sys user
ALTER SYSTEM SET parallel_degree_policy = MANUAL;
-- ALTER SYSTEM SET parallel_degree_policy = AUTO;

----------parallel execution

select degree
from user_tables
where table_name='ORDERITEM';
-- DEGREE: 1

alter table OrderItem parallel (degree 4);
-- DEGREE: 4

set feedback on SQL_ID;
select * from OrderItem where orderitem.unit_price between 1 and 300; 
set feedback off SQL_ID;
-- 59
-- SQL_ID: 56xntf3s3yzbq

explain plan for
select * from OrderItem where orderitem.unit_price between 1 and 300; 

select * from table(dbms_xplan.display);

exec PrintQueryStat('56xntf3s3yzbq', 892791741);

--------------

explain plan for
select /*+ no_parallel(oi) */ * from OrderItem oi 
where oi.unit_price between 1 and 300; 

select * from table(dbms_xplan.display);

---------------------------

set feedback on SQL_ID;

select * from Customer
where fname = 'Jana' and lname='PokornĂˇ' and residence = 'Berlin';

-- select count(*) from Customer
-- where fname = 'Jana' and lname='PokornĂˇ' and residence = 'Berlin';

set feedback off SQL_ID;

-- SQL_ID: c04y5q84zbd2y

explain plan for select * from Customer
where fname = 'Jana' and lname='PokornĂˇ' and residence = 'Berlin';

select * from table(dbms_xplan.display);

-- Plan hash value: 2844954298

exec PrintQueryStat('c04y5q84zbd2y', 2844954298);

---------------------------

truncate table OrderItem;
truncate table "Order";

delete from Customer where mod(idc, 3)=0;  -- Oracle

select count (*) from Customer;

exec printpages_space_usage('CUSTOMER', 'KRA28', 'TABLE');

alter table Customer enable row movement;
alter table Customer shrink space;

---------------------------


--week 5


SELECT lname, fname, residence, COUNT(*) 
FROM Customer
GROUP BY lname, fname, residence
ORDER BY COUNT(*)

--9 , 192

SELECT lname, fname, COUNT(*) 
FROM Customer
GROUP BY lname, fname
ORDER BY COUNT(*)

--678, 2889


SELECT lname, residence, COUNT(*) 
FROM Customer
GROUP BY lname, residence
ORDER BY COUNT(*)

--319, 1414


explain plan for
SELECT * FROM Customer
WHERE lname='Müller' and fname='Alena' and  residence='Praha';

select * from table(dbms_xplan.display);

--Plan hash value: 2844954298

set feedback on SQL_ID;
SELECT * FROM Customer
WHERE lname='Müller' and fname='Alena' and  residence='Praha';
set feedback off SQL_ID;

--SQL_ID: 2mmp45np5wjz9

exec PrintQueryStat('2mmp45np5wjz9', 2844954298);















