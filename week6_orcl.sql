select blocks from user_segments 
where segment_name = 'ORDERITEM';
-- 16640

select index_name from user_indexes 
where table_name='ORDERITEM';
-- SYS_IOT_TOP_645742

select blocks from user_segments 
where segment_name = 'ORDERITEM';
-- no rows selected

select blocks from user_segments 
where segment_name = 'SYS_IOT_TOP_645742';
-- 16384

exec PrintPages('ORDERITEM','KRA28', 'TABLE');

alter table OrderItem shrink space;

select blocks from user_segments 
where segment_name = 'SYS_IOT_TOP_645742';
-- 16384

select * from OrderItem 
where idOrder = 1235; -- 9 resp. 20 zaznamu

-----------------------------

select * from OrderItem 
where unit_price between 10000 and 10001; -- 169 resp. 196 zaznamu

explain plan for
select * from OrderItem 
where unit_price between 10000 and 10001;

select * from table(dbms_xplan.display);

set feedback on SQL_ID;
select * from OrderItem 
where unit_price between 10000 and 10001;
set feedback off SQL_ID;

exec PrintQueryStat('bdk1yg2a3r058', 3243520670);

create index OrderItem_unitprice on OrderItem(unit_price);

------

select index_name from user_indexes 
where table_name='CUSTOMER_CT';
-- ORDERITEM_UNITPRICE
-- SYS_IOT_TOP_645742

---CUSTOMER_CT - SYS_IOT_TOP_115345
exec PrintPagesIndex('SYS_IOT_TOP_115345','PAL0343');

---PK_ORDERITEM

set feedback on SQL_ID;
select * from CUSTOMER_CT where idc=1235;
set feedback off SQL_ID;

exec PrintQueryStat('970v6umy2z34n', 2939120960);

explain plan for
select * from CUSTOMER_CT where idc=1235;
select * from table(dbms_xplan.display);

--plain hash value : 2939120960



exec PrintPagesIndex('PK_ORDERITEM', 'PAL0343');
exec PrintPagesHeap('ORDERITEM', 'PAL0343');

exec PrintPages('ORDERITEM','KRA28', 'TABLE');
exec PrintPages('ORDERITEM_UNITPRICE','KRA28', 'INDEX');



select blocks from user_segments where segment_name='ORDERITEM_UNITPRICE';
select blocks from user_segments where segment_name='SYS_IOT_TOP_645742';

-----------------

ANALYZE INDEX SYS_IOT_TOP_112142 VALIDATE STRUCTURE;

select height-1 as h, blocks, lf_blks as leaf_pages, 
br_blks as inner_pages, lf_rows as leaf_items,
br_rows as inner_items, pct_used
from index_stats where name='SYS_IOT_TOP_112142'

-----------------

exec PrintPages('SYS_IOT_TOP_112142', 'PAL0343', 'INDEX');
exec PrintPages('ORDERITEM', 'PAL0343', 'TABLE');

create or replace procedure PrintPagesHeap(p_table_name varchar,  p_user_name varchar)
as
begin
  PrintPages(p_table_name, p_user_name, 'TABLE');
end;

create or replace procedure PrintPagesIndex(p_table_name varchar,  p_user_name varchar)
as
begin
  PrintPages(p_table_name, p_user_name, 'INDEX');
end;

create or replace procedure PrintPages(
  p_table_name varchar,  p_user_name varchar, p_type varchar)
as
  blocks             number;
  bytes              number;
  unused_blocks      number;
  unused_bytes       number;
  expired_blocks     number;
  expired_bytes      number;
  unexpired_blocks   number;
  unexpired_bytes    number;
  unformatted_blocks number;
  unformatted_bytes  number;
  fs1_blocks         number;
  fs1_bytes          number;
  fs2_blocks         number;
  fs2_bytes          number;
  fs3_blocks         number;
  fs3_bytes          number;
  fs4_blocks         number;
  fs4_bytes          number;
  full_blocks        number;
  full_bytes         number;  
  used_blocks        number;
  used_bytes         number;
  mega number := 1024.0 * 1024.0;
begin 
  dbms_space.unused_space(p_user_name, p_table_name, p_type,
    blocks, bytes, unused_blocks, unused_bytes, expired_blocks,
    expired_bytes, unexpired_blocks, unexpired_bytes);
    
  dbms_space.space_usage(p_user_name, p_table_name, p_type,
    unformatted_blocks, unformatted_bytes, fs1_blocks, fs1_bytes,
    fs2_blocks, fs2_bytes, fs3_blocks, fs3_bytes, fs4_blocks,
    fs4_bytes, full_blocks, full_bytes, null);
    
  used_blocks := fs1_blocks + fs2_blocks + fs3_blocks + fs4_blocks + full_blocks;
  used_bytes := fs1_bytes + fs2_bytes + fs3_bytes + fs4_bytes + full_bytes;

  dbms_output.put_line('Allocated blocks: ' || blocks || ' (' || trunc(bytes / mega, 1) || 'MB)');
  dbms_output.put_line('Used blocks: ' || used_blocks || ' (' || trunc(used_bytes / mega, 1) || 'MB)');
end;