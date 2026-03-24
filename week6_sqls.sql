exec PrintPagesHeap 'OrderItem';



create or alter procedure PrintPagesClusteredTable 
  @tableName varchar(30)
as
  exec PrintPages @tableName, 1

create clustered index OrderItem on OrderItem(idOrder, idProduct);

exec PrintPagesClusteredTable 'OrderItem';

exec PrintIndexes 'OrderItem';

exec PrintPagesIndex 'pk_orderitem'; 

--------------------------------

select i.name, s.index_level as level, s.page_count, s.record_count, 
  s.avg_record_size_in_bytes as avg_record_size,
  round(s.avg_page_space_used_in_percent,1) as page_utilization, 
  round(s.avg_fragmentation_in_percent,2) as avg_frag
from sys.dm_db_index_physical_stats(DB_ID(N'kra28'), OBJECT_ID(N'OrderItem'), NULL, NULL , 'DETAILED') s
join sys.indexes i on s.object_id=i.object_id and s.index_id=i.index_id
-- where name='PK__Customer__D058768742B8AE8D'

alter table OrderItem rebuild;

drop index PK__OrderIte__CD443163B0970E7F on OrderItem;

----------------------------------------

set statistics time on;
set statistics time off;
set statistics io on;
set statistics io off;
set showplan_text on;
set showplan_text off;


select * from Customer_ct where idc=12345

  |--Clustered Index Seek(OBJECT:([PAL0343].[dbo].[Customer_ct].[PK__Customer__DC501A0DEE402327]), SEEK:([PAL0343].[dbo].[Customer_ct].[idc]=CONVERT_IMPLICIT(int,[@1],0)) ORDERED FORWARD)

select * from OrderItem 
where ido = 1235; -- 11 resp. 20 zaznamu

select * from OrderItem 
where unit_price between 10000 and 10001
option (maxdop 1);


SELECT lname, residence, COUNT(*) 
FROM Customer_ct
GROUP BY lname, residence
ORDER BY COUNT(*)

--cpu - 78ms

SELECT * FROM Customer_ct
WHERE lname='Svoboda' and fname='Pavel' and  residence='Barcelona';

----------------------------------------

create index OrderItem_unitprice on OrderItem(unit_price);

exec PrintPagesIndex 'OrderItem';

exec PrintPagesClusteredTable 'OrderItem';
exec PrintPagesIndex 'OrderItem_unitprice';

delete from OrderItem where ido % 2 = 0;

alter table OrderItem rebuild;

----------------------------------------

select * from OrderItem where ido=1;

SELECT qs.execution_count, 
 SUBSTRING(qt.text,qs.statement_start_offset/2 +1,   
                 (CASE WHEN qs.statement_end_offset = -1   
                       THEN LEN(CONVERT(nvarchar(max), qt.text)) * 2   
                       ELSE qs.statement_end_offset end -  
                            qs.statement_start_offset  
                 )/2  
             ) AS query_text,
qs.total_worker_time/qs.execution_count AS avg_cpu_time, qp.dbid, qt.text  
--   qs.plan_handle, qp.query_plan   
FROM sys.dm_exec_query_stats AS qs  
CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) as qp  
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) as qt  
where qp.dbid=DB_ID() and qs.execution_count > 10
--  and query_text LIKE '%SELECT * FROM [OrderItem]%'
ORDER BY avg_cpu_time DESC; 