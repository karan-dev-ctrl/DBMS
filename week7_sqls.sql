select * from
(
  SELECT qs.execution_count, 
    SUBSTRING(qt.text,qs.statement_start_offset/2 +1,   
                 (CASE WHEN qs.statement_end_offset = -1   
                       THEN LEN(CONVERT(nvarchar(max), qt.text)) * 2   
                       ELSE qs.statement_end_offset end -  
                            qs.statement_start_offset  
                 )/2  
             ) AS query_text,
  qs.total_worker_time/qs.execution_count AS avg_cpu_time, qp.dbid 
  --, qt.text, qs.plan_handle, qp.query_plan   
  FROM sys.dm_exec_query_stats AS qs  
  CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) as qp  
  CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) as qt  
  where qp.dbid=DB_ID() and qs.execution_count > 10
) t
where query_text like 'select * from%'
order by avg_cpu_time desc;



set statistics io on;
set statistics io off;
set showplan_text on;
set showplan_text off;
SET STATISTICS TIME ON;
SET STATISTICS TIME OFF;





select count (*) from OrderItem;

SELECT * 
FROM "OrderItem" oi
JOIN Product P ON oi.idp = P.idp
WHERE P.unit_price BETWEEN 20000000 AND 20002000;

option (maxdop 1);



--I/o 
--Order item logical reads 17922
--product  logical reads 507


--QEP 
  |--Parallelism(Gather Streams)
  |--Hash Match(Inner Join, HASH:([P].[idp])=([oi].[idp])DEFINE:([Opt_Bitmap1006]))
  |--Table Scan

--CPU
--CPU time = 625 ms,  elapsed time = 87 ms.



create index idx_product_up on Product (unit_price);

---logical reads 17922 

---QEP

|--Nested Loops
|--Index Seek
|--RID Lookup - Product
|--Table Scan

--CPU
--CPU time = 624 ms,  elapsed time = 81 ms.


create index idx_orderitem on OrderItem (idp);


--i/o
--logical reads 109


--CPU
-- CPU time = 0 ms,  elapsed time = 0 ms

--QEP

  |--Nested Loops(Inner Join, OUTER REFERENCES:([Bmk1000], [Expr1006]) WITH UNORDERED PREFETCH)
       |--Nested Loops(Inner Join, OUTER REFERENCES:([P].[idp]))
       |    |--Nested Loops(Inner Join, OUTER REFERENCES:([Bmk1002]))
       |    |    |--Index Seek(OBJECT:([PAL0343].[dbo].[Product].[idx_product_up] AS [P]), SEEK:([P].[unit_price] >= (20000000) AND [P].[unit_price] <= (20002000)) ORDERED FORWARD)
       |    |    |--RID Lookup(OBJECT:([PAL0343].[dbo].[Product] AS [P]), SEEK:([Bmk1002]=[Bmk1002]) LOOKUP ORDERED FORWARD)
       |    |--Index Seek(OBJECT:([PAL0343].[dbo].[OrderItem].[idx_orderitem] AS [oi]), SEEK:([oi].[idp]=[PAL0343].[dbo].[Product].[idp] as [P].[idp]) ORDERED FORWARD)
       |--RID Lookup(OBJECT:([PAL0343].[dbo].[OrderItem] AS [oi]), SEEK:([Bmk1000]=[Bmk1000]) LOOKUP ORDERED FORWARD)



SELECT 
    COUNT(*) AS total_rows,
    SUM(oi.quantity) AS total_quantity
FROM OrderItem oi
JOIN Product P ON oi.idp = P.idp
WHERE P.unit_price BETWEEN 20000000 AND 20002000;


create index idx_orderitem on OrderItem include (OrderItem.Quantity);

            












