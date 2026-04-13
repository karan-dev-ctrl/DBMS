--order--orderitem--product


SET STATISTICS IO ON;
SET STATISTICS IO OFF;
SET STATISTICS TIME ON;
SET STATISTICS TIME OFF;
SET SHOWPLAN_TEXT ON;
SET SHOWPLAN_TEXT OFF;


select count(*) as cnt, count(distinct o.idc) as cust
from "Order" o
join OrderItem oi on o.ido = oi.ido
join Product p on p.idp = oi.idp
where p.unit_price between 10000 and 10050 and
oi.unit_price between 10000 and 10050

---before creating index

--I/O

----logical reads 316 - Order
--logical reads 17922 - OrderItem
--logical reads 508, - Product

--CPU

--SQL Server Execution Times:
-- CPU time = 625 ms,  elapsed time = 95 ms.

---SHOWPLAN
---stream aggregate
---merge join
---bitmap
---parallelism
---index seek
---rid lookup


--after creating index


exec PrintIndexes 'OrderItem';

create index idx_orderitem_price
on OrderItem(unit_price);



create index idx_product_price
on Product(unit_price);

exec PrintIndexes 'Product';



---I/O

--logical reads 316 - Order
--logical reads 4243 - OrderItem
--logical reads 64, - Product

--CPU

-- SQL Server Execution Times:
-- CPU time = 32 ms,  elapsed time = 31 ms.

--SHOWPLAN

---Nested loops
--hash match
-- index seek
---rid lookup
---filter


--dropping the index

drop index idx_product_price on Product;
drop index idx_orderitem_price on OrderItem;