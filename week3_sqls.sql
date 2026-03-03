
SET STATISTICS IO ON;
SET STATISTICS IO OFF;
SET SHOWPLAN_TEXT ON;
SET SHOWPLAN_TEXT OFF;
--SET SHOWPLAN_ALL ON;
--SET SHOWPLAN_ALL OFF;
SET STATISTICS TIME ON;
SET STATISTICS TIME OFF;

select * from Customer 
  where birthday =  '2000-01-01';


exec PrintPagesHeap 'Customer';

---------------------------------------

SET STATISTICS IO ON;
SET STATISTICS TIME ON;
SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;

select * from Product 
where unit_price between 1300000 and 1800000;

select * from OrderItem 
where orderitem.unit_price between 1 and 500;

select * from OrderItem 
where orderitem.unit_price between 1 and 300
OPTION (MAXDOP 1);

exec PrintPagesHeap 'OrderItem';

-----------------------------

SET STATISTICS IO ON;
SET STATISTICS TIME ON;
SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;

select * from Customer
where fname = 'Jana' and lname='Pokorná' and residence = 'Berlin';
-- 67

exec PrintPagesHeap 'Customer';

---------------------

truncate table OrderItem;
delete from "Order";

delete from Customer where idc % 3 = 0;    -- SQL Server

select * from Customer
where fname = 'Jana' and lname='Pokorná' and residence = 'Berlin';
-- 46

alter table Customer rebuild;







