
SET STATISTICS IO ON;
SET STATISTICS IO OFF;
SET STATISTICS TIME ON;
SET STATISTICS TIME OFF;
SET SHOWPLAN_TEXT ON;
SET SHOWPLAN_TEXT OFF;


exec PrintIndexes 'Order';
exec PrintIndexes OrderItem
exec PrintIndexes Customer


DROP INDEX IF EXISTS IX_Order_composite ON "Order";
DROP INDEX IF EXISTS IX_Customer_residence_idc ON Customer;
DROP INDEX IF EXISTS IX_OrderItem_ido ON OrderItem;


---Baseline  result 

-- ============================================
-- BASELINE
-- ============================================
-- No additional indexes exist
-- Problems identified:
-- 1. Table Scan on Order (no index on order_datetime)
-- 2. RID Lookup on Customer (PK has no residence)
-- 3. RID Lookup on OrderItem (PK has no unit_price)

select count(*) as cnt 
from OrderItem oi
join "Order" o on o.ido = oi.ido
join Customer c on c.idc = o.idc
where o.order_datetime = '2025-05-01' and 
oi.unit_price >=100000 and oi.unit_price <=200000 and
c.residence = 'Berlin';

--logical reads - 4778
--CPU Time - 62ms
--QEP 

  |--Compute Scalar(DEFINE:([Expr1006]=CONVERT_IMPLICIT(int,[Expr1018],0)))
       |--Stream Aggregate(DEFINE:([Expr1018]=Count(*)))
            |--Nested Loops(Inner Join, OUTER REFERENCES:([Bmk1000]))
                 |--Nested Loops(Inner Join, OUTER REFERENCES:([o].[ido]))
                 |    |--Nested Loops(Inner Join, OUTER REFERENCES:([Bmk1004], [Expr1017]) WITH UNORDERED PREFETCH)
                 |    |    |--Nested Loops(Inner Join, OUTER REFERENCES:([o].[idc], [Expr1016]) WITH UNORDERED PREFETCH)
                 |    |    |    |--Table Scan(OBJECT:([PAL0343].[dbo].[Order] AS [o]), WHERE:([PAL0343].[dbo].[Order].[order_datetime] as [o].[order_datetime]='2025-05-01'))
                 |    |    |    |--Index Seek(OBJECT:([PAL0343].[dbo].[Customer].[PK__Customer__DC501A0C91986ED4] AS [c]), SEEK:([c].[idc]=[PAL0343].[dbo].[Order].[idc] as [o].[idc]) ORDERED FORWARD)
                 |    |    |--RID Lookup(OBJECT:([PAL0343].[dbo].[Customer] AS [c]), SEEK:([Bmk1004]=[Bmk1004]),  WHERE:([PAL0343].[dbo].[Customer].[residence] as [c].[residence]='Berlin') LOOKUP ORDERED FORWARD)
                 |    |--Index Seek(OBJECT:([PAL0343].[dbo].[OrderItem].[pk_orderitem] AS [oi]), SEEK:([oi].[ido]=[PAL0343].[dbo].[Order].[ido] as [o].[ido]) ORDERED FORWARD)
                 |--RID Lookup(OBJECT:([PAL0343].[dbo].[OrderItem] AS [oi]), SEEK:([Bmk1000]=[Bmk1000]),  WHERE:([PAL0343].[dbo].[OrderItem].[unit_price] as [oi].[unit_price]>=(100000) AND [PAL0343].[dbo].[OrderItem].[unit_price] as [oi].[unit_price]<=(200000)) LOOKUP ORDERED FORWARD)




-- ============================================
-- ITERATION 1
-- ============================================
-- Index: IX_Order_composite on Order(order_datetime, idc, ido)
-- Eliminates: Table Scan on Order
-- order_datetime FIRST  → WHERE filter, point query
-- idc SECOND            → JOIN to Customer, point query
-- ido THIRD             → JOIN to OrderItem, point query

CREATE INDEX IX_Order_composite
ON "Order"(order_datetime, idc, ido);

---logical reads - 423
--CPU Time - 0ms

--QEP

  |--Compute Scalar(DEFINE:([Expr1006]=CONVERT_IMPLICIT(int,[Expr1014],0)))
       |--Stream Aggregate(DEFINE:([Expr1014]=Count(*)))
            |--Nested Loops(Inner Join, OUTER REFERENCES:([Bmk1000], [Expr1013]) WITH UNORDERED PREFETCH)
                 |--Nested Loops(Inner Join, OUTER REFERENCES:([o].[ido]))
                 |    |--Nested Loops(Inner Join, OUTER REFERENCES:([Bmk1004], [Expr1012]) WITH UNORDERED PREFETCH)
                 |    |    |--Nested Loops(Inner Join, OUTER REFERENCES:([o].[idc], [Expr1011]) WITH UNORDERED PREFETCH)
                 |    |    |    |--Index Seek(OBJECT:([PAL0343].[dbo].[Order].[IX_Order_composite] AS [o]), SEEK:([o].[order_datetime]='2025-05-01') ORDERED FORWARD)
                 |    |    |    |--Index Seek(OBJECT:([PAL0343].[dbo].[Customer].[PK__Customer__DC501A0C91986ED4] AS [c]), SEEK:([c].[idc]=[PAL0343].[dbo].[Order].[idc] as [o].[idc]) ORDERED FORWARD)
                 |    |    |--RID Lookup(OBJECT:([PAL0343].[dbo].[Customer] AS [c]), SEEK:([Bmk1004]=[Bmk1004]),  WHERE:([PAL0343].[dbo].[Customer].[residence] as [c].[residence]='Berlin') LOOKUP ORDERED FORWARD)
                 |    |--Index Seek(OBJECT:([PAL0343].[dbo].[OrderItem].[pk_orderitem] AS [oi]), SEEK:([oi].[ido]=[PAL0343].[dbo].[Order].[ido] as [o].[ido]) ORDERED FORWARD)
                 |--RID Lookup(OBJECT:([PAL0343].[dbo].[OrderItem] AS [oi]), SEEK:([Bmk1000]=[Bmk1000]),  WHERE:([PAL0343].[dbo].[OrderItem].[unit_price] as [oi].[unit_price]>=(100000) AND [PAL0343].[dbo].[OrderItem].[unit_price] as [oi].[unit_price]<=(200000)) LOOKUP ORDERED FORWARD)



-- ============================================
-- ITERATION 2
-- ============================================
-- Index: IX_Customer_residence_idc on Customer(idc, residence)
-- Eliminates: RID Lookup on Customer
-- idc FIRST        → JOIN column from Order, point query
-- residence SECOND → WHERE filter, point query

CREATE INDEX IX_Customer_residence_idc
ON Customer(idc, residence);

--- logical reads - 369
---CPU Time - 0ms

---QEP : 
  |--Compute Scalar(DEFINE:([Expr1006]=CONVERT_IMPLICIT(int,[Expr1013],0)))
       |--Stream Aggregate(DEFINE:([Expr1013]=Count(*)))
            |--Nested Loops(Inner Join, OUTER REFERENCES:([Bmk1000], [Expr1012]) WITH UNORDERED PREFETCH)
                 |--Nested Loops(Inner Join, OUTER REFERENCES:([o].[ido]))
                 |    |--Nested Loops(Inner Join, OUTER REFERENCES:([o].[idc], [Expr1011]) WITH UNORDERED PREFETCH)
                 |    |    |--Index Seek(OBJECT:([PAL0343].[dbo].[Order].[IX_Order_composite] AS [o]), SEEK:([o].[order_datetime]='2025-05-01') ORDERED FORWARD)
                 |    |    |--Index Seek(OBJECT:([PAL0343].[dbo].[Customer].[IX_Customer_residence_idc] AS [c]), SEEK:([c].[idc]=[PAL0343].[dbo].[Order].[idc] as [o].[idc] AND [c].[residence]='Berlin') ORDERED FORWARD)
                 |    |--Index Seek(OBJECT:([PAL0343].[dbo].[OrderItem].[pk_orderitem] AS [oi]), SEEK:([oi].[ido]=[PAL0343].[dbo].[Order].[ido] as [o].[ido]) ORDERED FORWARD)
                 |--RID Lookup(OBJECT:([PAL0343].[dbo].[OrderItem] AS [oi]), SEEK:([Bmk1000]=[Bmk1000]),  WHERE:([PAL0343].[dbo].[OrderItem].[unit_price] as [oi].[unit_price]>=(100000) AND [PAL0343].[dbo].[OrderItem].[unit_price] as [oi].[unit_price]<=(200000)) LOOKUP ORDERED FORWARD)


-- ============================================
-- ITERATION 3
-- ============================================
-- Index: IX_OrderItem_ido on OrderItem(ido) INCLUDE(unit_price)
-- Eliminates: RID Lookup on OrderItem
-- ido KEY COLUMN      → JOIN column from Order, point query
-- unit_price INCLUDE  → range filter, stored not sorted

CREATE INDEX IX_OrderItem_ido
ON OrderItem(ido)
INCLUDE (unit_price);

---logical read - 309
--CPU Time - 0ms


  |--Compute Scalar(DEFINE:([Expr1006]=CONVERT_IMPLICIT(int,[Expr1013],0)))
       |--Stream Aggregate(DEFINE:([Expr1013]=Count(*)))
            |--Nested Loops(Inner Join, OUTER REFERENCES:([o].[ido]))
                 |--Nested Loops(Inner Join, OUTER REFERENCES:([o].[idc], [Expr1012]) WITH UNORDERED PREFETCH)
                 |    |--Index Seek(OBJECT:([PAL0343].[dbo].[Order].[IX_Order_composite] AS [o]), SEEK:([o].[order_datetime]='2025-05-01') ORDERED FORWARD)
                 |    |--Index Seek(OBJECT:([PAL0343].[dbo].[Customer].[IX_Customer_residence_idc] AS [c]), SEEK:([c].[idc]=[PAL0343].[dbo].[Order].[idc] as [o].[idc] AND [c].[residence]='Berlin') ORDERED FORWARD)
                 |--Index Seek(OBJECT:([PAL0343].[dbo].[OrderItem].[IX_OrderItem_ido] AS [oi]), SEEK:([oi].[ido]=[PAL0343].[dbo].[Order].[ido] as [o].[ido]),  WHERE:([PAL0343].[dbo].[OrderItem].[unit_price] as [oi].[unit_price]>=(100000) AND [PAL0343].[dbo].[OrderItem].[unit_price] as [oi].[unit_price]<=(200000)) ORDERED FORWARD)

-- ============================================
-- DROP indexes at END (make script rerunnable)
-- ============================================
DROP INDEX IF EXISTS IX_Order_composite ON "Order";
DROP INDEX IF EXISTS IX_Customer_residence_idc ON Customer;
DROP INDEX IF EXISTS IX_OrderItem_ido ON OrderItem;





----Oracle 

---baseline index

---QEP :
--|   0 | SELECT STATEMENT                |              |     1 |    42 |   748   (2)| 00:00:01 |
--|   1 |  SORT AGGREGATE                 |              |     1 |    42 |            |          |
--|   2 |   NESTED LOOPS                  |              |    18 |   756 |   748   (2)| 00:00:01 |
--|   3 |    NESTED LOOPS                 |              |   180 |   756 |   748   (2)| 00:00:01 |
--|   4 |     NESTED LOOPS                |              |    18 |   558 |   694   (2)| 00:00:01 |
--|*  5 |      TABLE ACCESS FULL          | Order        |    18 |   324 |   658   (2)| 00:00:01 |
--|*  6 |      TABLE ACCESS BY INDEX ROWID| CUSTOMER     |     1 |    13 |     2   (0)| 00:00:01 |
--|*  7 |       INDEX UNIQUE SCAN         | SYS_C0091653 |     1 |       |     1   (0)| 00:00:01 |
--|*  8 |     INDEX RANGE SCAN            | PK_ORDERITEM |    10 |       |     2   (0)| 00:00:01 |



----Iteration 1

--CPU Time - 17
---IO/cost - 283

--QEP :

--|   0 | SELECT STATEMENT                |                    |     1 |    42 |    76   (0)| 00:00:01 |
--|   1 |  SORT AGGREGATE                 |                    |     1 |    42 |            |          |
--|   2 |   NESTED LOOPS                  |                    |    18 |   756 |    76   (0)| 00:00:01 |
--|   3 |    NESTED LOOPS                 |                    |   180 |   756 |    76   (0)| 00:00:01 |
--|   4 |     NESTED LOOPS                |                    |    18 |   558 |    22   (0)| 00:00:01 |
--|*  5 |      INDEX RANGE SCAN           | IX_ORDER_COMPOSITE |    18 |   324 |     3   (0)| 00:00:01 |
--|*  6 |      TABLE ACCESS BY INDEX ROWID| CUSTOMER           |     1 |    13 |     2   (0)| 00:00:01 |
--|*  7 |       INDEX UNIQUE SCAN         | SYS_C0091653       |     1 |       |     1   (0)| 00:00:01 |
--|*  8 |     INDEX RANGE SCAN            | PK_ORDERITEM       |    10 |       |     2   (0)| 00:00:01 |
--|*  9 |    TABLE ACCESS BY INDEX ROWID  | ORDERITEM          |     1 |    11 |     3   (0)| 00:00:01 |
--|*  9 |    TABLE ACCESS BY INDEX ROWID  | ORDERITEM    |     1 |    11 |     3   (0)| 00:00:01 |


----iteration 2
---QEP : 

--|   0 | SELECT STATEMENT              |                           |     1 |    42 |    76   (0)| 00:00:01 |
--|   1 |  SORT AGGREGATE               |                           |     1 |    42 |            |          |
--|   2 |   NESTED LOOPS                |                           |    18 |   756 |    76   (0)| 00:00:01 |
--|   3 |    NESTED LOOPS               |                           |   180 |   756 |    76   (0)| 00:00:01 |
--|   4 |     NESTED LOOPS              |                           |    18 |   558 |    22   (0)| 00:00:01 |
--|*  5 |      INDEX RANGE SCAN         | IX_ORDER_COMPOSITE        |    18 |   324 |     3   (0)| 00:00:01 |
--|*  6 |      INDEX RANGE SCAN         | IX_CUSTOMER_RESIDENCE_IDC |     1 |    13 |     2   (0)| 00:00:01 |
--|*  7 |     INDEX RANGE SCAN          | PK_ORDERITEM              |    10 |       |     2   (0)| 00:00:01 |
--|*  8 |    TABLE ACCESS BY INDEX ROWID| ORDERITEM                 |     1 |    11 |     3   (0)| 00:00:01 |
