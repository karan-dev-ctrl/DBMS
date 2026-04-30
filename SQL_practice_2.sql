--================
-- Statistics & Helper
--================


SET STATISTICS IO ON;
SET STATISTICS IO OFF;
SET STATISTICS TIME ON;
SET STATISTICS TIME OFF;
SET SHOWPLAN_TEXT ON;
SET SHOWPLAN_TEXT OFF;


exec PrintIndexes 'Order';
exec PrintIndexes OrderItem
exec PrintIndexes Customer
exec PrintIndexes Staff

-- ============================================
-- DROP indexes at START (make script rerunnable)
-- ============================================
DROP INDEX IF EXISTS IX_Order_datetime_ids ON "Order";
DROP INDEX IF EXISTS IX_Staff_idsa_residence ON Staff;
DROP INDEX IF EXISTS IX_Staff_residence_idsa ON Staff;
DROP INDEX IF EXISTS IX_Customer_idc_residence ON Customer;
DROP INDEX IF EXISTS IX_OrderItem_ido_quantity ON OrderItem;


-- =========================================
-- Selectivity Analysis
-- =========================================


select count(*) as ord from "Order"; --501414
select count(*) as cust from Customer; --300000
select count(*) as ordit from OrderItem; --5000000
select count(*) as sta from Staff; --10000

select count(*) as sa_res from Staff
where residence = 'Praha' --- 488/10000 = 0.0488 = 4.88% -- second

select count(*) as cus_res from Customer
where residence = 'Praha' ---15021/300000 = 0.05007 = 5.007% -- third

select count(*) as dat_ti from "Order"
where order_datetime between '2025-01-01' and '2025-01-31'; ---2322/501414 = 0.00463090 =0.46% --first

----------------------------------------------------------------



-- =========================================
-- Iteration 0: Baseline
-- =========================================

-- Problems found in QEP:
-- 1. Table Scan on Order   → no index on order_datetime, reads all 501414 rows
-- 2. Table Scan on Staff   → no index on residence, reads all 10000 rows
-- 3. Hash Match(Order,Staff) → caused by both tables having Table Scans
-- 4. RID Lookup on Customer  → residence not in PK, fetches full row
-- 5. RID Lookup on OrderItem → quantity not in PK, fetches full row


select count(*) cnt, sum(oi.quantity) quant
from Staff sa
join "Order" o on sa.idsa=o.idsa
join Customer c on o.idc=c.idc
join OrderItem oi on o.ido=oi.ido
where sa.residence = 'Praha' and c.residence='Praha' and
o.order_datetime between '2025-01-01' and '2025-01-31'
option (maxdop 1);




--QEP 

      |--Compute Scalar(DEFINE:([Expr1008]=CONVERT_IMPLICIT(int,[Expr1033],0), [Expr1009]=CASE WHEN [Expr1033]=(0) THEN NULL ELSE [Expr1034] END))
       |--Stream Aggregate(DEFINE:([Expr1033]=Count(*), [Expr1034]=SUM([PAL0343].[dbo].[OrderItem].[quantity] as [oi].[quantity])))
            |--Nested Loops(Inner Join, OUTER REFERENCES:([Bmk1006], [Expr1032]) WITH UNORDERED PREFETCH)
                 |--Nested Loops(Inner Join, OUTER REFERENCES:([o].[ido]))
                 |    |--Nested Loops(Inner Join, OUTER REFERENCES:([Bmk1004], [Expr1031]) WITH UNORDERED PREFETCH)
                 |    |    |--Nested Loops(Inner Join, OUTER REFERENCES:([o].[idc], [Expr1030]) WITH UNORDERED PREFETCH)
                 |    |    |    |--Hash Match(Inner Join, HASH:([sa].[idsa])=([o].[idsa]), RESIDUAL:([PAL0343].[dbo].[Order].[idsa] as [o].[idsa]=[PAL0343].[dbo].[Staff].[idsa] as [sa].[idsa])DEFINE:([Opt_Bitmap1012]))
                 |    |    |    |    |--Table Scan(OBJECT:([PAL0343].[dbo].[Staff] AS [sa]),  WHERE:([PAL0343].[dbo].[Staff].[residence] as [sa].[residence]='Praha'))
                 |    |    |    |    |--Filter(WHERE:(PROBE([Opt_Bitmap1012],[PAL0343].[dbo].[Order].[idsa] as [o].[idsa])))
                 |    |    |    |         |--Table Scan(OBJECT:([PAL0343].[dbo].[Order] AS [o]),  WHERE:([PAL0343].[dbo].[Order].[order_datetime] as [o].[order_datetime]>='2025-01-01' AND [PAL0343].[dbo].[Order].[order_datetime] as [o].[order_datetime]<='2025-01-31'))
                 |    |    |    |--Index Seek(OBJECT:([PAL0343].[dbo].[Customer].[PK__Customer__DC501A0C91986ED4] AS [c]), SEEK:([c].[idc]=[PAL0343].[dbo].[Order].[idc] as [o].[idc]) ORDERED FORWARD)
                 |    |    |--RID Lookup(OBJECT:([PAL0343].[dbo].[Customer] AS [c]), SEEK:([Bmk1004]=[Bmk1004]),  WHERE:([PAL0343].[dbo].[Customer].[residence] as [c].[residence]='Praha') LOOKUP ORDERED FORWARD)
                 |    |--Index Seek(OBJECT:([PAL0343].[dbo].[OrderItem].[pk_orderitem] AS [oi]), SEEK:([oi].[ido]=[PAL0343].[dbo].[Order].[ido] as [o].[ido]) ORDERED FORWARD)
                 |--RID Lookup(OBJECT:([PAL0343].[dbo].[OrderItem] AS [oi]), SEEK:([Bmk1006]=[Bmk1006]) LOOKUP ORDERED FORWARD)


-- =========================================
-- Iteration 1: Composite Index on Order
-- =========================================

-- Goal: Eliminate Table Scan on Order
--
-- Which columns to include → look at query:
--   WHERE  o.order_datetime BETWEEN ... → filter column  → FIRST
--   JOIN   o.idc = c.idc               → join Customer  → SECOND
--   JOIN   o.idsa = sa.idsa            → join Staff     → THIRD
--   JOIN   o.ido = oi.ido              → join OrderItem → FOURTH
--
-- order_datetime → FIRST  : most selective (0.46%), range query
-- idc            → SECOND : JOIN to Customer, point query
-- idsa           → THIRD  : JOIN to Staff, point query
-- ido            → FOURTH : JOIN to OrderItem, point query

CREATE INDEX IX_Order_datetime_ids
ON "Order"(order_datetime, idc, idsa, ido);

--QEP
  |--Compute Scalar(DEFINE:([Expr1008]=CONVERT_IMPLICIT(int,[Expr1023],0), [Expr1009]=CASE WHEN [Expr1023]=(0) THEN NULL ELSE [Expr1024] END))
       |--Stream Aggregate(DEFINE:([Expr1023]=Count(*), [Expr1024]=SUM([PAL0343].[dbo].[OrderItem].[quantity] as [oi].[quantity])))
            |--Nested Loops(Inner Join, OUTER REFERENCES:([Bmk1006], [Expr1022]) WITH UNORDERED PREFETCH)
                 |--Nested Loops(Inner Join, OUTER REFERENCES:([o].[ido]))
                 |    |--Nested Loops(Inner Join, OUTER REFERENCES:([Bmk1004], [Expr1021]) WITH UNORDERED PREFETCH)
                 |    |    |--Nested Loops(Inner Join, OUTER REFERENCES:([o].[idc], [Expr1020]) WITH UNORDERED PREFETCH)
                 |    |    |    |--Hash Match(Inner Join, HASH:([sa].[idsa])=([o].[idsa]), RESIDUAL:([PAL0343].[dbo].[Order].[idsa] as [o].[idsa]=[PAL0343].[dbo].[Staff].[idsa] as [sa].[idsa])DEFINE:([Opt_Bitmap1012]))
                 |    |    |    |    |--Table Scan(OBJECT:([PAL0343].[dbo].[Staff] AS [sa]),  WHERE:([PAL0343].[dbo].[Staff].[residence] as [sa].[residence]='Praha'))
                 |    |    |    |    |--Filter(WHERE:(PROBE([Opt_Bitmap1012],[PAL0343].[dbo].[Order].[idsa] as [o].[idsa])))
                 |    |    |    |         |--Index Seek(OBJECT:([PAL0343].[dbo].[Order].[IX_Order_datetime_ids] AS [o]), SEEK:([o].[order_datetime] >= '2025-01-01' AND [o].[order_datetime] <= '2025-01-31') ORDERED FORWARD)
                 |    |    |    |--Index Seek(OBJECT:([PAL0343].[dbo].[Customer].[PK__Customer__DC501A0C91986ED4] AS [c]), SEEK:([c].[idc]=[PAL0343].[dbo].[Order].[idc] as [o].[idc]) ORDERED FORWARD)
                 |    |    |--RID Lookup(OBJECT:([PAL0343].[dbo].[Customer] AS [c]), SEEK:([Bmk1004]=[Bmk1004]),  WHERE:([PAL0343].[dbo].[Customer].[residence] as [c].[residence]='Praha') LOOKUP ORDERED FORWARD)
                 |    |--Index Seek(OBJECT:([PAL0343].[dbo].[OrderItem].[pk_orderitem] AS [oi]), SEEK:([oi].[ido]=[PAL0343].[dbo].[Order].[ido] as [o].[ido]) ORDERED FORWARD)
                 |--RID Lookup(OBJECT:([PAL0343].[dbo].[OrderItem] AS [oi]), SEEK:([Bmk1006]=[Bmk1006]) LOOKUP ORDERED FORWARD)
 


 -- =========================================
-- Iteration 2: Composite Index on Staff
-- =========================================

-- Goal: Eliminate Table Scan on Staff
--
-- Which columns to include → look at query:
--   WHERE  sa.residence = 'Praha'  → filter column → FIRST or SECOND
--   JOIN   sa.idsa = o.idsa        → join Order    → FIRST or SECOND
--
-- Two options:
-- Option A: (residence, idsa) → seek Praha first, then idsa available
-- Option B: (idsa, residence) → seek idsa first from Order, then check Praha
--
-- residence → FIRST : WHERE filter (point query =)
--             seeks only 488 Praha staff immediately
--             reduces rows early before join
-- idsa      → SECOND: JOIN column to Order (point query =)
--             available after residence seek

CREATE INDEX IX_Staff_idsa_residence
ON Staff(idsa, residence);

                 |--Nested Loops(Inner Join, OUTER REFERENCES:([o].[ido]))
                 |    |--Nested Loops(Inner Join, OUTER REFERENCES:([Bmk1004], [Expr1021]) WITH UNORDERED PREFETCH)
                 |    |    |--Nested Loops(Inner Join, OUTER REFERENCES:([o].[idc], [Expr1020]) WITH UNORDERED PREFETCH)
                 |    |    |    |--Hash Match(Inner Join, HASH:([sa].[idsa])=([o].[idsa]), RESIDUAL:([PAL0343].[dbo].[Order].[idsa] as [o].[idsa]=[PAL0343].[dbo].[Staff].[idsa] as [sa].[idsa])DEFINE:([Opt_Bitmap1012]))
                 |    |    |    |    |--Index Seek(OBJECT:([PAL0343].[dbo].[Staff].[IX_Staff_residence_idsa] AS [sa]), SEEK:([sa].[residence]='Praha') ORDERED FORWARD)
                 |    |    |    |    |--Filter(WHERE:(PROBE([Opt_Bitmap1012],[PAL0343].[dbo].[Order].[idsa] as [o].[idsa])))
                 |    |    |    |         |--Index Seek(OBJECT:([PAL0343].[dbo].[Order].[IX_Order_datetime_ids] AS [o]), SEEK:([o].[order_datetime] >= '2025-01-01' AND [o].[order_datetime] <= '2025-01-31') ORDERED FORWARD)
                 |    |    |    |--Index Seek(OBJECT:([PAL0343].[dbo].[Customer].[PK__Customer__DC501A0C91986ED4] AS [c]), SEEK:([c].[idc]=[PAL0343].[dbo].[Order].[idc] as [o].[idc]) ORDERED FORWARD)
                 |    |    |--RID Lookup(OBJECT:([PAL0343].[dbo].[Customer] AS [c]), SEEK:([Bmk1004]=[Bmk1004]),  WHERE:([PAL0343].[dbo].[Customer].[residence] as [c].[residence]='Praha') LOOKUP ORDERED FORWARD)
                 |    |--Index Seek(OBJECT:([PAL0343].[dbo].[OrderItem].[pk_orderitem] AS [oi]), SEEK:([oi].[ido]=[PAL0343].[dbo].[Order].[ido] as [o].[ido]) ORDERED FORWARD)
                 |--RID Lookup(OBJECT:([PAL0343].[dbo].[OrderItem] AS [oi]), SEEK:([Bmk1006]=[Bmk1006]) LOOKUP ORDERED FORWARD)


=========================================
-- Iteration 3: Composite Index on Customer
-- =========================================

-- Goal: Eliminate RID Lookup on Customer
--
-- idc       → FIRST : JOIN column from Order (point query =)
--             Order provides idc values one by one via Nested Loop
--             index must be seekable by idc first
-- residence → SECOND: WHERE filter (point query =)
--             checked together with idc in same seek

CREATE INDEX IX_Customer_idc_residence
ON Customer(idc, residence);

  |--Compute Scalar(DEFINE:([Expr1008]=CONVERT_IMPLICIT(int,[Expr1021],0), [Expr1009]=CASE WHEN [Expr1021]=(0) THEN NULL ELSE [Expr1022] END))
       |--Stream Aggregate(DEFINE:([Expr1021]=Count(*), [Expr1022]=SUM([PAL0343].[dbo].[OrderItem].[quantity] as [oi].[quantity])))
            |--Nested Loops(Inner Join, OUTER REFERENCES:([Bmk1006], [Expr1020]) WITH UNORDERED PREFETCH)
                 |--Nested Loops(Inner Join, OUTER REFERENCES:([o].[ido]))
                 |    |--Nested Loops(Inner Join, OUTER REFERENCES:([o].[idc], [Expr1019]) WITH UNORDERED PREFETCH)
                 |    |    |--Hash Match(Inner Join, HASH:([sa].[idsa])=([o].[idsa]), RESIDUAL:([PAL0343].[dbo].[Order].[idsa] as [o].[idsa]=[PAL0343].[dbo].[Staff].[idsa] as [sa].[idsa])DEFINE:([Opt_Bitmap1012]))
                 |    |    |    |--Index Seek(OBJECT:([PAL0343].[dbo].[Staff].[IX_Staff_residence_idsa] AS [sa]), SEEK:([sa].[residence]='Praha') ORDERED FORWARD)
                 |    |    |    |--Filter(WHERE:(PROBE([Opt_Bitmap1012],[PAL0343].[dbo].[Order].[idsa] as [o].[idsa])))
                 |    |    |         |--Index Seek(OBJECT:([PAL0343].[dbo].[Order].[IX_Order_datetime_ids] AS [o]), SEEK:([o].[order_datetime] >= '2025-01-01' AND [o].[order_datetime] <= '2025-01-31') ORDERED FORWARD)
                 |    |    |--Index Seek(OBJECT:([PAL0343].[dbo].[Customer].[IX_Customer_idc_residence] AS [c]), SEEK:([c].[idc]=[PAL0343].[dbo].[Order].[idc] as [o].[idc] AND [c].[residence]='Praha') ORDERED FORWARD)
                 |    |--Index Seek(OBJECT:([PAL0343].[dbo].[OrderItem].[pk_orderitem] AS [oi]), SEEK:([oi].[ido]=[PAL0343].[dbo].[Order].[ido] as [o].[ido]) ORDERED FORWARD)
                 |--RID Lookup(OBJECT:([PAL0343].[dbo].[OrderItem] AS [oi]), SEEK:([Bmk1006]=[Bmk1006]) LOOKUP ORDERED FORWARD)


=========================================
-- Iteration 4: Covering Index on OrderItem
-- =========================================

-- Goal: Eliminate RID Lookup on OrderItem
--
-- ido      → KEY COLUMN: JOIN from Order (point query =)
--            must be first for direct seek
-- quantity → INCLUDE   : needed for SUM(quantity) projection
--            not a filter column, only needs to be READ
--            INCLUDE stores value at leaf level
--            no need to fetch full OrderItem row

CREATE INDEX IX_OrderItem_ido_quantity
ON OrderItem(ido)
INCLUDE (quantity);

  |--Compute Scalar(DEFINE:([Expr1008]=CONVERT_IMPLICIT(int,[Expr1020],0), [Expr1009]=CASE WHEN [Expr1020]=(0) THEN NULL ELSE [Expr1021] END))
       |--Stream Aggregate(DEFINE:([Expr1020]=Count(*), [Expr1021]=SUM([PAL0343].[dbo].[OrderItem].[quantity] as [oi].[quantity])))
            |--Nested Loops(Inner Join, OUTER REFERENCES:([o].[ido]))
                 |--Nested Loops(Inner Join, OUTER REFERENCES:([o].[idc], [Expr1019]) WITH UNORDERED PREFETCH)
                 |    |--Hash Match(Inner Join, HASH:([sa].[idsa])=([o].[idsa]), RESIDUAL:([PAL0343].[dbo].[Order].[idsa] as [o].[idsa]=[PAL0343].[dbo].[Staff].[idsa] as [sa].[idsa])DEFINE:([Opt_Bitmap1012]))
                 |    |    |--Index Seek(OBJECT:([PAL0343].[dbo].[Staff].[IX_Staff_residence_idsa] AS [sa]), SEEK:([sa].[residence]='Praha') ORDERED FORWARD)
                 |    |    |--Filter(WHERE:(PROBE([Opt_Bitmap1012],[PAL0343].[dbo].[Order].[idsa] as [o].[idsa])))
                 |    |         |--Index Seek(OBJECT:([PAL0343].[dbo].[Order].[IX_Order_datetime_ids] AS [o]), SEEK:([o].[order_datetime] >= '2025-01-01' AND [o].[order_datetime] <= '2025-01-31') ORDERED FORWARD)
                 |    |--Index Seek(OBJECT:([PAL0343].[dbo].[Customer].[IX_Customer_idc_residence] AS [c]), SEEK:([c].[idc]=[PAL0343].[dbo].[Order].[idc] as [o].[idc] AND [c].[residence]='Praha') ORDERED FORWARD)
                 |--Index Seek(OBJECT:([PAL0343].[dbo].[OrderItem].[IX_OrderItem_ido_quantity] AS [oi]), SEEK:([oi].[ido]=[PAL0343].[dbo].[Order].[ido] as [o].[ido]) ORDERED FORWARD)


 ---SQL SERVER OPTIMIZER CHOICE:
--    Order date range returns 2322 rows
--    Staff Praha filter returns 488 rows
--    Optimizer calculates:
--    Hash Match cost  = build hash table from 488 Praha staff
--                       probe with 2322 Order rows = cheaper
--    Nested Loop cost = 2322 seeks into Staff index = more expensive
--    Therefore optimizer chooses Hash Match



-- =========================================
-- SUMMARY
-- =========================================

-- Iteration | Index Created                              | Problem Fixed
-- ----------|--------------------------------------------|---------------------------
-- Baseline  | None                                       | -
-- 1         | IX_Order_datetime_ids(datetime,idc,idsa,ido)| Table Scan Order removed
-- 2         | IX_Staff_residence_idsa(residence,idsa)    | Table Scan Staff removed
-- 3         | IX_Customer_idc_residence(idc,residence)   | Customer RID Lookup removed
-- 4         | IX_OrderItem_ido_quantity(ido) INCLUDE(qty) | OrderItem RID Lookup removed

-- WHY PHYSICAL DESIGN IS OPTIMAL (with one exception):
-- ✅ No Table Scans        → all tables use Index Seek
-- ✅ No RID Lookups        → all required columns covered by indexes
-- ✅ Nested Loop Joins     → Customer and OrderItem use Nested Loop
-- ⚠️ Hash Match persists  → Staff vs Order join
--    This is the optimizer's best choice given:
--    → conflicting index requirements on Staff
--    → 488 Praha staff rows vs 2322 Order date rows
--    → Hash Match cost is low, both sides already use Index Seek

-- ============================================
-- DROP indexes at END (make script rerunnable)
-- ============================================ 
DROP INDEX IF EXISTS IX_Order_datetime_ids ON "Order";
DROP INDEX IF EXISTS IX_Staff_idsa_residence ON Staff;
DROP INDEX IF EXISTS IX_Customer_idc_residence ON Customer;
DROP INDEX IF EXISTS IX_OrderItem_ido_quantity ON OrderItem;
DROP INDEX IF EXISTS IX_Staff_residence_idsa ON Staff;
