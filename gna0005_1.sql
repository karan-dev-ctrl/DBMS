--================
-- Statistics
--=================
set statistics time on;
set statistics time off;
set statistics io on;
set statistics io off;
set showplan_text on;
set showplan_text off;

--================
-- Helper Functions
--=================
exec PrintIndexes "OrderItem";

--=================
-- Query - Sequential execution
--==================
SELECT COUNT(*) as Count FROM OrderItem oi
JOIN "Order" o ON oi.ido = o.ido
JOIN Customer c ON o.idc = c.idc
WHERE c.residence = 'Berlin' AND o.order_datetime = '2025-05-01' AND oi.unit_price >= 100000 AND oi.unit_price <= 200000 option (maxdop 1);

-- =========================================
-- Selectivity analysis
-- =========================================
select count(*) from Customer; -- 300000
select count(*) from Customer WHERE residence='Berlin'; -- 14643
-- Selectivity = 14643/300000 = 0.048 = 4.8%

select count(*) from "Order"; -- 501414
select count(*) from "Order" WHERE order_datetime = '2025-05-01'; -- 82
-- selectivity = 82/501414 = 0.000016 (Highly selective query)

select count(*) from "OrderItem"; -- 5000000
select count(*) from "OrderItem" WHERE unit_price >= 100000 AND unit_price <= 200000; -- 220897
-- selectivity = 220897 / 5000000 = 0.0441 = 4.1%


-- =========================================
-- Iteration 0: baseline query without additional indexes
-- =========================================

-- CPU: 63 ms, IO Cost: 333 + 78  + 2179 = ~2800

-- QEP:
  |--Compute Scalar(DEFINE:([Expr1006]=CONVERT_IMPLICIT(int,[Expr1019],0)))
       |--Stream Aggregate(DEFINE:([Expr1019]=Count(*)))
            |--Nested Loops(Inner Join, OUTER REFERENCES:([Bmk1000], [Expr1018]) WITH UNORDERED PREFETCH)
                 |--Nested Loops(Inner Join, OUTER REFERENCES:([o].[ido]))
                 |    |--Nested Loops(Inner Join, OUTER REFERENCES:([Bmk1004], [Expr1017]) WITH UNORDERED PREFETCH)
                 |    |    |--Nested Loops(Inner Join, OUTER REFERENCES:([o].[idc], [Expr1016]) WITH UNORDERED PREFETCH)
                 |    |    |    |--Table Scan(OBJECT:([GNA0005].[dbo].[Order] AS [o]), WHERE:([GNA0005].[dbo].[Order].[order_datetime] as [o].[order_datetime]='2025-05-01'))
                 |    |    |    |--Index Seek(OBJECT:([GNA0005].[dbo].[Customer].[PK__Customer__DC501A0CBCCF2642] AS [c]), SEEK:([c].[idc]=[GNA0005].[dbo].[Order].[idc] as [o].[idc]) ORDERED FORWARD)
                 |    |    |--RID Lookup(OBJECT:([GNA0005].[dbo].[Customer] AS [c]), SEEK:([Bmk1004]=[Bmk1004]),  WHERE:([GNA0005].[dbo].[Customer].[residence] as [c].[residence]='Berlin') LOOKUP ORDERED FORWARD)
                 |    |--Index Seek(OBJECT:([GNA0005].[dbo].[OrderItem].[pk_orderitem] AS [oi]), SEEK:([oi].[ido]=[GNA0005].[dbo].[Order].[ido] as [o].[ido]) ORDERED FORWARD)
                 |--RID Lookup(OBJECT:([GNA0005].[dbo].[OrderItem] AS [oi]), SEEK:([Bmk1000]=[Bmk1000]),  WHERE:([GNA0005].[dbo].[OrderItem].[unit_price] as [oi].[unit_price]>=(100000) AND [GNA0005].[dbo].[OrderItem].[unit_price] as [oi].[unit_price]<=(200000)) LOOKUP ORDERED FORWARD)

--  Comments:
-- Baseline QEP includes, Table scan (Order table), Index seek & RID lookup on Customer table, Index seek and RID lookup on OrderItem table. This is inefficient as it includes table scan and RID lookup operations. This can be improved by creating appropiate indexes.


-- =========================================
-- Iteration 1
-- =========================================


create index idx_ct_residence_idc on Customer(residence, idc);

-- Comments:
-- Currently Optimizer performs index seek and RID lookup on customer table to find residence='Berlin', creating a composite index Customer(residence, idc) eliminates the RID Lookup for customer table.
-- the second parameter 'idc' helps in join operation of Order table (o.idc = c.idc) 

-- CPU: 46 ms, IO Cost: 54 + 78 + 2179 = ~2350

-- QEP
  |--Compute Scalar(DEFINE:([Expr1006]=CONVERT_IMPLICIT(int,[Expr1017],0)))
       |--Stream Aggregate(DEFINE:([Expr1017]=Count(*)))
            |--Nested Loops(Inner Join, OUTER REFERENCES:([Bmk1000], [Expr1016]) WITH UNORDERED PREFETCH)
                 |--Nested Loops(Inner Join, OUTER REFERENCES:([o].[ido]))
                 |    |--Hash Match(Inner Join, HASH:([o].[idc])=([c].[idc])DEFINE:([Opt_Bitmap1008]))
                 |    |    |--Table Scan(OBJECT:([GNA0005].[dbo].[Order] AS [o]),  WHERE:([GNA0005].[dbo].[Order].[order_datetime] as [o].[order_datetime]='2025-05-01'))
                 |    |    |--Index Seek(OBJECT:([GNA0005].[dbo].[Customer].[idx_ct_residence_idc] AS [c]), SEEK:([c].[residence]='Berlin')  WHERE:(PROBE([Opt_Bitmap1008],[GNA0005].[dbo].[Customer].[idc] as [c].[idc])) ORDERED FORWARD)
                 |    |--Index Seek(OBJECT:([GNA0005].[dbo].[OrderItem].[pk_orderitem] AS [oi]), SEEK:([oi].[ido]=[GNA0005].[dbo].[Order].[ido] as [o].[ido]) ORDERED FORWARD)
                 |--RID Lookup(OBJECT:([GNA0005].[dbo].[OrderItem] AS [oi]), SEEK:([Bmk1000]=[Bmk1000]),  WHERE:([GNA0005].[dbo].[OrderItem].[unit_price] as [oi].[unit_price]>=(100000) AND [GNA0005].[dbo].[OrderItem].[unit_price] as [oi].[unit_price]<=(200000)) LOOKUP ORDERED FORWARD)



-- =========================================
-- Iteration 2
-- =========================================

create index idx_order_date_ids on "Order"(order_datetime, idc, ido);

-- Comments:
-- Currently the query  on Order table order_datetime = '2025-05-01' is highly selective <1%, table scan on Order table is very inefficient, creating an index 
-- "Order"(order_datetime, idc, ido) eliminates table scan on Order table 
-- the second and third parameters, idc and ido help in efficient join operations (o.idc = c.idc), (o.ido = oi.ido)

-- CPU: 15 ms, IO Cost: 54 + 78 + 3 = ~150

-- QEP:
  |--Compute Scalar(DEFINE:([Expr1006]=CONVERT_IMPLICIT(int,[Expr1012],0)))
       |--Stream Aggregate(DEFINE:([Expr1012]=Count(*)))
            |--Nested Loops(Inner Join, OUTER REFERENCES:([Bmk1000], [Expr1011]) WITH UNORDERED PREFETCH)
                 |--Nested Loops(Inner Join, OUTER REFERENCES:([o].[ido]))
                 |    |--Hash Match(Inner Join, HASH:([o].[idc])=([c].[idc])DEFINE:([Opt_Bitmap1008]))
                 |    |    |--Index Seek(OBJECT:([GNA0005].[dbo].[Order].[idx_order_date_ids] AS [o]), SEEK:([o].[order_datetime]='2025-05-01') ORDERED FORWARD)
                 |    |    |--Index Seek(OBJECT:([GNA0005].[dbo].[Customer].[idx_ct_residence_idc] AS [c]), SEEK:([c].[residence]='Berlin')  WHERE:(PROBE([Opt_Bitmap1008],[GNA0005].[dbo].[Customer].[idc] as [c].[idc])) ORDERED FORWARD)
                 |    |--Index Seek(OBJECT:([GNA0005].[dbo].[OrderItem].[pk_orderitem] AS [oi]), SEEK:([oi].[ido]=[GNA0005].[dbo].[Order].[ido] as [o].[ido]) ORDERED FORWARD)
                 |--RID Lookup(OBJECT:([GNA0005].[dbo].[OrderItem] AS [oi]), SEEK:([Bmk1000]=[Bmk1000]),  WHERE:([GNA0005].[dbo].[OrderItem].[unit_price] as [oi].[unit_price]>=(100000) AND [GNA0005].[dbo].[OrderItem].[unit_price] as [oi].[unit_price]<=(200000)) LOOKUP ORDERED FORWARD)



-- =========================================
-- Iteration 3
-- =========================================

create index idx_orderItem_up_ido on OrderItem(ido,unit_price);

-- Comments:
-- Currently dbms is performing index seek and RID lookup on OrderItem table for unit_price, creating a composite index OrderItem(ido,unit_price) 
-- eliminates RID lookup
-- ido helps in effecient join opertaion with Order table (oi.ido = o.ido)
-- unit_price helps in matching unit price in index without table access

-- CPU: 5 ms, IO Cost: 54 + 18 + 3 = ~70

-- QEP:
  |--Compute Scalar(DEFINE:([Expr1006]=CONVERT_IMPLICIT(int,[Expr1011],0)))
       |--Stream Aggregate(DEFINE:([Expr1011]=Count(*)))
            |--Nested Loops(Inner Join, OUTER REFERENCES:([o].[ido]))
                 |--Hash Match(Inner Join, HASH:([o].[idc])=([c].[idc])DEFINE:([Opt_Bitmap1008]))
                 |    |--Index Seek(OBJECT:([GNA0005].[dbo].[Order].[idx_order_date_ids] AS [o]), SEEK:([o].[order_datetime]='2025-05-01') ORDERED FORWARD)
                 |    |--Index Seek(OBJECT:([GNA0005].[dbo].[Customer].[idx_ct_residence_idc] AS [c]), SEEK:([c].[residence]='Berlin')  WHERE:(PROBE([Opt_Bitmap1008],[GNA0005].[dbo].[Customer].[idc] as [c].[idc])) ORDERED FORWARD)
                 |--Index Seek(OBJECT:([GNA0005].[dbo].[OrderItem].[idx_orderItem_up_ido] AS [oi]), SEEK:([oi].[ido]=[GNA0005].[dbo].[Order].[ido] as [o].[ido] AND [oi].[unit_price] >= (100000) AND [oi].[unit_price] <= (200000)) ORDERED FORWARD)


--=======
-- Comparison
--=======
-------------------
-- Before
-------------------
-- table scan (Order) (datetime)
-- RID look up (Customer) (Residence)
-- RID lookup (OrderItem) (unit_price)

-- CPU time: 63 ms
-- IO Cost: ~2800

----------------------
-- After Optimization
-------------------
-- Index Seek (Order) (datetime)
-- Index Seek (Customer) (Residence)
-- Index Seek (OrderItem) (unit_price)

-- CPU time: 5 ms
-- IO Cost: ~70


-- =========================================
-- Cleanup
-- =========================================

drop index idx_ct_residence_idc on Customer;

drop index idx_order_date_ids on "Order";

drop index idx_orderItem_up_ido on OrderItem;