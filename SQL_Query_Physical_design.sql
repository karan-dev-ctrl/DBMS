
-- Cleanup from previous execution
DROP INDEX IF EXISTS IX_Order_order_datetime ON "Order";
DROP INDEX IF EXISTS IX_Staff_residence_Praha ON Staff;
DROP INDEX IF EXISTS IX_Customer_residence_Praha ON Customer;
DROP INDEX IF EXISTS IX_Order_composite ON "Order";
DROP INDEX IF EXISTS IX_OrderItem_ido ON OrderItem;


SET STATISTICS IO ON;
SET STATISTICS IO OFF;
SET STATISTICS TIME ON;
SET STATISTICS TIME OFF;
SET SHOWPLAN_TEXT ON;
SET SHOWPLAN_TEXT OFF;


select count(*)as cnt, sum(oi.quantity)as total
from "Order" o 
join Staff s on s.idsa = o.idsa
join orderItem oi on oi.ido = o.ido
join customer c on c.idc = o.idc
where o.order_datetime >= '2025-01-01'and o.order_datetime <'2025-02-01' and s.residence = 'Praha' and c.residence = 'Praha'
option (maxdop 1);

--Baseline result = cnt - 82 & sum - 411



--Iteration 0 : -- Iteration 0: baseline without additional indexes
-- Purpose: obtain the original execution plan, IO statistics, and CPU time.
----- Parallelism is disabled using OPTION (MAXDOP 1) to ensure consistent.

-- Iteration 0: baseline without additional indexes
-- This iteration is used to identify the expensive operations of the query.
-- The baseline plan shows which selections, joins, and aggregations are not
-- supported by suitable indexes, and therefore which indexes should be created
-- in the following iterations.

---Logical Reads:
--Table 'OrderItem'. Scan count 9, logical reads 17922
--Table 'Order'. Scan count 9, logical reads 4358
--Table 'Staff'. Scan count 9, logical reads 143
--Table 'Customer'. Scan count 0, logical reads 491

--Iteration 0: 17922 + 4358 + 143 + 491 = 22914

---CPU : 
--- SQL Server Execution Times:
---   CPU time = 717 ms,  elapsed time = 92 ms.

---only cpu time 
-- CPU time = 469 ms


---Observed QEP:

-- The baseline plan contains table scans on Staff, Order, and OrderItem.
-- Customer is accessed using an index seek followed by a RID lookup.
-- The join between Staff and Order is implemented as a hash join.
-- The join between Order and OrderItem is also implemented as a hash join.
-- Nested loops are used for Customer access.
-- This plan is not ideal for CPU minimization because it contains sequential scans,
-- hash joins, and a RID lookup.


-- Iteration 0: baseline without additional indexes

-- The query performs selection on order_datetime and residence attributes,
-- joins between Order, Staff, Customer, and OrderItem, and aggregation.

-- Because no suitable indexes exist, these operations require access to
-- complete table rows. Therefore, the optimizer uses a scan-based plan.

-- Due to large input sizes, hash joins are used, which increases CPU cost.

-- Customer is accessed via index seek followed by RID lookup, indicating that
-- the index does not cover required attributes and full rows must be accessed.
-- Therefore, the plan is inefficient due to:
-- 1. full table scans,
-- 2. hash joins on large datasets,
-- 3. non-covering index access (RID lookup).

-- IO cost = 22914 logical reads
-- CPU time = 717 ms

--1. Are tables scanned?

--👉 If yes → missing index

--2. Are there lookups?

--👉 If yes → index not covering

--3. Are joins hash joins?

--👉 If yes → inputs are large (usually due to scans)

-- Indexes are created to support selection, join, and projection (including aggregation) operations, in order to eliminate full table scans, reduce the need for row lookups, and enable more efficient join strategies.
-- Indexes are designed to support selection, join, and projection operations in a query so that full table scans and row-level lookups are eliminated and more efficient access paths, such as index seeks and nested loop joins, can be used.
--support WHERE conditions → avoid scans
--support JOIN conditions → reduce join cost
--support SELECT / aggregation → avoid row lookup

--Iteration 1 : Creating index on Order(order_datetime)

CREATE INDEX IX_Order_order_datetime
ON "Order"(order_datetime);


---Logical Reads:
--Table 'OrderItem'. Scan count 1, logical reads 17922
--Table 'Order'. Scan count 1, logical reads 4358
--Table 'Staff'. Scan count 1, logical reads 143
--Table 'Customer'. Scan count 0, logical reads 468 


---CPU : 
--- SQL Server Execution Times:
---   CPU time = 469 ms,  elapsed time = 461 ms.


-- Observed QEP:
-- The execution plan did not change significantly compared to the baseline.
-- SQL Server still chose a table scan on Order instead of using the new index
-- on order_datetime. Staff and OrderItem also remained table-scanned.
-- Customer was still accessed using an index seek followed by a RID lookup.
-- The joins between Staff and Order, and between Order and OrderItem,
-- remained hash joins.

-- Comparison with baseline:
-- Although the physical plan shape remained essentially unchanged,
-- CPU time decreased from 717 ms to 469 ms.
-- Therefore the index on Order(order_datetime) provided only limited benefit
-- and was not sufficient to change the optimizer to an index-driven plan

-- Iteration 1: index on Order(order_datetime)
-- This index is created to support the selection on Order.order_datetime.
-- However, the query also needs idsa, idc, and ido for subsequent joins.
-- Therefore, the index does not cover enough operations to avoid access to
-- complete Order rows, and SQL Server still chooses a table scan on Order.
-- As a result, the plan remains scan-based and hash joins are still used.


--- Iteration 2 : Staff filtered Index

CREATE INDEX IX_Staff_residence_Praha
ON Staff(idsa)
WHERE residence = 'Praha';


---Logical Reads:
--Table 'OrderItem'. Scan count 1, logical reads 17922
--Table 'Order'. Scan count 1, logical reads 4358
--Table 'Customer'. Scan count 0, logical reads 468
--Table 'Staff'. Scan count 1, logical reads 4

---CPU:
--- SQL Server Execution Times:
---   CPU time = 469 ms,  elapsed time = 466 ms.

-- Observed QEP:
-- The filtered index on Staff was used by the optimizer.
-- The previous table scan on Staff was replaced by an index scan on
-- IX_Staff_residence_Praha.
-- However, Order and OrderItem were still accessed by table scans.
-- Customer was still accessed by index seek followed by RID lookup.
-- The join between Staff and Order remained a hash join, and the join
-- between Order and OrderItem also remained a hash join.

-- Comparison with previous iteration:
-- The logical reads for Staff decreased significantly from 143 to 4,
-- which confirms that the filtered index on Staff is effective.
-- However, CPU time remained unchanged at 469 ms, indicating that the
-- dominant cost of the query is still caused by scanning Order and OrderItem
-- and by the RID lookup on Customer.

-- Iteration 2: filtered index on Staff(idsa) WHERE residence = 'Praha'.
-- This index is created to support the selection on Staff.residence and the join
-- from Order to Staff by idsa.
-- The index is used, so access to Staff no longer requires a full table scan.
-- However, Order and OrderItem are still accessed by full scans, and Customer
-- still requires access to complete rows. Therefore, the overall plan remains
-- hash-join-based and the total improvement is limited.
                


--- Iteration 3 : Customer filtered Index

CREATE INDEX IX_Customer_residence_Praha
ON Customer(idc)
WHERE residence = 'Praha';


---Logical Reads:
--Table 'OrderItem'. Scan count 1, logical reads 17922
--Table 'Customer'. Scan count 0, logical reads 42
--Table 'Order'. Scan count 1, logical reads 4358
--Table 'Staff'. Scan count 1, logical reads 4


---CPU:
--- SQL Server Execution Times:
---   CPU time = 468 ms,  elapsed time = 470 ms.

--QEP:
-- The filtered index on Customer(idc) WHERE residence='Praha' was used by the optimizer.
-- Staff is accessed via Index Scan on IX_Staff_residence_Praha (4 reads).
-- Customer is accessed via Index Scan on IX_Customer_residence_Praha (42 reads, down from 468).
-- However, Order is still accessed via Table Scan (4358 reads) with a WHERE filter on
-- order_datetime, meaning IX_Order_order_datetime from Iteration 1 is still ignored.
-- OrderItem is still accessed via Table Scan (17922 reads) — the dominant IO cost.
-- All three joins remain Hash Joins, which are CPU-intensive.
-- CPU time is unchanged at 468ms, confirming Order and OrderItem remain the bottleneck.
-- The bitmap filters (Opt_Bitmap) are being used to reduce rows probed in OrderItem
-- and Customer, but this is not sufficient to avoid the full scans.

-- Iteration 3: filtered index on Customer(idc) WHERE residence = 'Praha'.
-- This index is created to support the selection on Customer.residence and the
-- join from Order to Customer by idc.
-- The index is used, and the previous RID lookup on Customer is eliminated,
-- so complete Customer rows no longer need to be accessed.
-- However, Order and OrderItem are still accessed by full scans.
-- Therefore, the overall plan remains hash-join-based and the main bottlenecks
-- of the query are not yet removed.

-- Iteration 4: Composite index on Order
CREATE INDEX IX_Order_composite
ON "Order"(order_datetime, idsa, idc, ido);

---Logical Reads:
--Table 'OrderItem'. Scan count 1, logical reads 17922
--Table 'Customer'. Scan count 1, logical reads 42
--Table 'Order'. Scan count 1, logical reads 13
--Table 'Staff'. Scan count 1, logical reads 4

---CPU:
--- SQL Server Execution Times:
---   CPU time = 391 ms,  elapsed time = 398 ms.


--QEP : 

-- The composite index IX_Order_composite(order_datetime, idsa, idc, ido) was used
-- by the optimizer. Order is now accessed via Index Seek on the order_datetime range
-- predicate, reducing logical reads from 4358 to 13 — a dramatic improvement.
-- CPU time decreased from 468ms to 391ms.
-- Staff remains at 4 reads (Index Scan on IX_Staff_residence_Praha).
-- Customer remains at 42 reads (Index Scan on IX_Customer_residence_Praha).
-- However, OrderItem is still accessed via Table Scan (17922 reads), which is now
-- All three joins remain Hash Joins.
-- The next step is to add an index on OrderItem(ido) to eliminate this scan

-- Iteration 4: composite index on Order(order_datetime, idsa, idc, ido).
-- order_datetime is the leading key because the query first applies the
-- January 2025 selection.
-- idsa is included to support the join from Order to Staff.
-- idc is included to support the join from Order to Customer.
-- ido is included to support the join from Order to OrderItem.
-- Therefore, this index supports the selection and all subsequent joins on
-- Order without accessing complete Order rows.
-- The index is used, and the previous full scan on Order is replaced by
-- an index seek. However, OrderItem is still scanned, so the overall plan
-- still uses hash joins.

-- The baseline QEP shows that Order is accessed by a full table scan and
-- participates in multiple joins.
-- The query first filters Order by order_datetime and then immediately joins
-- the resulting rows to Staff, Customer, and OrderItem.
-- Therefore, an index only on order_datetime is not sufficient, because the
-- optimizer would still need to access complete Order rows to obtain idsa,
-- idc, and ido for the joins.
-- A composite index is required to support both the selection and the joins
-- in a single access path.

-- Iteration 5: Index on OrderItem(ido) including quantity
CREATE INDEX IX_OrderItem_ido
ON OrderItem(ido)
INCLUDE (quantity);

---Logical Reads:
--Table 'Customer'. Scan count 1, logical reads 42,
--Table 'Order'. Scan count 1, logical reads 13
--Table 'OrderItem'. Scan count 7, logical reads 21
--Table 'Staff'. Scan count 1, logical reads 4


--CPU:
--SQL Server Execution Times:
--   CPU time = 15 ms,  elapsed time = 8 ms.

--QEP

--- Iteration 5 - QEP Analysis:
-- The covering index IX_OrderItem_ido(ido) INCLUDE(quantity) was used by the optimizer.
-- OrderItem is now accessed via Index Seek 
-- reducing logical reads from 17922 to 21 — eliminating the last remaining table scan.
-- The join between Order and OrderItem changed from Hash Join to Nested Loops,
-- the date range and Praha filters on Staff and Customer.
-- CPU time dropped from 391ms to 15ms — a 96% reduction from the 717ms baseline.
-- All four tables are now accessed via indexes with zero table scans remaining.
-- Customer: Index Scan on IX_Customer_residence_Praha (42 reads) 
-- Order: Index Seek on IX_Order_composite (13 reads) — range seek on order_datetime.
-- OrderItem: Index Seek on IX_OrderItem_ido (21 reads) — driven by Nested Loops on ido.

--- Iteration 5: 
-- covering index on OrderItem(ido) INCLUDE(quantity).
-- ido is the join attribute from Order to OrderItem.
-- quantity is included because it is needed for SUM(oi.quantity).
-- Therefore, this index supports both the join and the aggregation without
-- accessing complete OrderItem rows.
-- The index is used, and the previous table scan on OrderItem is replaced by
-- an index seek.
-- As a result, the join between Order and OrderItem changes from hash join
-- to nested loops, which is more efficient after the preceding filters have
-- reduced the number of qualifying Order rows.


-- Final conclusion:
-- The best physical design was obtained in Iteration 5 by combining:
-- 1. a filtered index on Staff(idsa) for residence = 'Praha',
-- 2. a filtered index on Customer(idc) for residence = 'Praha',
-- 3. a composite index on Order(order_datetime, idsa, idc, ido),
-- 4. a covering index on OrderItem(ido) INCLUDE(quantity).
-- This design reduced CPU time from the serial baseline to 15 ms and reduced
-- logical reads substantially, especially on Order and OrderItem.
-- The final plan uses index-driven access and nested loops for the Order–OrderItem join,
-- which best satisfies the objective of minimizing CPU time.

-- In Iteration 4, although Order is accessed using an index,
-- OrderItem is still scanned, so the join is performed using a hash join.

-- In Iteration 5, the covering index on OrderItem allows the join
-- to be evaluated using index seeks, and the optimizer switches
-- to nested loops.

-- This is more efficient because the number of qualifying Order rows
-- is small, so repeated index lookups are cheaper than scanning and
-- hashing the entire OrderItem table.  

--“Yes, because all major tables are accessed through indexes, full table scans are eliminated, and the join between Order and OrderItem changes from hash join to nested loops. 
--This shows that the plan is fully index-driven and significantly reduces both IO cost and CPU time.


--template

-- This index is created to support [selection / join / aggregation].
-- [column1] is included because ...
-- [column2] is included because ...
-- The goal is to eliminate [full scan / row lookup / expensive join input].