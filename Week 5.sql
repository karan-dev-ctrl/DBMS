
SELECT lname, fname, residence, COUNT(*) 
FROM Customer
GROUP BY lname, fname, residence
ORDER BY COUNT(*)

--21,89


SELECT lname, fname, COUNT(*) 
FROM Customer
GROUP BY lname, fname
ORDER BY COUNT(*)

--747, 1254



SELECT lname, residence, COUNT(*) 
FROM Customer
GROUP BY lname, residence
ORDER BY COUNT(*)




set statistics time on;
set statistics time off;

--SQL Server Execution Times:
--CPU time = 78 ms,  elapsed time = 73 ms.

set statistics io on;
set statistics io off;

--logical reads 1605

set showplan_text on;
set showplan_text off;

--  |--Table Scan(OBJECT:([PAL0343].[dbo].[Customer]), WHERE:([PAL0343].[dbo].[Customer].[lname]='Svoboda' AND [PAL0343].[dbo].[Customer].[fname]='Pavel' AND [PAL0343].[dbo].[Customer].[residence]='Barcelona'))

SELECT * FROM Customer
WHERE lname='Svoboda' and fname='Pavel' and  residence='Barcelona';



option (maxdop 1);


CREATE INDEX idx_ln_fn_rs
ON Customer (lname, fname, residence);


