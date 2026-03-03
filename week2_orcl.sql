select index_name from user_indexes 
where table_name='Order';

----------------------
--Customer - SYS_C0020206
--Order - SYS_C0020227
--OrderItem - PK_ORDERITEM
--Product - SYS_C0020210
--Staff - SYS_C0020221
--Store - SYS_C0020213


select blocks, bytes/1024/1024 as MB from user_segments
where segment_name = 'CUSTOMER';

COLUMN table_name FORMAT A20;

select table_name,blocks, empty_blocks,pct_free,pct_used from user_tables 
where table_name='CUSTOMER'; 

exec PrintPages_unused_space('CUSTOMER', 'PAL0343', 'TABLE');
exec PrintPages_space_usage('CUSTOMER', 'PAL0343', 'TABLE');

-----------------

select blocks, bytes/1024/1024 as MB from user_segments
where segment_name = 'CUSTOMER';
 
exec PrintPages_unused_space('SYS_C0020206', 'PAL0343', 'INDEX');
exec PrintPages_space_usage('SYS_C0020206', 'PAL0343', 'INDEX');

-----------------

col index_name for a15

select index_name, blevel, leaf_blocks
from user_indexes where table_name='CUSTOMER';

-----------------

ANALYZE INDEX SYS_C0020206 VALIDATE STRUCTURE;

select height-1 as h, blocks, lf_blks as leaf_pages, 
br_blks as inner_pages, lf_rows as leaf_items,
br_rows as inner_items, pct_used
from index_stats where name='SYS_C0020206'

--------------------------------------------------

ANALYZE INDEX PK_ORDERITEM VALIDATE STRUCTURE;

select height-1 as h, blocks, lf_blks as leaf_pages, 
br_blks as inner_pages, lf_rows as leaf_items,
br_rows as inner_items, pct_used
from index_stats where name='PK_ORDERITEM'


--Task 2.3 :
--Customer : 
--Height - 2 
--leaf pages - 1021
--internal pages - 3 
-- pct-used - 55

--IOcost
--point query - h + 1 = 2+1 = 3
--Range query - h+ leafpages - h + 1021 - 1023


--OrderItem : 
--Height - 2 
--leaf pages - 23686
--internal pages - 69
--PCT -used - 53

--point query - h + 1 = 2+1 = 3
--Range query - h+ leafpages - h + 23686 - 23688



