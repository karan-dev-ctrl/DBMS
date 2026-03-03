exec PrintPagesHeap 'Customer';

------------------------------

create or alter procedure PrintIndexes 
  @table VARCHAR(30)
as
  select i.name as indexName
  from sys.indexes i
  inner join sys.tables t on t.object_id = i.object_id
  where T.Name = @table and i.name is not null;
go

---Index StoreProcedure

exec PrintIndexes 'Customer';
exec PrintIndexes 'Product';
exec PrintIndexes 'Store';
exec PrintIndexes 'Staff';
exec PrintIndexes 'Order';
exec PrintIndexes 'OrderItem';

---Drop Primary key constraint
ALTER TABLE OrderItem
DROP CONSTRAINT pk_orderitem

---Adding Primary key constraint
ALTER TABLE OrderItem
ADD CONSTRAINT pk_orderitem PRIMARY KEY (ido, idp)


------------------------------

create or alter procedure PrintPagesIndex 
  @index varchar(30)
as
  select 
    i.name as IndexName,
    p.rows as ItemCounts,
    sum(a.total_pages) as TotalPages, 
    round(cast(sum(a.total_pages) * 8 as float) / 1024, 1) 
      as TotalPages_MB, 
    sum(a.used_pages) as UsedPages,
    round(cast(sum(a.used_pages) * 8 as float) / 1024, 1) 
      as UsedPages_MB
  from sys.indexes i
  inner join sys.partitions p 
    on i.object_id = p.OBJECT_ID and i.index_id = p.index_id
  inner join sys.allocation_units a 
    on p.partition_id = a.container_id
  where i.name = @index
  group by i.name, p.Rows
  order by i.name
go

exec PrintPagesHeap 'OrderItem';

exec PrintPagesIndex 'pk_orderitem';


---Customer : PK__Customer__DC501A0CCAA5EAB5
---Product : PK__Product__DC501A01E99E6613
---Store : PK__Store__9DBB2CF2D0BF8430
---Staff : PK__Staff__9DBB2CFCF434FACE
---Order : PK__Order__DC501A0025D816D7
---OrderItem : pk_orderitem

------------------------

create or alter procedure PrintIndexStats @user varchar(30), @table varchar(30), @index varchar(30)
as
    select i.name, s.index_depth - 1 as height, 
      sum(s.page_count) as page_count 
    from sys.dm_db_index_physical_stats(DB_ID(@user),
      OBJECT_ID(@table), NULL, NULL , 'DETAILED') s
    join sys.indexes i 
      on s.object_id=i.object_id and s.index_id=i.index_id
    where name=@index
    group by i.name, s.index_depth
go

create or alter procedure PrintIndexLevelStats @user varchar(30), @table varchar(30), @index varchar(30)
as
    select s.index_level as level, s.page_count, 
      s.record_count, s.avg_record_size_in_bytes 
        as avg_record_size,
      round(s.avg_page_space_used_in_percent,1) 
        as page_utilization, 
      round(s.avg_fragmentation_in_percent,2) as avg_frag
    from sys.dm_db_index_physical_stats(DB_ID(@user), 
      OBJECT_ID(@table), NULL, NULL , 'DETAILED') s
    join sys.indexes i 
      on s.object_id=i.object_id and s.index_id=i.index_id
    where name=@index
go

exec PrintIndexStats 'PAL0343', 'OrderItem', 'pk_orderitem'
exec PrintIndexStats 'PAL0343', 'Customer', 'PK__Customer__DC501A0CCAA5EAB5'
exec PrintIndexStats 'PAL0343', 'Product', 'PK__Product__DC501A01E99E6613'
exec PrintIndexStats 'PAL0343', 'Store', 'PK__Store__9DBB2CF2D0BF8430'
exec PrintIndexStats 'PAL0343', 'Staff', 'PK__Staff__9DBB2CFCF434FACE'
exec PrintIndexStats 'PAL0343', 'Order', 'PK__Order__DC501A0025D816D7'
exec PrintIndexStats 'PAL0343', 'OrderItem', 'pk_orderitem'


exec PrintIndexLevelStats 'PAL0343', 'Customer', 'PK__Customer__DC501A0CCAA5EAB5'
exec PrintIndexLevelStats 'PAL0343', 'Product', 'PK__Product__DC501A01E99E6613'
exec PrintIndexLevelStats 'PAL0343', 'Store', 'PK__Store__9DBB2CF2D0BF8430'
exec PrintIndexLevelStats 'PAL0343', 'Staff', 'PK__Staff__9DBB2CFCF434FACE'
exec PrintIndexLevelStats 'PAL0343', 'Order', 'PK__Order__DC501A0025D816D7'
exec PrintIndexLevelStats 'PAL0343', 'OrderItem', 'pk_orderitem'


---B-tree stats
---height - 2
---number of leaf pages(level 0) - 17925
---number of internal pages(level 1) - 41
---Root pages (level 2) - 1

---Page Utilization
---Leaf pages - 99.9
---internal pages - 91.8

---The number of pages (IO cost) of the point query: h + 1.
---2 + 1 - 3 I/o Operations

---IO Cost of Range query = h + b (no of leaf nodes)
--- 2 + 17925
--- 17927 I/o Operations

---Store table had an lowest page index