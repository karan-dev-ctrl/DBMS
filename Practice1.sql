---List the products of an order (tables Product and OrderItem) and the orders (Order) 
---where both the item price and the product price are between 10,000 and 10,050 (attribute unit price in tables OrderItem and Product). 
---The result will include only the number of records and the number of unique customers (attribute Order.idc).


select  count (*) cnt, count (distinct o.idc) cust
from "Order" o
join OrderItem oi on o.ido = oi.ido
join Product p on oi.idp = p.idp
where p.unit_price between 10000 and 10050 and
      oi.unit_price between 10000 and 10050; 


select * from Product
select * from OrderItem;
select * from "Order";

select  count(*) cnt, count(distinct o.idc) cust
from "Order" o
join OrderItem oi on o.ido = oi.ido
join Product p on oi.idp = p.idp
where p.unit_price between 10000 and 10050 and
      oi.unit_price between 10000 and 10050;


select * from Product p
where p.unit_price between 5000 and 10000;


select * from Product p
where p.producer = 'Samsung';

select count (*) from OrderItem oi
join Product p on oi.idp = p.idp
where oi.unit_price > p.unit_price;

select * from OrderItem oi
join Product p on oi.idp = p.idp
where p.unit_price between 2000 and 3000 and
oi.unit_price between 2000 and 3000;


select p.name,oi.unit_price
from Product p
join OrderItem oi on p.idp = oi.idp
where oi.quantity > 5;

select count (distinct o.ido)nof, count(distinct o.idc)cst
from "Order" o
join OrderItem oi on oi.ido = o.ido
where oi.unit_price > 10000

select * from "Order";

select  distinct idc
from "Order"  o
join OrderItem oi on o.ido = oi.ido
join Product p on p.idp = oi.idp
where p.unit_price between 8000 and 9000;

select count (*) from Product p 
join OrderItem oi on oi.idp = p.idp
where p.unit_price <> oi.unit_price;

select count(distinct o.idc) 
from "Order" o
join OrderItem oi on o.ido = oi.ido
where oi.quantity >= 10 


select count (*) as cnt, count(distinct o.idc)cust
from "Order" o
join OrderItem oi on o.ido = oi.ido
join Product p on oi.idp = p.idp
where p.unit_price between 10000 and 10050 and
oi.unit_price between 10000 and 10050;

select oi.idp, count(*) as cnt
from OrderItem oi
group by oi.idp;


select * from OrderItem


---customers who made more than 2 orders 

select * from Product 

select * from OrderItem; 

select oi.idp
from OrderItem oi
group by idp 
having count(*) > 3;

select *
from OrderItem oi
where oi.idp = 54624 and 
oi.quantity > 3


select oi.idp, count(*) as items
from OrderItem oi
group by oi.idp


select count (*)cnt, count(distinct  o.idc) as cust
from "Order" o 
join OrderItem oi on o.ido = oi.ido
join Product p on p.idp = oi.idp
where p.unit_price between 10000 and 10050 and
oi.unit_price between 10000 and 10050

--Count how many order items exist for each product

select  oi.idp
from OrderItem oi
group by oi.idp

select * from Product
select * from OrderItem

select oi.idp, sum(oi.quantity) as cnt
from OrderItem oi
group by oi.idp

select oi.idp, avg(oi.unit_price) as Average
from OrderItem oi
group by oi.idp

 
select oi.idp, min(oi.unit_price) as min
from OrderItem oi 
group by oi.idp

select p.name, sum(oi.quantity) as total_quant
from Product p 
join OrderItem oi on oi.idp = p.idp
group by p.name


select o.idc, count(o.ido) as total
from "Order" o 
join OrderItem oi on oi.ido = o.ido
group by o.idc;

