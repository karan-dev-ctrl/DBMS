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


select distinct * 
from "Order"
where Order_datetime between '2000-01-01' and '2023-01-01'
order by order_datetime asc;

--List all orders between 2000 and 2023

select * from "Order"
where order_datetime between '2000' and '2023'
order by order_datetime asc;

--List all products with price greater than 10,000.

select * from Product
where unit_price > 10000;

--List all order items where quantity > 5

select * from OrderItem
where quantity > 5;

--Show product name and quantity from orders.

select p.name, oi.quantity
from Product p
join OrderItem oi on p.idp = oi.idp;

--List customer id and product id for all orders.
select o.idc, oi.idp
from "Order" o
join OrderItem oi on o.ido = oi.ido

--List product name and order item price.

select p.name, oi.unit_price
from "Product" p
join OrderItem oi on p.idp = oi.idp;


--- Count how many order items exist
select count(*) as cnt
from OrderItem


--Find total quantity ordered for each product

select oi.idp, sum(oi.quantity) as total_quantity
from "OrderItem" oi
group by oi.idp

--Find maximum order item price per product.
select oi.idp, max(oi.unit_price) as max
from "OrderItem" oi
group by oi.idp

--Find average order item price per product.
select oi.idp, avg(oi.unit_price) as avg
from OrderItem oi
group by oi.idp;

--total sales per customer

select o.idc, sum(oi.unit_price * oi.quantity) as total_sales
from "Order" o
join OrderItem oi on o.ido = oi.ido
group by o.idc;

---products ordered more than 3 times



--Find customers whose total sales > 50,000

select o.idc, sum(oi.unit_price  * oi.quantity) as total_Sales_cust
from "Order" o
join OrderItem oi on o.ido = oi.ido
group by o.idc
having sum(oi.unit_price  * oi.quantity) > 50000

--Show first 5 products

select top 3 * 
from Product 
order by unit_price desc

---Top 5 most expensive products
select top 5 * 
from Product 
order by unit_price asc

----Top 5 order items with highest quantity
select top 5 *
from OrderItem oi
order by oi.quantity asc

---Top 5 products by total quantity ordered
--| Phrase            | Use    |
--| ----------------- | ------ |
--| Top N             | `DESC` |
--| Lowest / Cheapest | `ASC`  |

select top 5 oi.idp, sum(oi.quantity) as total_quantity
from OrderItem oi 
group by oi.idp
order by total_quantity desc

--Top 3 customers by total sales

select top 3 o.idc, sum(oi.unit_price * oi.quantity) as total_sales
from "Order" o 
join OrderItem oi on o.ido = oi.ido
group by o.idc 
order by total_sales desc

---Top 3 customers whose total sales > 50,000

select top 3 o.idc, sum(oi.unit_price * oi.quantity) as total_sales
from "Order" o 
join OrderItem oi on o.ido = oi.ido
group by o.idc
having sum(oi.unit_price * oi.quantity) > 50000
order by total_sales desc


---query 1

select count(*) as record,count(distinct o.idc ) as cnt
from "Order" o
join OrderItem oi on o.ido = oi.ido
join Product p on p.idp = oi.idp
where oi.quantity  > 5 and
p.unit_price > 10000


--query 2
select distinct o.idc as cust
from "Order" o 
join OrderItem oi on o.ido = oi.ido
join Product p on p.idp = oi.idp
where p.unit_price between 8000 and 9000;

---query 3
select oi.idp, sum(oi.unit_price * oi.quantity) as total_quantity
from OrderItem oi
group by oi.idp

---query 4
select oi.idp, max(oi.unit_price) as max
from OrderItem oi
group by oi.idp

--query 5 
select oi.idp, avg(oi.unit_price) as max
from OrderItem oi
group by oi.idp



--query 20,

select o.idc as cust, sum(oi.unit_price * oi.quantity) as total_sales 
from "Order" o 
join OrderItem oi on o.ido = oi.ido
join Product p on p.idp = oi.idp
where p.unit_price > 10000 and 
oi.quantity > 5 
group by o.idc 
having sum(oi.unit_price * oi.quantity) > 50000;

--query 12 

select top 3 o.idc as top_3_cust,
       sum(oi.unit_price * oi.quantity) as total_sales
from "Order" o
join OrderItem oi on o.ido = oi.ido
join Product p on p.idp = oi.idp
group by o.idc
order by total_sales desc;

---query 8

select oi.idp as pid, count (*)  as no_of_order_items
from OrderItem oi
group by oi.idp
having count (*)  > 3

--query 6 

select o.idc as cust, sum(oi.unit_price * oi.quantity) as total_sales
from "Order" o 
join OrderItem oi on o.ido = oi.ido
group by o.idc

---Top 5 products by total sales (slightly tricky)

select top 5 p.name, sum(oi.unit_price * oi.quantity) as total_sales
from Product p 
join OrderItem oi on p.idp = oi.idp
group by p.name
order by total_sales desc
