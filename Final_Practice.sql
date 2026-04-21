---Practice queries 


select * from Customer;
select * from Staff;
select * from Product;
select * from Store;
select * from "Order";

select lname, fname
from Staff;

select name, unit_price
from Product;


--Customers living in Prague.
--Staff with gender = 'F'.
--Products with unit_price > 10000.
--Orders with order_status = 'completed'.
--Customers born before 1995-01-01.
--Staff with end_contract IS NULL.
--Products where description IS NULL.


select fname, lname, residence 
from Customer
where residence='Praha';

select lname, fname, gender
from Staff 
where gender = 'f';

select p.name, p.unit_price
from Product p
where unit_price > 10000;

select o.ido, o.order_datetime
from "Order" o
where o.order_datetime >= '1984-01-01';

select s.fname,lname
from Staff s
where s.end_contract is null

select ido, order_status
from "Order"
where order_status is null

-- ORDER BY
-- Customers sorted by lname.
--Products sorted by unit_price DESC.
--Staff sorted by start_contract.
--Orders sorted by order_datetime DESC.
--DISTINCT
--Unique customer residences.
--Unique product producers.
--Unique order statuses.

	select fname, lname
	from Customer
	order by fname, lname asc;

select name, unit_price
from Product
order by unit_price asc;

select fname, lname, start_contract
from Staff
order by start_contract

select idc, ido, order_datetime
from "Order"
order by order_datetime desc

---distinct 

select distinct fname, lname, residence
from Customer

select distinct name, producer
from Product

select count(*) as cnt
from (
    select distinct ido, order_status
    from "Order"
) t;	


--Count all customers.
--Count all orders.
--Sum of quantity in OrderItem.
--Average product price.
--Maximum product price.
--Minimum product price.


select count(*) as total 
from Customer;
select count(*) as total 
from "Order";

select * from OrderItem

select sum(oi.quantity) as total_quantity
from OrderItem oi

select * from Product;

select avg(cast(p.unit_price as bigint))
from product p

select max(p.unit_price) as maxi
from product p

select min(p.unit_price) as mini
from product p


--INNER JOIN (2 Tables)
--Orders with customer names.
--Orders with staff names.
--Staff with store names.
--Order items with product names.
--Orders with store location.

--🔹 INNER JOIN (3+ Tables)
--Orders + Customer + Staff.
--Orders + Store + Staff.
--OrderItem + Product + Order.
--Order + Customer + Store.

--🔹 JOIN + WHERE
--Orders for customers in Prague.
--Orders handled by staff in Brno.
--Order items for products with price > 10000.
--Orders from stores in Ostrava.
--Orders in 2025 with customer names.

select o.ido, c.fname, c.lname
from "Order" o
join Customer c on o.idc = c.idc

select * from Staff

select s.fname,s.lname
from "Order" o
join Staff s on s.idsa = o.idsa

select ido, s.name
from "Order" o
join Store s on s.idso=o.idso;

select p.name
from OrderItem oi
join Product p on p.idp = oi.idp

select o.ido, s.residence
from "Order" o
join Store s on s.idso = o.idso


-- join + where

select o.ido, c.idc, c.fname, c.lname, c.residence
from "Order" o
join Customer c on o.idc = c.idc
where c.residence='Praha'

select s.fname, s.lname, s.residence
from "Order" o 
join Staff s on s.idsa = o.idsa
where s.residence='Brno'

select p.name 
from OrderItem oi
join Product p on p.idp = oi.idp
where oi.unit_price > 10000

select o.ido, s.residence
from "Order" o
join Store s on s.idso = o.idso
where s.residence = 'Ostrava'

select o.ido, c.lname, c.fname, o.order_datetime
from "Order" o 
join Customer c on c.idc = o.idc
where o.order_datetime >= '2025-01-01' and 
o.order_datetime < '2026-01-01'

--1. Query specification: List order items (OrderItem) and orders (Order) with an order date in February 2025 for employees (Staff) based in Brno and customers (Customer) based in Prague. 
--The result should include only the number of records and the sum of OrderItem.quantity.

select count (*) as cnt, sum(oi.quantity) as total
from "Order" o 
join OrderItem oi on oi.ido = o.ido
join Staff s on s.idsa = o.idsa
join Customer c on c.idc = o.idc
where o.order_datetime >= '2025-02-01' and 
s.residence = 'Brno' and c.residence = 'Praha'

--2. Query specification: List orders (Order) and stores (Store) for orders created in March 2025 in stores located in Ostrava. 
--The result should include only the number of orders.

select count (o.ido) as no_of_orders
from "Order" o
join Store s on s.idso = o.idso
where o.order_datetime >= '2025-03-01' and
s.residence='Ostrava'

--3. Query specification: List order items (OrderItem), products (Product), and orders (Order) for orders created in January 2025 where the ordered product was produced by Samsung. 
--The result should include only the number of records and the sum of OrderItem.quantity.

select count(*) as no_of_records, sum(oi.quantity) as total
from OrderItem oi
join Product p on p.idp = oi.idp
join "Order" o on o.ido = oi.ido
where o.order_datetime >='2025-01-01' and
o.order_datetime < '2025-02-01' and
p.producer = 'Samsung';

--4. Query specification: List order items (OrderItem) and products (Product) for products with unit_price greater than 10000 included in orders (Order) placed in April 2025. 
--The result should include only the number of records and the sum of OrderItem.quantity.

select count (*) as cnt, sum(oi.quantity) as quant
from OrderItem oi
join Product p on p.idp = oi.idp
join "Order" o on o.ido = oi.ido
where p.unit_price > 10000 and 
o.order_datetime >='2025-04-01' and 
o.order_datetime < '2025-05-01';

--5.Query specification: List orders (Order), employees (Staff), and stores (Store) where employees work in stores located in Brno, and the orders were created in January 2026. 
--The result should include only the number of orders.

select count(*) as no_of_records
from "Order" o 
join Staff s on s.idsa = o.idsa
join Store so on so.idso = o.idso
where so.residence = 'Brno' and 
o.order_datetime >='2026-01-01' and
o.order_datetime <'2026-02-01';


select * from "Order"
select * from OrderItem
select * from Store

--List the products of an order (tables Product and OrderItem) and the orders (Order) where the order item quantity is greater than 3 and the product price is greater than 5,000 (attributes OrderItem.quantity and Product.unit_price).
--The result will include only the number of records and the number of unique customers (attribute Order.idc).


select count(*) as cnt, count(distinct o.idc) as no_of_cust
from OrderItem oi
join "Order" o on o.ido = oi.ido
join Product p on p.idp = oi.idp
where oi.quantity > 3 and p.unit_price > 5000

--Query specification:
--List the order items (OrderItem) grouped by product (attribute OrderItem.idp).
--The result will include the product identifier and the total quantity ordered for each product.

select oi.idp, sum(oi.quantity) as total
from OrderItem oi
group by oi.idp

--Query specification:
--List the orders (Order) and their order items (OrderItem) grouped by customer (attribute Order.idc).
--The result will include the customer identifier and the total sales amount, where sales = unit_price * quantity.

select o.idc as cust,  sum(oi.unit_price * oi.quantity) as total_sales
from OrderItem oi 
join "Order" o on o.ido = oi.ido
group by o.idc

select o.idc as cst, sum(oi.unit_price * oi.quantity) as total_sales
from OrderItem oi
join "Order" o on o.ido = oi.ido
group by o.idc
having sum(oi.unit_price * oi.quantity) > '40000'

select oi.idp as id, count (*) as no_of_items
from OrderItem oi
group by oi.idp
having count (*) > 4

select count(*) as cnt, count(distinct o.idc) as cust
from OrderItem oi
join "Order" o on o.ido = oi.ido
join Product p on p.idp = oi.idp
where oi.quantity > 4 and p.unit_price < 9000;

select o.idc as cust_id, sum(oi.unit_price * oi.quantity)as total_sales
from OrderItem oi
join "Order" o on o.ido = oi.ido
group by o.idc 

select top 3 o.idc as cust_id, sum(oi.unit_price * oi.quantity)as total_sales
from OrderItem oi
join "Order" o on o.ido = oi.ido
join Product p on p.idp = oi.idp
where p.unit_price > 10000 and oi.quantity > 5
group by o.idc
having sum(oi.unit_price * oi.quantity) > 50000
order by total_sales desc
