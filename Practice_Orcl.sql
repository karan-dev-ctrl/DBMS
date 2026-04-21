
--Order(ido) → OrderItem(o.ido=oi.ido) → Product(p.idp=oi.idp)

select count(*) as cnt, count(distinct o.idc) as cst
from "Order" o
join OrderItem oi on o.ido = oi.ido
join Product p on p.idp = oi.idp
where p.unit_price between 10000 and 10050 and
oi.unit_price between 10000 and 10050;


select * from "Order"

select *
from "Order"
where order_datetime between 
      to_date('1997-01-01','YYYY-MM-DD') 
  and to_date('2023-01-01','YYYY-MM-DD');

select * from Product
where unit_price < 5000;


select * from OrderItem
where quantity > 3;

select distinct p.name as name, oi.quantity
from OrderItem oi
join Product p on p.idp = oi.idp


select distinct p.name, sum(oi.quantity)
from OrderItem oi 
join Product p on p.idp = oi.idp
group by p.name

select * from Product;
select * from OrderItem

--Total quantity per product
--Maximum price per product
select oi.idp, max(oi.unit_price) as total_quantity
from OrderItem oi 
group by oi.idp;


--All non-aggregated columns must be in GROUP BY

select p.name, oi.idp, count(*)
from OrderItem oi
join Product p on p.idp = oi.idp
group by oi.idp, p.name

select oi.idp, sum(oi.unit_price * oi.quantity) as total_sales
from OrderItem oi
group by oi.idp

select * from "Order"

select o.idc, sum(oi.unit_price * oi.quantity) as Cust_sales
from "Order" o
join OrderItem oi on o.ido = oi.ido
group by o.idc

---

select o.idc, sum(oi.unit_price  * oi.quantity) as total_Sales_cust
from "Order" o
join OrderItem oi on o.ido = oi.ido
group by o.idc
having sum(oi.unit_price  * oi.quantity) > 50000

select *
from Product
where rownum <= 5;

select *
from Product
order by unit_price desc
fetch first 5 rows only;

select o.idc, sum(oi.unit_price * oi.quantity) as total_sales
from "Order" o
join OrderItem oi on o.ido = oi.ido
group by o.idc
order by total_sales desc
fetch first 3 rows only;
