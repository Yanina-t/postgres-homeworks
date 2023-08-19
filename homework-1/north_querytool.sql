CREATE TABLE customers
(
    customer_id varchar(5) PRIMARY KEY,
    company_name varchar(100) NOT NULL,
	contact_name varchar(50) NOT NULL
);

CREATE TABLE employees
(
    employee_id int PRIMARY KEY,
    first_name varchar(20) NOT NULL,
	last_name varchar(20) NOT NULL,
	title varchar(50) NOT NULL,
	birth_date date NOT NULL,
	notes text NOT NULL
);

CREATE TABLE orders
(
    order_id int PRIMARY KEY,
    customer_id varchar(5) REFERENCES customers(customer_id) NOT NULL,
	employee_id int REFERENCES employees(employee_id) NOT NULL,
	order_date date NOT NULL,
	ship_city varchar(20) NOT NULL
);

select * from customers
delete from customers
select * from employees
delete from employees
select * from orders
delete from orders