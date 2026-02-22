--  the is comment

/* that 
also is 
a comment */


--	Retrieve each customer's name, country, and score
/*SELECT 
first_name,
country,
score
FROM customers */



/*	Retrive customers with a country is Germany

    SELECT * 
    FROM customers
    WHERE country = 'Germany' 
    
    */

--Order BY : Sort Your data   
/* select *
from customers
order by 
    country asc,
    score desc */


/*! Grouping the Data with Group BY
select 
    country As Group_Customer_country,
    sum(score) AS totale_score
from customers
Group by country
 */

 /* Find the totale score and the totale number of customers for each country
 */

 SELECT 
    country, 
    sum(score) as total_score,
    COUNT(id)AS tota_customers
from customers
Group by  country  

/* The having CLose it's use after group by 

 SELECT 
    country, 
    sum(score) as total_score,
    COUNT(id)AS tota_customers

from customers
where score>400
Group by  country 
Having sum(score)>800

*/



/*REturn Unique list of all countries

select DISTinct
    country  as ville
from customers
*/

/* Top  for limit data
select Top 2 *

from customers
Order by score DESC
 */

 /*
 Select top 2 * 
 from orders
 order by order_date desc
 
 select top 2 *
 from customers
 order by score  asc  


 select 
 id, 
 first_name,
 '341' as refernce 
 from customers


 select *
 from customers
 WHERE country = 'germany'

 */





/* ==============================================================================
     chapter 3 : DATA DEFINITION LANGUAGE (DDL )
 =================================================================================*/



/*create Table  new_table (
id int NOT NULL,
person_name  varchar(50) NOT NULL ,
birth_date  date,
tel_numb varChar(15) NOT NULL,
constraint pk_persons primary key (id)  
)*/

Alter table new_table 
add email varchar (30) NOT NULL

/* alter table new_table 
drop column  birth_date
select *	from new_table
drop table new_table */


select  * from customers order by score desc


-- chapter 4 : DATA MANIPULATION LANGUAGE (DML Commandes)
        --->    INSERT  :   Insert a values to table
        --->    UPDATE  :   Update data from table already exciste
        --->    DELETE  :   Remove data frome table or row  
        --->    TRUNCATE:   It's empty the table 


insert into customers (id, first_name, country, score)
values 
    
    (7, 'Ahmed',  'Morroco' , 412),
    (8, 'Amine',  'Alger' , 1021),
    (9, 'Youssef',  'Alger' , 764),
    (10, 'Jawad',  'Sudan' , 517)


 use MyDatabase

 /* create table my_costumer(
 id int NOT NULL,
 first_name varChar(20) NOT NULL,
 country varchar(30) Not null,
 score int, 
 constraint pk_costumer primary key (id)  
 ) */
insert into my_costumer 
select 
id, 
first_name,
country,
score
from customers

select * from my_costumer
select * from customers

update my_costumer
    set first_name = 'Med'
    where first_name = 'Mohamed'


Delete from my_costumer
where id=9


 --- Chapter 5 : FILTRING DATA 
 /*   
        The comparison operator :			|			Logical Operators:				|		Rang Operator				|	Membership operator			|		Searche operator		|
____________________________________________|___________________________________________|___________________________________|_______________________________|_______________________________|
                                            |											|									|								|								|
                    =						|						AND					|			BETWEEN					|			IN					|								|
                <>      !=					|						OR					|									|		   NOT IN				|			 LIKE				|
                    >						|						NOT					|									|								|								|
                    >=						|											|									|								|								|
                    <						|											|									|								|								|
                    <=						|											|									|								|								|

*/

--		1.	the comparaison operator
select * 
from customers
where country  ='Morroco' -- = (egale operator)

select * 
from customers
where country !='Morroco' -- != or we can also use <> (Not egale operator)



select * 
from customers
where score > 500  --  > operator greather than

select * 
from customers
where score < 500  --  > operator less  than

--->	2.	Logic opertors
        ---->	2.1.	AND	(ALL condition must be true ) 
select *
from customers 
where score > 400 and country='GERMANY'

        ---->	2.2.	OR	(At least one  condition must be true ) 

select *
from customers 
where score > 500 or country='Morroco'

        ---->	2.3.	NOT	((Reverse) Excludes matching values  

select *
from customers 
where Not country='Morroco'

--->	3.	Range opertors:BETWEEN
            ---> BETWEEN : check if value is within a range 
                /* Exemple : Retrieve all customers whose score
                    falls in range between 100 and 500 */

select *
from customers
where score BETWEEN  100 and 500

select *
from customers
where score >= 100 and  score <= 500
                
                
--->	4.	Membership opertors:
        ---> 4.1.	IN : check if value is exists in a liste 
        
select *
from customers
where country='alger' or country='Morroco'


select *
from customers
where country in ('alger' ,'Morroco')

            
        ---> 4.2.	NOT IN : reverse of in 

select *
from customers
where country NOT IN ('alger' ,'Morroco')

--->	5.	Searche opertors:
            --> LIKE : searche for a patternin text 
            /* 

                                                        |---> 0
                                                        |
                              |----->'%' Anything   ---->|---> 1
                              |							|
            LIKE ---> Pattern |							|---> Many
                              |	
                              |----->'_' Exact 1
            */
            
select *
from customers
where country like 'G%' --it's required to stare with G and then anything alse


select *
from customers
where first_name like '%n' --it's required first name will be  end  with letter n and stare with anything else


select *
from customers
where score like '%4%' --just having a '4' in the score  


select *
from customers
where country like '__d%' --it's required to have a letter d in thired position 



--	CHAPTER 6 : COMPINING DATA
--		CHAPTER 6-1 : JOINING DATA
--			6-1-A :BASIC JOIN 
            
        /*	
            Q : why we need joind in sql ? 
            A:	whith Joinning we can :
                    1.	Recombine DATA  "Big Picture"
                    2.	Data Enrichement "Extra Info"
                    3.	Check Existance "Filtring"
            Q :	What is join types ?
            A :	In join Types we hv: 
                    1.	The MATCHING data (the data is alivable on the left table AND also in the right table 
                            in the same time )
                    2.	ALL data in one table 
                    3.	UnMatching Data : the data in one table but not exists in the other table
            Q: Basics SQL join ?
            A: In basic SQL join we hv : 
                    1. NO join: thers not compining  between the tables 
                    2. Inner Join: It'just the parte of data is existe the all table it's required to existe to all tables 
                    3.	Left Join: the data for the left table
                    4.	Right Join: the data for right table
                    5. Full : the full data for the all table we have not exciption 
            Q: Advenced SQL Join ? 
            A: In the advenced SQL joind we have : 
                    1.	Left anti Join: we select the data of table left but not exciste on table right
                    2.	Right Anti Join: we select the data in the right table but not excist on the table left 
                    3.	Full Anti Join : The data in the left table and the data on the right table but not the
                            data sharing between the left and right table 
                    4.	Cross Join:we have right to select any that we need in the two tables no united 


        -- we star with : 

        */
--			6-1-A.1 No Join : we can have two result not compining e.g : 

select *
From orders -- al rows in order  table

select *
From customers -- all rows in customers tables
 -- and not join between them 

--			6-1-A.2 Inner Join : compining with two tables and result return only the Matching rows  from both tables  E.G :

select 
    c.id,
    c.first_name,
    c.country,
    o.order_id,
    o.order_date,
    o.sales
from customers As c
inner join orders As o
on c.id = o.customer_id    -- the condition 
            /*
             /\
            /  \
           / !  \			The condition it's required 
          /  !   \ 
         /________\				We can use inner for filtring data and also to Recombine data 

         */

--			6-1-A.3 Left Join : Return all the rows frome the left table and only the matching from the right table 
--				(from the table left we have Everting (all rows) in the right table we have only the matching rows ) e.g : 

select 
    c.id,
    c.first_name,
    c.country,
    o.order_id,
    o.order_date,
    o.sales
from customers as c -- !!!! : it's required for the LEFT table 
Left Join orders as o
on c.id = o.customer_id



            /*
             /\				we use Left in :
            /  \				-->	Recombine Data
           /  ! \				-->	Data Enrichement
          /   !  \				-->	Filtring but we need WHERE operator with theme 	
         /________\				 

         */


--			6-1-A.4 Right Join : Return all the rows frome the Right table and only the matching from the Left table 
--				(from the table Right we have Everting (all rows) in the Left table we have only the matching rows ) e.g : 

/*		Get all	customers along with thier orders, 
        including orders without matching customers  */
select 
    c.id,
    c.first_name,
    c.country,
    o.order_id,
    o.order_date,
    o.sales
from customers as c -- !!!! : it's required for the left table 
right Join orders as o
on c.id = o.customer_id


/* Task One: use  the lift join to have the same result of the Right join
*/
select 
    c.id,
    c.first_name,
    c.country,
    o.order_id,
    o.order_date,
    o.sales
from orders as o -- !!!! : it's required for the left table 
left Join customers as c
on c.id = o.customer_id
where   c.id = o.customer_id

--			6-1-A.5 Full Join : Return ALL ROWS from BOTH Tables (Everthing) e.g: 
use MyDatabase
select 
c.id,
    c.first_name,
    c.country,
    o.order_id,
    o.order_date,
    o.sales
from orders as o 
full Join customers as c
on c.id = o.customer_id

            /*
             /\				we use Full  in :
            /  \				-->	Recombine Data
           / !  \			
          /  !   \				-->	Filtring but we need WHERE operator with theme 	
         /________\				 

         */

--				6-1-B : Advanced Join :

--					6-1-B.1	LEFT ANTI JOIN: Return ROWS from  LEFT That has NO MATCH in RIGHT
--							Only unmatching Rows from the Left Nothing from the right e.g:


Select * 
from customers as c		-- the Left table is Customer
Left join orders As o	-- the right table is Orders 
On c.id = o.customer_id	-- so we do the Left join order to cutomers 
Where O.customer_id is null 


    /*in this we use is null for select nothing in the orders table -THE RIGHT TABLE),
    and select only the unmatching rows from the customers (THE LEFT TABLE ) */

        /*
             /\				we use LEFT ANTI JOIN only  for  :
            /  \				
           / !  \				-->	Filtring but we need WHERE operator with theme 
          /  !   \					
         /________\				 

         */

--					6-1-B.2	Right ANTI JOIN: (Opposite of the Left Anti JOin)  --> Return ROWS from  RIGHT That has NO MATCH in LEFT
--							Only unmatching Rows from the RIGHT Nothing from the LEFT e.g:


Select * 
from customers as c		-- the Left table is Customer
RIGHT join orders As o	-- the right table is Orders 
On c.id = o.customer_id	-- so we do the Left join order to cutomers 
Where c.id is null 


/* Cause of all rows in the right table orders are matche with the left table customers 
    so i will update the customer_id to the five item 6 ==> 14 
    for having unmatching row */
    
update orders
    set customer_id= '14'
    where customer_id= '6'

    /*
             /\				we use RIGHT ANTI JOIN only  for  :
            /  \				
           / !  \				-->	Filtring but we need WHERE operator with theme 
          /  !   \					
         /________\				 

         */



--					6-1-B.3 FULL ANTI JOIN : Retourn ONLY the ROWS that DON'T MATCH in EITHER tables 
--									Only unmatching rows frome the left and the same frome the right 
 
 select * 
 from  customers as c 
 full join orders as o 
 On c.id = o.customer_id
 where c.id is null or  o.customer_id is null  -- it's required to use 'OR' operator for that 

    /*
             /\				we use FULL ANTI JOIN only  for  :
            /  \				
           / !  \				-->	Filtring but we need WHERE operator with theme 
          /  !   \					
         /________\				 

         */
         
    -- Task 2 : GET all customers along with their orders but only for customers who have placed an order (without using an INNER JOIN

    -- solution: 

    select *
    from customers as c 
    Left join orders as o
     ON c.id = o.customer_id
     where o.customer_id  is not null


--					6-1-B.4 CROSS JOIN : Combines EVERY ROW from LEFT with EVERY ROW from RIGHT 

--							All possible Combination - CARTERSIAN JOIN  
use MyDatabase
select * 
from customers
CROSS JOIN orders  -- we don't need any condition 
        


--				6-1-C   the Right Join 
/*			we have to table and if we need 
                

                    |--> Only Matching  ----> INNER JOIN 
                    |
                    |
                    |			
                    |					|---> ONE SIDE 	--> LEFT JOIN
                    |					|
                    |--> ALL ROWS ---->	|
How to choise? ---->|					|
                    |					|---> BOTH SIDES --> FULL JOIN
                    |
                    |
                    |	
                    |								|---> ONE SIDE 	--> LEFT ANTI JOIN
                    |								|
                    |--> Only UnMAtching ROWS ---->	|
                    |								|
                    								|---> BOTH SIDES --> FULL ANTI JOIN


*/

--				6-1-D  Join Multiple Tables  
/*			we will practice with this task :
                Using SalesDB, Retrieve a list of all orders, along with the related customer, product, and employee details
                
                for eache order, display :
                        -	 Order ID
                        -	Customer's name
                        -	Product Name
                        -	Sales amount
                        -	Product Price
                        -	SalesPerson's name
                        
*/
use SalesDB

select 
    o.OrderID, 
    o.Sales,
    c.FirstName,
    c.LastName, 
    p.Product As 'ProductName',
    p.price,
    o.Sales * p.Price as total,
    E.FirstName,
    e.LastName
from sales.Orders as o
LEFT JOIN sales.Customers as c
ON o.CustomerID = c.CustomerID
Left Join sales.Products as p
ON o.ProductID = p.ProductID
LEFT JOIN sales.Employees as e
ON o.SalesPersonID = e.EmployeeID

            

--			CHAPTER 6-2 : SET OPERATORS
/*	It's for to combine the ROWS (some Columns)
    syntax and rules : it's  select to table and bettwen two select we use opirator set 'UNION' e.G: */

----------------------------------------------+
select										--|	
    c.FirstName,							--|	
    c.LastName								--|	
from sales.customers as c					--|	
                                            --|	
union										--|	
                                            --|	
select e.FirstName,							--|	
e.LastName									--|	
from sales.Employees as e					--|	
                                            --|	
----------------------------------------------+
 --						6-2-A : RULES : 
 /* RULE #1 : SWL CLAUSES :
    -	SET Operator can be used almost in all clauses WHERE | JOIN | GROUPE BY |HAVING
    -	ORDER BY is allowed only ONCE at the end of query			*/

    
--------------------------------------------------|
    SELECT										--|	
        c.FirstName,							--|---> 1st SELECT statement  
        c.LastName								--|	
    FROM sales.customers AS c					--|	
--------------------------------------------------|	
    UNION										--|--->	Set Operatur		
--------------------------------------------------|	
    SELECT										--| 
        e.FirstName,							--|	
        e.LastName								--|--->	2nd SELECT statement	
    FROM sales.Employees AS e					--|	
--------------------------------------------------|	
    ORDER BY  FirstName;							--|	
/*												  |
        N.B : We use order by only once time      |
        at the end of query	can be use only at    |
                    the end	      
*/
--------------------------------------------------|
--------------------------------------------------|


 /* RULE #2 | Number of columns :
    -	The number of columns in each query !!!! MUST BE THE SAME !!!!
    */

    
 /* RULE #3 | DATA TYPES :
    -	Data types of columns in eache query must be comptabile
    */

    SELECT										--|	
        c.CustomerID As ID, -- 	Type : INT		--|  
        c.LastName	  --	Type: VarChar		--|	---> 1st SELECT statement
    FROM sales.customers AS c					--|	
--------------------------------------------------|	
    UNION										--|--->	Set Operatur		
--------------------------------------------------|	
    SELECT										--| 
        e.EmployeeID , --Type: INT				--|	
        e.LastName	  -- Type:Varchar			--|--->	2nd SELECT statement	
    FROM sales.Employees AS e					--|	
--------------------------------------------------|	

    
 /* RULE #4 | ORDER OF COLUMNS :
    -	The order of columns in each query must be the same 
    */

 /* RULE #5 | COLUMN ALIASES:
    -	The column names in the result set are determine by the column names specified in the first query.
                                                                                                            */
 /* RULE #6 | CORRECT COLUMNs:
    - Even if all rules are met and SQL show no error  /// !!!! the result may be incorrect !!! ///
    - Incorrect column  selection leads to inaccurate results.
                                                                                                            */

    
--------------------------------------------------|
    SELECT										--|	
        c.FirstName,							--|---> 1st SELECT statement  
        c.LastName								--|	
    FROM sales.customers AS c					--|	
--------------------------------------------------|	
    UNION										--|--->	Set Operatur		
--------------------------------------------------|	
    SELECT										--| 
      --  e.LastName, -- incorrect column   	--|	
        e.FirstName								--|--->	2nd SELECT statement	
    FROM sales.Employees AS e					--|	
--------------------------------------------------|	


--				6-2-B : UNION OPERATORS
--	DEF: 
        --	Return all district rows from both queries.
        --	Removes dublicate rows from the result.
/* 
    task : Combine the data from employees  and customers into one table 
*/

SELECT 
    CustomerID As ID,
    FirstName,
    LastName
from sales.Customers

UNION 


select 
    EmployeeID As ID,
    FirstName,
    LastName
from Sales.Employees 

-- Result : unio remove two dublicate rows : MARY NULL and KEVIN BROWN 


--				6-2-C : UNION ALL OPERATORS
--		Returns all rows from both queries, including duplicate 
--			Union ALL is more faster then UNION 


SELECT 
    CustomerID As ID,
    FirstName,
    LastName
from sales.Customers

UNION  ALL


select 
    EmployeeID As ID,
    FirstName,
    LastName
from Sales.Employees 

--				6-2-D : EXCEPT OPERATORS
--		Return all distinct rows from the first query that are not found in the second query.
--		It's the only one where the order of queries affects the final result

SELECT 
    CustomerID As ID,
    FirstName,
    LastName
from sales.Customers

EXCEPT

select 
    EmployeeID As ID,
    FirstName,
    LastName
from Sales.Employees 
-- result return only the rows dosn't excet in the second table



--				6-2-E : INTERSECT SET OPERATOR 

--	Return only the rows that are common in both queries 
use SalesDB
SELECT 
    CustomerID As ID,
    FirstName,
    LastName
from sales.Customers

INTERSECT


select 
    EmployeeID As ID,
    FirstName,
    LastName
from Sales.Employees 

-- Reult return only the commun rows 


--				6-2-F : UNION USE CASES. COMBINE INFORMATION : 
--			Combine similaire information before analysing the data 
/*	Task : Orders are stored in sperate  tables(Orders and ordersArchive).
            Combine all orders into one report without duplicates				*/

SELECT 
    'orders' as [Source],
    [OrderID]
    ,[ProductID]
    ,[CustomerID]
    ,[SalesPersonID]
    ,[OrderDate]
    ,[ShipDate]
    ,[OrderStatus]
    ,[ShipAddress]
    ,[BillAddress]
    ,[Quantity]
    ,[Sales]
    ,[CreationTime]
from sales.Orders as O
UNION 
select 
    'ordersArchive' as [Source],
    [OrderID]
    ,[ProductID]
    ,[CustomerID]
    ,[SalesPersonID]
    ,[OrderDate]
    ,[ShipDate]
    ,[OrderStatus]
    ,[ShipAddress]
    ,[BillAddress]
    ,[Quantity]
    ,[Sales]
    ,[CreationTime]
from sales.OrdersArchive as OAR
order by OrderID

--				6-2-G : EXCEPT USE CASES | DELTA DETECTION  : 

--		Identifiying the differernce or charges (delta) between two batches of data.

--				6-2-H : EXCEPT USE CASES | DATA Completeness check  :

--	EXCEPT OPERATUR can be used to compare tables to detect discrepancies between databases.

/*
========================================================================================================================================================================

	                                                            CHAPTER 7 : ROW-LEVEL FUNCTION

========================================================================================================================================================================
                                                                

*//*	INTRO:
-----------> Function A built in SQL code:
                -	Accepts on input value
                -	Processes it
                -	Return an output value
-----------> Nested Function is :
                used inside another function	eg:														*/
                select 
                LEN(LOWER (LEFT ('Mohamed', 4)))


/*										|--> Single-Row Functions : String, Numeric, Data & Time, Null 
                                        |
-----------> Type of SQL Function: ---->|
                                        |							 
                                        |							
                                        |--> Multi-Row Functions : Aggregate Functions (Basics) , Window Functions (Advanced) 				*/								
                                                                    
--			7-1 STRING FUNCTIONS
/*
                                        |----> Manipulation  --> CONCAT | UPPER | LOWER | TRIM | REPLACE |
                                        |
                                        |
                    String Function --->|---> Calculation	--> LEN
                                        |
                                        |
                                        |----> String Extraction --> LEFT | RIGHT |SUBSTRING																					*/


--					7-1-1 CONCAT FUNCTION : Combines multiple string into one
/*		eg: we have two row 
            First Name : 'Michale'-----|
                                       |
                                       |--> CONCAT FUNCTION --> Full Name : ' Michale Scott'
                                       |
            Last Name : 'Scott'  ------|

    Task : Concatenate firs name and coutry  into one column :																			*/
use MyDatabase
SELECT 
    first_name,
    country,
    CONCAT(first_name,'_' ,country) as Name_Country
FROM customers

--					7-1-2  UPPER & LOWER FUNCTION :
/*												Upper function : converts all charcters to uppercase
/												Lower function : converts all charcters to Lowercase      */


--		Task convert data of first_name to upperCase and lowercase for country :

SELECT 
    first_name,
    country,
    CONCAT(first_name,'_' ,country) as Name_Country,
    UPPER(first_name) As FIRST_NAME_UPPERCASE,
    Lower(country) AS country_lowercase
FROM customers

--					7-1-3  TRIM FUNCTION :

/*  Remove Leading and Trailing spaces (in the stare and in the end of value)

Task : Find customers whoes first_name contains Leading or trialing spaces	*/

select
    first_name
from customers
where first_name !=  trim(first_name)

--					7-1-4  REPLACE FUNCTION :

/*	Replaces specific character with a new character 

Task : Remove dashes (-) frome a phone number */

SELECT 
    '123-456-7890' phone_Number,
    REPLACE('123-456-7890', '-', '') Clean_Phone


--					7-1-5  LEN FUNCTION :
/*	Counts how many characters you have in one value 

Task : Calculate the lenght of each customer's first name */

SELECT 
    first_name,
    LEN(first_name) Len_first_name
FROM customers


--					7-1-6  LEFT & RIGHT FUNCTION :
/*													LEFT FUNCTION: Extracts specific Number of characters from the START 
                                                    RIGHT FUNCTION: Extracts specific Number of characters from the END 

TASK ONE : Retrieve the first two characters of each first_name */

SELECT 
    first_name,
    LEFT (TRIM(first_name), 2) STR_Name
FROM customers

-- Task TWO : Retrieve	the last two charcters of first_name :


SELECT 
    first_name,
    LEFT (TRIM(first_name), 2) STR_Name,
    RIGHT (UPPER(TRIM(first_name)), 2) End_Name
FROM customers


--					7-1-6  SUBSTRING FUNCTION :

/*	Extract a part of string at a specified position  syntax : SUBSTRING( Value, Start, Lenght)

Task : Retrieve a list of customers' first_names removing the first charcter		*/

SELECT 
    first_name,
    SUBSTRING(TRIM(first_name), 2, len(first_name)) sub_name
FROM customers
-------------------------------------------------------------------------------------------------
--			7-2	Number FUNCTIONS

--				7-2-1 ROUND FUNCTION: 

/* Rounds a numeric value to a specified number of decimal places.	eg :  */

SELECT 
    3.516 decimal_Num,
    ROUND(3.516, 2) round_2,
    ROUND(3.516, 1) round_1,
    ROUND(3.516, 0) round_0

--				7-2-2 ABS FUNCTION: 
-- Returns the absolute value of a number (removes any negative sign).

SELECT -546 neg_num,
    ABS(-546) abs_num


----------------------------------------------------------------------------------------
--			7-3	DATE & TIME FUNCTIONS


-- INTRO ( format of date and GETDATE() function )
use SalesDB
select 
    orderDate,
    ShipDate,
    CreationTime
From Sales.Orders


--	GETDATE(): Return the current date and time at the moment  when the query is executed 
select 
    orderDate,
    CreationTime,
    '2025-11-21' HardCoded,
    GETDATE() time_now
From Sales.Orders


--				 FUNCTIONS OVERVIEW :

/*   we can use it for : 
                        - Extract a part of date (year or Month or Day ...)
                                ->	DAY
                                ->	MONTH
                                ->	YEAR
                                ->	DATEPART
                                ->	DATENAME
                                ->	DATETRUNC
                                ->	EOMONTH

                        - Change FORMAT
                                ->	FORMAT
                                ->	CONVERT
                                ->	CAST

                        - Calculation  (ADD, DIFF)
                                ->	DATEADD
                                ->	DATEDIFF

                        - TEST AND VALIDATE (TRUE, FALSE, 0, 1)
                                -> ISDATE


            7-3-1 Part Extraction:
                
                7-3-1-1: DAY | MONTH | YEAR:
             
                            DAY(): return the day from a date ;		--->	synatax : DAY(date)
                            MONTH(): return the month from a date;	--->	synatax : MONTH(date)
                            YEAR(): return the yer from a date		--->	synatax : YEAR(date)    */

use SalesDB
SELECT orderID,
CreationTime,
DAY(creationTime) AS DayOfCreation,
MONTH(creationTime) AS MonthOfCreation,
YEAR(creationTime) AS YearOfCreation
From Sales.Orders
            
--				7-3-1-2: DATEPART():
--	Return a specific part of a date  as number 
SELECT CreationTime,
DATEPART(DD,CreationTime) as PartDay,			--		DD --> DAY
DATEPART(DW,CreationTime) as PartWeekDay,		--		DW --> WeekDay
DATEPART(WW,CreationTime) as PartWeek ,			--		WW --> Week 
DATEPART(MM,CreationTime) as PartMonth,			--		MM --> Month
DATEPART(QUARTER,CreationTime) as PartQuarter,	--		  Quarter
DATEPART(YY,CreationTime) as PartYear,			--		YY --> Year
DATEPART(HH,CreationTime) as PartHours,			--		HH --> Hours
DATEPART(MI,CreationTime) as PartMinutes,		--		MI --> Minutes
DATEPART(SS,CreationTime) as PartSecond			--		SS --> Second
From Sales.Orders

--				7-3-1-3: DATENAME():
-- return the name of a specific part of a date 

Select CreationTime,
DATENAME(WEEKDAY, CreationTime) As DayName,
DATENAME(DAY, CreationTime) As DAY, -- will return a number but it's string,
DATENAME(Month, CreationTime) as MonthName
from Sales.Orders

--				7-3-1-3: DATETRUNC():

-- Truncates the date to the specific part.
Select CreationTime,
--													+--------+---------------------------------------------+
--													|		 |											   |
--													|  Keep  |				Reset                          |
--													|	     |                                             |
--													|--------+---------------------------------------------|
DATETRUNC(YEAR, CreationTime) as YearTrunc,	--		|  Year  |	Month |  Day |  Hours | Minutes  |  Second |
--													+--------+---------------------------------------------+
--												


--													+-----------------+------------------------------------+
--													|		          |									   |
--													|       Keep      |				Reset                  |
--													|	              |                                    |
--													|-----------------|------------------------------------|
DATETRUNC(MONTH, CreationTime) As MonthTrunc,--		|  Year  |	Month |  Day |  Hours | Minutes  |  Second |
--													+-----------------+------------------------------------+



--													|------------------------|-----------------------------|
--													|						 |							   |
--													|          Keep			 |			 Reset             |
--													|						 |                             |
--													|------------------------|-----------------------------|
DATETRUNC(DAY, CreationTime) As DayTrunc,-- 		|  Year  |	Month |  Day |  Hours | Minutes  |  Second |
--													|------------------------|-----------------------------|     			



--													|---------------------------------|--------------------|
--													|								  |					   |
--													|              Keep		          |		  Reset        |
--													|								  |                    |
--													|---------------------------------|--------------------|
DATETRUNC(HOUR, CreationTime) As HourTrunc,-- 		|  Year  |	Month |  Day |  Hours | Minutes  |  Second |
--													|---------------------------------|--------------------|     			



--													|--------------------------------------------|---------|
--													|											 |		   |
--													|                   Keep      				 |	Reset  |
--													|											 |         |
--													|--------------------------------------------|---------|
DATETRUNC(MINUTE, CreationTime) As MinutesTrunc,-- 	|  Year  |	Month |  Day |  Hours | Minutes  |  Second |
--													|--------------------------------------------|---------|     			



--													|------------------------------------------------------|
--													|											 		   |
--													|                         Keep       		           |
--													|											           |
--													|------------------------------------------------------|
DATETRUNC(SECOND, CreationTime) As SecondsTrunc-- 	|  Year  |	Month |  Day |  Hours | Minutes  |  Second |
--													|------------------------------------------------------|     			


from Sales.Orders

--				7-3-1-4: EOMONTH():
--	Returns the last Day of a month.

Select CreationTime,
EOMONTH(creationTime) EndOfMonth , -- returns the last day of a months
cast(DATETRUNC(month,CreationTime) As date) StartOfMonth --Start day of the month
from Sales.Orders



--					7-3-1-5	 Part Extraction Use case: Data aggregations
--	Using date functions to take parts of a date (year, month, day) so you can group and summarize your data easily.
--	Task: How many orders were placed each year ? 
USE SalesDB
SELECT 
    YEAR(OrderDate), 
    COUNT(*) NrOfOrders
FROM	Sales.Orders
GROUP BY YEAR(OrderDate)		-- The ansewer is 10 orders   in year 2025

--	Task: How many orders were placed each Month ? 
USE SalesDB
SELECT 
    DATENAME(Month, OrderDate) AS orderDate, 
    COUNT(*) NrOfOrders
FROM	Sales.Orders
GROUP BY DateName(MONTH, OrderDate)		-- The ansewer is 4 orders in Februruay , 4 orders in January and 2 orders in  March


--					7-3-1-6 Data Filtering 

-- Using part of a date or date calculations to keep only the data you want (like this year, last month, today, etc.).

-- Task : show all orders that were placed during the month of February 

SELECT *
--	COUNT(*) NrOfOrders
FROM	Sales.Orders
Where MONTH(OrderDate) = 2

--					7-3-1-7 Function Comparison 
/*

                                                                            |----> DAY()
                                                                            |
                                                     |----> Numeric ? ----->|
                                                     |						|
                            |----> Day, Month ? ---->|						|----> MONTH()
                            |						 |      
                            |						 |----> Full Name -----------> DATENAME()
                            |
    Which Part ?			|---> Year ?			 |----------------------------> YEAR() 
                            |
                            |
                            |
                            |--> Other Parts ?	     |---------------------------> DATEPART() 
*/



--			7-3-2 Format Casting :

--		INTRO: 

--				Q: What is DATE FORMAT ? 

--				A:	
--					-	International Standard (ISO 8601)			YYYY-MM-dd	HH:mm:ss		---> SQL SERVER	STANDAR 
--					-	USA Standar :								MM-dd-YYYY	HH:mm:ss
--					-	European Standard:							dd-MM-YYYY	HH:mm:ss
                        
--				Q: What are FORMATING & CASTING? 

--				A:
--					-	Formating is changing the format of a value from one to another.
--					-	Casting is changing the data type from one to another.
--							|->	String ----cast()---> Number		
--							|-> Date   ----cast()---> String
--							|-> String ----cast()---> Date

--				7-3-2-1 FORMAT()
--	Format a date or time value 
--	syntax : FORMAT(value, fomrat[,culture])    // thired argement is optional 
    
    SELECT OrderDate, 
        FORMAT(orderDate, 'MM-dd-yyyy','USA') USA_FORMAT,	-- USA Formate
        FORMAT(orderDate, 'dd-MM-yyyy','fr-FR') EURO_FORMAT,	-- USA Formate
        FORMAT(orderDate, 'dd') dd,							 -- dd retrun day as number
        FORMAT(orderDate, 'ddd') ddd,						 -- ddd retrun shurt (abreviated) name of the day 
        FORMAT(orderDate, 'dddd') dddd,						 -- ddd retrun Full name of the day 

        FORMAT(orderDate, 'MM') MM,							 -- dd retrun Month as number
        FORMAT(orderDate, 'MMM') MMM,						 -- ddd retrun shurt (abreviated) name of the Month 
        FORMAT(orderDate, 'MMMM') MMMM						 -- ddd retrun Full name of the Month 

    FROM Sales.Orders

/*	Task : 
            Show CreationTime  using the following fomrat:
                |--> Day Wed Jan Q1 2025 12:34:56 PM
*/

SELECT  
    OrderDate,
    CreationTime,
    'Day ' + FORMAT(CreationTime, 'ddd MMM') +
    '    Q' + DATENAME(quarter, CreationTime) +
    '    ' + FORMAT(CreationTime, 'yyyy  hh:mm:ss ') +
        case 
            when DATEPART(HOUR, CreationTime)<12 THEN 'AM'
            ELSE 'PM'
        END
        AS CustomFormat
FROM Sales.Orders;

--				7-3-2-1-1 Formatting Use Case: Data Aggregations
-- e.g. : 
Select 
Format(orderDate, 'MMM yy') orderDate,
COUNT(*)
From sales.Orders
GROUP BY FORMAT(OrderDate, 'MMM yy')

--				7-3-2-1-2 Formatting Use Case: Data Standardization

-- Our Data could be stored in deffirent Technologie :  

    
/*	
            CSV Files   --------------  20/08/25  --------------->|	
                                                                  |	
                                                                  |						
                                                                  |		  Standar Format
               API      -------------- 20.08.2025 --------------->|	------> 2025-08-20 ---> we can use it   
                                                                  |	
                                                                  |			
                                                                  |	
             DataBase	-------------- 20 Aug 2025 -------------->|
*/

    
--				7-3-2-2 CONVERT():

-- Converts a date or time value to a different data type & Formats the value.

-- Syntax:	CONVERT(data_type, value[, Style])

Select 
    convert(int, '124' ) as INT,
    convert(VARCHAR,OrderDate,32 ) as Varchar 
From Sales.Orders

--				7-3-2-3 CAST():
-- Convert Value to a specific DATATYPE
-- Syntax:	CAST(Value AS data_type )

Select 
    CAST('123' AS INT)AS [Sring to Int],
    CAST(123 AS VARCHAR)AS [Int to String],
    CAST('2025-08-20' AS DATE) [String to Date],
    CAST('2025-08-20' AS datetime2) [String to DateTime],
    Cast(CreationTime As date) [Datetime to Date]
From Sales.Orders

--				7-3-2-4 FORMAT() vs CONVERT vs CAST():
/*

                                                                                                                    
|-------------------|-------------------------------|------------------------------------|		
|					|			CASTING				|			  FORMATING 			 |
|-------------------|-------------------------------|------------------------------------|													
|					|								|									 |
|	   CAST		    |	  Any Type To Any Type		|		     No Formating			 |					
|					|								|									 |
|-------------------|-------------------------------|------------------------------------|				
|					|								|									 |	
|	  CONVERT		|	 Any Type To Any Type		|   Formates Only The Data & Time    |
|					|								|									 |					
|-------------------|-------------------------------|------------------------------------|
|					|								|	Formtes :						 |								
|	  FORMAT		|	 Any Type To Only String	|			  - Date & time			 |
|					|								|			  - Numbers 			 |
|-------------------|-------------------------------|------------------------------------|				*/



--			7-3-3 Calculation :

--				7-3-3-1 DATEADD():
-- Adds or Subtracts a specific time interval to/From a date 
-- Syntax and e.g:
SELECT 
    CAST('2025-08-20' AS date) [Originale date],
    DATEADD(year, +3, CAST('2025-08-20' AS date)) [Add 3 year],
    DATEADD(Month, -2, CAST('2025-08-20' AS date)) [Substract  2 Month],
    DATEADD(DAY, +23, CAST('2025-08-20' AS date)) [ADD  23 Day]

--				7-3-3-1 DATEDIFF():
-- Find the difference between two dates .
-- Syntax and e.g
Select 
    CAST('2025-11-26' AS date) [Today],
    CAST('1997-10-14' AS date) [Birthday],
    DATEDIFF(year, CAST('1997-10-14' AS date),CAST('2025-11-26' AS date) ) as [Diff Years],
    DATEDIFF(Month, CAST('1997-10-14' AS date),CAST('2025-11-26' AS date) ) as [Diff Month],
    DATEDIFF(DAY, CAST('1997-10-14' AS date),CAST('2025-11-26' AS date) ) as [Diff Day]
/*
    Task 1 :
            Calculate the age of employees 
*/

use SalesDB
Select *,
DATEDIFF(year,BirthDate, GETDATE()) as Age
from Sales.Employees 


/*
    Task 2:
            Find the average shipping duration in days for each month  
*/
select
    Month(OrderDate),
    AVG(DATEDIFF (day,OrderDate,ShipDate)) As [Avg Shipping]
From Sales.Orders
Group  by MONTH(orderDate)


/*
    Task 3:
            Find the number of days between each order and the previous order  
*/
select
OrderID,
OrderDate CurrentOrderDate,
LAG(orderDate) Over (Order by orderDate ) PreviousOrderDate,-- LAG(): Access a value from the previous row 
DateDiff(day, LAG(orderDate) Over (Order by orderDate) , orderDate) NrOfDays

From Sales.Orders


--			7-3-4 Validation
--				7-3-4-1 ISDATE: 
/*
     Check is value is date: return 1 if the string value is valid date, and 0 if is not validate date 
     Syntax: 
                ISDATE(value)	
*/

Select orderDate,
ISDATE('20-08-2025') DateCheck,
ISDATE('August') IsDate 
from Sales.Orders


Select 
OrderDate,
ISDATE(OrderDate),
CASE WHEN ISDATE(OrderDate) = 1 THEN CAST(OrderDate AS DATE) 
END NewOrderDate
FROM
(
    SELECT '2025-21-08' As OrderDate UNION
    SELECT '2025-08-22' UNION
    SELECT '2025-08-23' 

)t


SELECT 
    OrderDate,
    ISDATE(OrderDate) AS IsValid,
    TRY_CAST(OrderDate AS DATE) AS NewOrderDate
FROM
(
    SELECT '2025-21-08' As OrderDate UNION
    SELECT '2025-22-08' UNION
    SELECT '2025-23-08' 
) t;

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------

--			7-4 NULL FUNCTIONS
/*
    NULL means Nothing, Unknown!
    NULL is not egale to anything!
            -	NULL is not zero
            -	NULL is not empty string
            -	NULL is not blank space
            
*/

--		7-4-1 OVERVIEW NULL FUNCTIONS 

/*			
                        +-------------+---------------------+----------------+	
                        | FirstValue  |     Functions		|   SortedValue  |
            +-----------|-------------|---------------------|----------------|	
            |			|			  |	      ISNULL	    |	             |
            |	Replace |	 NULL  	  |---------------------|	    40   	 |
            |	        | 		   	  |	     COALESCE       |				 |	
            |	Values  |-------------|---------------------|----------------|			 
            |			|		40	  |	      NULLIF        |	    NULL     |
            |-----------|-------------|---------------------|----------------|
            |-----------|-------------|---------------------|----------------|	
            |	Check	|			  |	      IS NULL	    |	  TRUE       |
            |			|	 NULL  	  |---------------------|----------------|
            |	 for    |			  |	     IS NOT NULL    |	  FALSE		 |	
            |			|-------------|---------------------|----------------|			 
            |	NULLS 	|		40	  |	      NULLIF        |	     NULL    |
            +-----------+-------------+---------------------+----------------+


*/

--		7-4-2 NULL FUNCTIONS: ISNULL
/*
        Replace a Null With a specific value 
        Syntax: 
            ISNULL(value, replacement_value)
*/
select *,
ISNULL(ShipAddress, 'NO Ship address') as  nullShipAddress,
ISNULL(BillAddress, 'NO Bill address') as  nullBilAddres
from Sales.Orders


--		7-4-3 NULL FUNCTIONS: COALESCE
/*
        Return the first non-null value froma list  
        Syntax: 
            COALESCE(value1,value2, value3,...)
*/
select *,
COALESCE
    (
    ShipAddress,				--Value 1
    BillAddress,				--Value 2
    'NO Ship and Bill address'	--Value 3
    ) 
    as isShipAndBilAddress

from Sales.Orders

----------------------------------------------------> ISNULL vs COALESCE >------------------------------------------------------------------------------------------------
/*


            |---------------------------|----------------------------|
            |			ISNULL			|		COALESCE			 |		  
            |---------------------------|----------------------------|
            |							|							 |
            |	- Limited to two		|	-  Unlimited			 |
            |         Values.			|							 |
            |---------------------------|----------------------------|
            |	- Fast					|	- Slow					 |
            |---------------------------|----------------------------|
            |	-						|							 |
            |		-> SQL Server: 		|							 |
            |				ISNULL		|	- Available in ALL   	 |
            |		-> Oracle:			|			Databases		 |
            |				NVL			|							 |
            |		-> MySQL:			|							 |
            |				IFNULL		|						     |
            |---------------------------|----------------------------|
*/

--		7-4-4 USE CASE : 

---->  ISNULL | COALESCE USE CASE: Handle the NULL before doing Data aggregations


/*
    Task : Find the average scores of the customers
*/

USE SalesDB
SELECT 
customerId,
score,
AVG(score) Over()as AvgScore1,
AVG(ISNULL(score, 0)) Over()as AvgScore2

FROM sales.Customers

----->  ISNULL | COALESCE USE CASE: Handle the NULL before doing mathematical operations

/*
    Task : 
        Display the full name of customers in a single field 
        by merging thier first and last Names, 
        And add 10 bonus points to each customer's score
*/

SELECT 
CustomerID,
FirstName,
LastName,
COALESCE(FirstName, '') +' '+ ISNULL(LastName,'') as FullName,
score,
COALESCE(Score,0) + 10 AS [New Score With Bonus] 
FROM sales.Customers

----->  ISNULL | COALESCE USE CASE: Handle the NULL before JOOINING tables
  /*
        When you join two tables, if the join columns contain NULL, the join will fail because:

            👉 NULL = NULL is always FALSE
            👉 So the join will NOT match
            👉 Result: missing rows$

        To fix this, we replace NULL with a temporary value before joining.

*/



----->  ISNULL | COALESCE USE CASE: Handle the NULL before sorting date 
/*
        When you sort (ORDER BY) a date column that contains NULL, 
        SQL Server places the NULL values first (ascending) or last (descending).
        This can break the order you want.
        To control how NULL behaves, you replace it with a fake date using ISNULL or COALESCE.
*/

SELECT 
CustomerId,
Score
From Sales.Customers
Order by ISNULL(Score, 0)  

--		7-4-5 NULL FUNCTIONS: NULLIF()
/*
    Compares two experssions returns:
        - NULL, if they are equal.
        - First Value, if they are not equale.

                                                / \
                                               /ARE\
                            NO				  /	Two \			YES
                _____________________________/ Values\_____________________________
                |							 \ equal /							   |
                |							  \  ?  /							   |		
                |							   \   /							   |	
                V                               \ /		                           V
         +-------------+  					    		                    +-------------+								
         |   Value 1   |													|	  NULL    |
         +-------------+													+-------------+
    Syntax :
        ---> NULLIF(Value1, Value2)

    eg: 
*/

select CustomerID,
score,
NULLIF(score, 500) ScorNuLLIF -- when score= 500 well return NULL

from Sales.Customers

--=======> NULLIF Use Case :

---> DIVISION BY ZERO :  Preventing the error of divising by zero 
/*
    Task:
            Find the sales price each order by dividing by the Quantity
*/


select 
OrderID,
Sales,
Quantity,
Sales / NULLIF(Quantity,0) as Price -- whithout NULLIF the result of last order return error dividing by Zero 
from sales.Orders

--		7-4-6 NULL FUNCTIONS: IS NULL() | IS NOT NULL()
/*
    -> IS NULL: Return true if the  value is NULL,
            Otherwise it return false

    -> IS NOT NULL: Return true if the  value is NOT NULL
            Otherwise it return false
Syntax:
        --> Value IS NULL 
        --> Value IS NOT NULL 
*/

------->		IS NULL USE CASE :

---> Filtring DATA: Searching for missing (NULLS) informations
/*
    Task:
        Identify the customers who have no score.
*/

Select *
From Sales.Customers
WHERE  score IS NULL
/*
    Task:
        Liste all customers who have scores.
*/

Select *
From Sales.Customers
WHERE  score IS NOT NULL

---> ANTI JOINS: Finding the unmatched rows between tqo tables		 ----- LEFT ANTI JOIN | RIGHT ANTI JOIN -----		 
/*
        LEFT ANTI JOINM: All rows from the Left without matching rows we use for that LEFT JOIN + IS NULL
        RIGHT ANTI JOINM: All rows from  the Right without matching rows we use for that RIGTH JOIN + IS NULL

        Task:
            Liste all details for customers who have not placed any order 
*/

SELECT 
    c.*,
    o.CustomerID,
    o.OrderID
from sales.Customers  as c
LEFT JOIN sales.Orders as o 
On c.CustomerId = O.CustomerID
Where O.OrderID IS NULL

---> NULL vs Empty vs Space
/*
        NULL				------->	Means nothing, unknown!	
        EMPTY STRING''		------->	String value has zero characters
        BLANK SPACE			------->	String value has one or more space characters
*/

WITH Orders as (
SELECT 1 Id, 'character' type,'A' Category UNION
SELECT 2,'Null', NULL UNION
SELECT 3, 'Empty Character','' UNION
SELECT 4,'Space charcter', ' '
)
select *,
DATALENGTH(Category) CatLenght
from Orders



--		7-4-7 NULL FUNCTIONS: SUMMARY:
/*

    ->  NULLs special markers mean missing value.

    ->  Using NULLs can optimize storage and performance.

    -> FUNCTIONS: 
                 
                 |-->  COALESCE | ISNUL             ---> NULL --> 30 (value)
                 |-->  NULLIF  --->  30 (value)     ---> NULL
                 |
                 |                                            |-----> TRUE
                 |-->  ISNULL | IS NOT NULL          --->  ?  |
                                                              |------> False  
                 
    ->  USE CASES:
    
                  |---> Handle Nulls - Data agrregation.
                  |
                  |---> Handle Nulls - Mathematical operations.
                  |
                  |---> Handle Nulls - Joining Tables.
                  |
                  |---> Handle Nulls - Sorting Data.
                  |
                  |---> Finding unmatched data - Left Anti Join |  Right Anti Join
                  |
                  |                       |---> Nulls
                  |                       |
                  |---> Data Policies --->|
                                          |
                                          |--->Default Value

*/
 
--			7-5 CASE STATEMENT:
/*
        -  Evaluate a list of conditions and rerurns a value when first condition  is met.
        - Syntax :
                    CASE

                       WHEN condition1 THEN result1

                       WHEN condition2 THEN result2

                       WHEN condition3 THEN result3

                       ...

                       ELSE result -- the default value if all condition is false 

                    END
*/

--  			7-5-1 CASE STATEMENT: HOW IT WORKS
/*
e.g 1: One condition

    CASE
        
        WHEN Sales > 50 THEN 'Hight'        -- if sales is grather than 50  will retun 'Hight' and if not happen will END the case 

    END

e.g 2: Two conditions

    CASE
        
        WHEN Sales > 50 THEN 'Hight'        -- if sales is grather than 50  will retun 'Hight'and end the case  and if not happen will go to condition 2 

        WHEN Sales > 20 THEN 'Medium'        -- if sales is grather than 20  will retun 'Medium'and end the case  and if not happen will go to ELSE and return 'Low'

        ELSE 'Low'
   END
*/

--  			7-5-2 CASE STATEMENT: USE CASE
/*
    
    -   Main purpose is Data Transformation
    -   Derive new information
            -   Create new Columns based on existing data 

*/

--  			    7-5-2-1 USE CASE: CATEGORIZING DATA
/*
    -   Group the data into different categories based on certain conditions.

    -   Task : 
            Generate a report showing the totale sales for each category:

                -   High: If the sales higher than 50.

                -   Medium: If the sales bbetween 20 and 50.

                -   Low: If the sales equal or lower than 20.

            Sort the result from lowest to highest.
*/
use SalesDB
SELECT 
Category,
SUM(Sales) As TotalSales
FROM(
    select 
    orderId,
    Sales,
     CASE
        WHEN Sales > 50 THEN 'High'  
        WHEN Sales > 20 THEN 'Medium'
        ELSE  'Low'
    END Category
    From Sales.Orders
    )t
Group by Category

order by TotalSales desc



--  			    7-5-2-2 USE CASE: MPPING VALUES

--    Transform the values from one form to another.

/*
    Task 1:
            --  Retrieve employee details withe gender displayed as full text
*/

SELECT 
    EmployeeId,
    FirstName,
    LastName,
    Gender,
CASE
    WHEN  Gender='M' then 'Male'
    WHEN Gender='F' THEN 'Female'
    ELSE 'Not Avaiable'
End GenderFullTxt
From Sales.Employees

/*
    Task 2:
            --  Retrieve Customer details withe abbreviated country code
*/

SELECT CustomerID,
Country,
CASE
    When Country='Germany' THEN 'DE'
    When Country='USA' THEN 'US'
    ELSE 'Avalaible country'
END CountryAbb

From Sales.Customers
    
--  			    7-5-2-3 USE CASE: HANDING NULLS
--  Replace a specific NULLs with a specific value.
--      - NULLs can lead to inccurate resulte
--          Which can lead to wrong decision-making.
/*
    Task: 
        Find the averge scores of customers and treat NULLs as 0
        And additional provide details such CustomerID & LastName

*/

Select 
customerID,
score,
Avg(
    Case 
        when score IS NULL then 0
        else Score
    END
) over() As AvgScore
From Sales.Customers
order by Score

--  			    7-5-2-4 USE CASE: CONDITIONAL AGGREGATION
/*
    Apply aggregation functions only on subsets data fulfill Certain conditions.
*/
/*
    Task:
        Count how many times eache customer has made an order with sales greatet than 30.
*/

Select
    CustomerID,  
    Sum(case 
        when sales > 30 then 1
        else 0
     end ) TotalOrdersHightSales,
     Count(*) TotalOrder
From Sales.Orders
Group by CustomerID
--  			 7-5-3 CASE STATEMENTE: RULS
/*
    -   The data type of the results must be matching 
    -   Case staemtent can be used anywhere in the query 
*/

    SELECT 
Category,
SUM(Sales) As TotalSales
FROM(
    select 
    orderId,
    Sales,
     CASE
        WHEN Sales > 50 THEN 'High'  
        WHEN Sales > 20 THEN 2          -- These will be return error causte is defferent type int
        ELSE  'Low'
    END Category
    From Sales.Orders
    )t
Group by Category
order by TotalSales desc
        

--  			 7-5-4 CASE STATEMENTE: QUICK FORM
/*

 +--------------- FULL FORM -------------------+                                   +--------------- QUICK FORM -------------------+ 
 |                                             |                                   |                                              |               
 |  CASE                                       |                                   |     CASE Country                             |       
 |       When Country = 'Germany' THEN 'DE'    |                                   |         WHEN 'Germany' THEN 'DE'             |                               
 |       When Country = 'India' THEN 'IN'      |                                   |         WHEN 'India' THEN 'IN'               |               
 |       When Country = 'Morroco' THEN 'MR'    |                                   |         WHEN 'Morroco' THEN 'MR'             |                       
 |       When Country = 'Alger' THEN 'ALG'     |                                   |         When Country = 'Alger' THEN 'ALG'    |                               
 |       When Country = 'Angola' THEN 'ANG'    |                                   |         WHEN 'Angola' THEN 'ANG'             |                   
 |       ELSE 'n/a'                            |                                   |         ELSE 'n/a'                           |  
 |   END                                       |                                   |     END                                      |  
 |                                             |                                   |                                              |  
 +---------------------------------------------+                                   +----------------------------------------------+

*/

--  			 7-5-5 CASE STATEMENTE: SUMMARY

/*
    Evaluate a list of condition and return a value when the First condition is met
    
    RULES: 
                -   The data type of the result must be matching.
         
    USE CASES: 
                -   Derive New Information 
                -   Categorizing Data
                -   Mapping Value
                -   Handling NULLs
                -   Conditional Agreggations
*//*
========================================================================================================================================================================

	                                                        CHAPTER 8 : AGGREGATION & ANALYTICAL FUNCTIONS

========================================================================================================================================================================

*/

--          8-1 AGGREGATION FUNCTIONS:
/*
    -   The aggregate functions Accept multiples rows and values and output can be one single value 
    -   COUNT(): Count the number of the rows
    -   SUM(): sume the values of the rows
    -   MAX(): Return the max Value
    -   MIN(): Return the min value 
    -   AVG(): Calc Avg of values
*/

/*
    Task : 
        --> Find the total number of orders 
        --> Find the total Sales of all orders 
        --> Find the AVG Sales of all orders 
        --> Find the Highest Sales of all orders 
        --> Find the Lowest Sales of all orders 

*/

use MyDatabase
Select 
Count(*) As total_nr_orders,
SUM(sales) as total_sales,
AVG(sales)  as avg_sales,
Max(sales)  as highest_sales,
Min(sales)  as Lowest_sales
From orders 

--          8-2 Window FUNCTIONS:
/*

 . INTRO:
            Q: 
                What & why Window? 

            A:  
                -   Performer calculations (e.g aggregation) on specific subset of data 
                    without losing the level of details of rows. 
                
            Q: 
                What is the defferent between Group by & Window:

            A: 
                -   Group by:
                                -   Returns a single row for each group (Cahnge the granularity) 
                                -   Simple Aggregation
                                +   Functions:
                                                -   Aggregat functions: COUNT(expr) | SUM(expr) | AVG(expr) | MAX(expr) | MIN(expr)
                                    
                                -   Simple Data Analysis 

                -   Window 
                                -   Return a result for each row ( The granulatiy stays the same)  
                                -   Aggregation +  keep details 
                                +   Functions:
                                                -   Aggregat functions:             COUNT(expr) | SUM(expr) | AVG(expr) | MAX(expr) | MIN(expr) 
                                                -   Rank Functions:                 ROW_NUMBER() | RANK() | DENSE_RANK() | CUME_DIST() | PERCENT_RANK() | NTILE()
                                                -   Value ( Analytics) Functions:   LEAD(expr,offset,default) | LAG(expr,offset,default) | FIRST_VALUE(expr)   
                                -   Advanced Data Analysis.
*/

/* 
    Task 1: 
        Find the total sales across all orders
*/

USE SalesDB

Select 
SUM(sales) TotalSales
FROM Sales.Orders


/* 
    Task 2: 
        Find the total sales for each product.
*/

USE SalesDB

Select 
productID,
SUM(sales) TotalSales
FROM Sales.Orders
GROUP BY ProductID      --Rsult Granularity: the number of rows in the output is defined by the dimentions 


/* 
    Task 3: 
        Find the total sales for eache product, additionally provide details sush order id & order date 
*/

USE SalesDB
-- USE GORUP BY
Select 
OrderID,
OrderDate,
productID,
SUM(sales) TotalSales
FROM Sales.Orders
GROUP BY                   --  !!!! GROUP BY RULE: All columns in select must be included in GROUP BY
        OrderID,
        OrderDate,
        ProductID           -- !!! GROUPE BY LIMITS: Can't do aggregations and provide details at same time.
            
-- Using WINDOW

-- USE GORUP BY
Select 
    OrderID,
    OrderDate,
    PRODUCTID,
    SUM(sales) OVER(PARTITION BY ProductID)           -- Result Granularity: Window functions returns a result for eache row   
FROM Sales.Orders


--                  8-2-1 Window FUNCTIONS: THE SYNTAX

/*
    For Syntax we have two part: 

                                    Window Functions    -       Over Clauses: Partition Clause | Order Clause | Frame Clause

                                                                                                                      Expression                             Partition 
                                                                                                                                                               Clause
                                                            
                                                    |-->    COUNT(expr).................|------------------------->   All Data Type     |------>|                   
                                                    |                                   |                                               |       |
                                                    |-->    SUM(expr)...................|------------------->|                          |------>|         
                                                    |                                   |                    |                          |       |
                       |---> Aggregate Functions ---|-->    AVG(expr)...................|------------------->|                          |------>|
                       |                            |                                   |                    |----->  Numeric           |       |
                       |                            |-->    MAX(expr) ..................|------------------->|                          |------>|
                       |                            |                                   |                    |                          |       |
                       |                            |-->    MIN(expr)...................|------------------->|                          |------>|
                       |                                                                |                                               |       |
                       |                                                                |                                               |       |
                       |                            |-->    ROW_NUMBER()................|------------------->|                          |------>|
                       |                            |                                   |                    |                          |       |
                       |                            |-->    RANK()......................|------------------->|                          |------>|
                       |                            |                                   |                    |                          |       |
                       |                            |-->    DENSE_RANK()................|------------------->|----->   Empty            |------>|---------> Optional
    Window Functions --|---> RANK Functions --------|                                   |                    |                          |       |
                       |                            |-->    CUME_DIST().................|------------------->|                          |------>|
                       |                            |                                   |                    |                          |       |
                       |                            |-->    PERCENT_RANK()..............|------------------->|                          |------>|
                       |                            |                                   |                                               |       |
                       |                            |-->    NTILE(n)....................|--------------------------->  Numeric          |------>|
                       |                                                                |                                               |       |
                       |                                                                |                                               |       |
                       |                                                                |                                               |       |
                       |                            |-->    LEAD(expr)..................|------------------->|                          |------>| 
                       |                            |                                   |                    |                          |       |
                       |                            |-->    LAG(expr,offset,default)....|------------------->|                          |------>|
                       |---> Value (Analytics) -----|                                   |                    |----> All Data Type       |       |
                                 Functions          |-->    FIRST_VALUE(expr)...........|------------------->|                          |------>|
                                                    |                                   |                    |                          |       |
                                                    |-->    LAST_VALUE(expr)............|------------------->|                          |------>|

        
        
        
        -> Function Expression : Arguments you pass to a function can be:
                        
                                ->  Empty:                  RANK()                                          OVER (ORDER BY OrderData)
                                ->  Column:                 AVG(Salse)                                      OVER (ORDER BY OrderData)
                                ->  Number:                 NTEIL(2)                                        OVER (ORDER BY OrderData)
                                ->  Multiple Arguments:     LEAD(sales,2,10)                                OVER (ORDER BY OrderData)
                                ->  Conditional Logic:      SUM(CASE WHEN Sales > 100 THEN 1 ELSE 0 END)    OVER (ORDER BY OrderData)
*/


--                  8-2-1-1 Window FUNCTIONS: PARTITION BY
/*
        -   It divides the result set into separate groups (partitions)
        -   calculated independently for each group, without reducing the number of rows like GROUP BY.

        -   GROUP BY → groups rows and reduces the number of rows.
        -   PARTITION BY → groups rows logically but keeps all rows.
*/


/*
    Task 1: 
        -   Found the total sales across all orders
        -   Additionally provide details such order id & order data
*/
use SalesDB
SELECT
OrderId,
OrderDate,
Sales,
SUM(sales) Over() TotalSales -- Expression is empty because we need the sume of all orders
From sales.Orders



/*
    Task 2: 
        -   Found the total sales for each product
        -   Additionally provide details such order id & order data
*/
use SalesDB
SELECT
OrderId,
OrderDate,
ProductID,
Sales,
SUM(sales) Over(PARTITION BY productID) TotalSales -- partition by product 
From sales.Orders


/*
    Task 3: 
        -   Find the total sales across all Orders
        -   Found the total sales for each product
        -   Additionally provide details such order id & order data
*/
use SalesDB
SELECT
OrderId,
OrderDate,
ProductID,
Sales,
SUM(sales) Over(PARTITION BY productID) TotalSalesByProducts,-- partition by product 
Sales,
SUM(Sales) Over () TotalSales
From sales.Orders

/*
    Flexibility of Window: Allows aggregation of data 
                           at different granularities within
                                    the same query.
*/

/*
    Task 4: 
        -   Found the total sales for each 
        Combination of product and order satus.
*/
use SalesDB
SELECT
OrderId,
OrderDate,
ProductID,
Sales,
SUM(sales) Over(PARTITION BY productID) TotalSalesByProducts,-- partition by product 
Sales,
SUM(Sales) Over () TotalSales,
productID,
OrderStatus,
Sales,
SUM(sales) Over(PARTITION BY ProductID, OrderStatus)  TotalSalesByProductAndStatus
From sales.Orders

--                  8-2-1-2   Window FUNCTIONS: ORDER BY
/*
    Sort the data within a window (Ascending | Descending)
        -   Order by is Optional for Aggregation Functions
        -   Required for Rank Fuctions and ValueFunctions
*/

/*
    Task:
        -Rank each order based on thier sales from the highest to lowest 
        -Additionaly provide details such orderId and OrderDate.
*/

SELECT 
    OrderID,
    OrderDate,
    Sales,
    RANK() Over(Order by sales  Desc) RankSales 
        /*
             Default Sorting: As default ORDER BY sorts the data in ascending order 'ASC'
                                        (From Lowest to highest)
        */
     
From Sales.Orders

--                  8-2-1-3 WINDOW FRAME
/*
    -   The FRAME clause is the part of a window function that tells
            SQL exactly which rows inside the partition 
                should be included in the calculation.
    -   Synatx :

====================================================== Syntax: ==========================================================================                       
                               AVG(Sales) OVER (PARTITION BY Category ORDER BY OrderDate
        ROWS             BETWEEN            CURRENT ROW                     AND                 UNBOUNDED FOLLOWING  )
         |                                        |                                                        |
    Fram Types:                         Frame Boundary (Lower Value):                         Frame Boundary (Higher Value):           
            -   ROWS                        -   CURRENT ROW                                         -   CURRENT ROW
            -   RANGE                       -   N PRECEDING                                         -   N FOLLOWING
                                            -   UNBOUNDED PRECIDING                                 -   UNBOUNDED FOLLOWING
=========================================================================================================================================
    - N.B ::
            -   UNBOUNDED FOLLOWING: The last possible row within a window.
            -   N-PRECIDING: The N-th row before the corrent row.
            -   UNBOUNDED PRECIDING: The first possible row within aa window.
    -   RULES:
========================================================= RULES: =========================================================================       
        -   Frame Clause can only be used together with order by clause)
        -   Lower Value must be BEFORE the higher Value.
==========================================================================================================================================
*/

SELECT 
orderID,
OrderDate,
OrderStatus,
Sales,
SUM(sales) OVER (PARTITION By OrderStatus ORDER BY OrderDate 
    ROWS BETWEEN  CURRENT ROW AND 2 FOLLOWING) TotalSales
    /* 
        -    #1 Window : Delivred orders 
        -    #2 Window : Shipped orders 
    */
FROM Sales.Orders

/*
        -   COMPACT FRAME:
                         -   For Only PRECIDING, the CURRENT ROW can be skipped
                                -  NORMALE FORM    -->     ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING.
                                -   SHORT FORM     -->     ROWS 2 FOLLOWING
*/
--                  8-2-2 WINDOW FUNCTION: 4x RULES

--                      RULE: #1
    /*
        Window function can be use ONLY in SELECT and ORDER BY Clauses 
        Note: Window Function can't be used to filter Data 
    */

--                      RULE: #2
    /*
        Nesting Window function is not allowed! 
        Window function cannot be used in the context of another window function or aggregate
    */

--                      RULE: #3
    /*
        SQL execute WINDOW Functions after WHERE Clause  
    */

 /*
    Task:
        Find the total sales for each order status,
         only for two products 101 and 102
*/


SELECT 
orderID,
OrderDate,
OrderStatus,
ProductID,
Sales,
SUM(sales) OVER (PARTITION BY OrderStatus) TotaleSales -- Afte where clause the window function excuted 
FROM Sales.Orders
Where ProductID IN(101,102) -- In the first Where Clause excuted 

--                      RULE: #4

/*
    Window Function can be used together with GROUP BY in
    the same query, ONLY if the same columns are used 
*/

/*
    Task:
        Rank Customers based on their total Sales 
*/

SELECT
    CustomerID,
     RANK() OVER(ORDER BY SUM(Sales) DESC ) RankCustomer,
    SUM(Sales) TotalSales
   
FROM Sales.Orders
GROUP BY CustomerID 

/*
    Note:
        Use GROUP BY for simple Aggregation
*/

--                  8-2-3 WINDOW FUNCTION: SUMMARY
/*
    SQL WINDOW FUNCTIOS: 
        -   Performs Calculation en subset of data without losing details 
        +   Window vs Group by:
            -   Window is more poqwerfull dynamic than Group By
            -                   |-> Advanced --> WINDOW
            -   Data Analysis---|
                                |-> Simple  --> Group By

            -   Use Group By + Window in same Quary, only if same conlumn used.
                    -   Step 1  -> Group By
                    -   Step 2  ->  Window Functions
        -   Components: 
                        Window Functions + Window Definition OVER
                                                       |
                                   |-------------------|-------------------|
                                   V                   V                   V
                          +-----------------+ +-----------------+ +-----------------+
                          | Divide Data:    | |  Sort Data      | | Define Subset   |
                          |  PARTITION BY   | |     ORDER BY    | |     FRAME       |
                          +-----------------+ +-----------------+ +-----------------+
                          
        - Rules:
            -   Nesting is not allowed !
            -   Window can be used only in Select and Order By.

*/

--                  8-3 WINDOW AGGREGATION FUNCTIONS
 /*
    INTRO:
            Q:
                What are Aggregate Window Function ?
            A:
                - Aggregate Window Functions = Aggregate functions + Window (OVER)
                - They calculate totals over a specified window (partition) while keeping each row visible.
                -  Return One Aggregate Value
            Q: 
                Syntax of Aggregat Function ?
            A:
                AGGREGATE_FUNCTION(column_name) 
                    OVER (
                        [PARTITION BY expression]
                        [ORDER BY expression]
                        [ROWS or RANGE frame_clause]
                    )
     */

--                         8-3-1 WINDOW AGGREGATION FUNCTIONS: COUNT()

/*
        -   Returns the number of rows within a window 
        -   Counts the number of values in a column, regardless of thier Data Types.
*/

/*
    Task    1:
        Find the total number of orders for eache product
*/

SELECT 
    OrderID,
    CustomerID,
    OrderStatus,
    productId,
    Count(*) over(PARTITION BY productId) OrderStautCount,
    Sales
From Sales.Orders

/*
    Task    2:
        -   Find the total number of orders.
        -   Find the total number of orders for each Customers
        -   Additionally provide details such order id & order data 
*/

SELECT 
    OrderId,
    OrderDate,
    OrderStatus,
    productId,
    Count(OrderID) over() TotaleOrders,
    Count(OrderID) over(Partition by CustomerId) OrdersByCustomer,
    Sales
From Sales.Orders

/*
    Task    3:
        -   Find the total number of Cutomers.
        -   Additionally provide All cutomer's details 
*/

SELECT 
    *,
    Count(*) over() TotaleCustomers
From Sales.Customers

/*
    Task    4:
        -   Find the total number of Scores for the customers.
        -   Additionally provide All cutomer's details 
*/

SELECT 
    *,
    COUNT(*) Over() TotalCustomers,
    COUNT(Score) Over() TotlaScores
     /*
        Detecting Number of NULLs by Comparing to total number of rows
        5 (Total Customers ) - 4 (TotlaScores)  = 1 Null in Scores
     */
From Sales.Customers

/*
    DATA QUALITY ISSUE: 
        Duplicates leades to inaccuracies in analysis
        COUNT() can be used to identifiy duplicates
*/

/*
    Task    5: 
        Check whether the table 'Orders' Contains any duplicate rows
*/

Select 
OrderId,
COUNT(*) OVER(Partition By OrderId) CheckPK
/*
    EXPECTATION:
        Maximum number of rows for eache window(ID)=1
*/
From sales.orders

Select
OrderId,
Count(*) over(Partition By OrderId) CheckPK
--    There is a duplicate PK 
From Sales.OrdersArchive

--                  COUNT | USE CASE:
/*
    #1  Overall Analysis.
    #2  Category Analysis.
    #3  Quality Checks: Identify NULLs.
    #4  Quality checks: Identify Duplicate.
*/        

--                         8-3-2 WINDOW AGGREGATION FUNCTIONS: SUM()
/*
    Return The sum of vaues within a window 
*/

/*
    Task    1:
        -   Find the total sales all orders
        -   And total sales for each product.
        -   Additionaly provide some details such order Id, Order Date.
        
*/
select *
From sales.Orders

SELECT
OrderId,
OrderDate,
sales,
SUM (Sales) over() TotalSales,
/*
    #1 USE CASE: OVERALL ANALYSIS
        Quick summary or snapshot 
        of entire dataset
*/
ProductID,
SUM (Sales) over(PARTITION BY ProductID) SalesByProduct
/*
    #2 USE CASE: TOTAL PER GROUPS
        Group-wise analysis, to understand patterns within 
        different categories.
*/
From Sales.orders

/*
    Task    2: 
           Find the percentage contribution of  each product's sales to the totale sales.
*/

Select 
OrderID,
ProductID,
Sales,
Sum(sales) over() TotalSales,
SUM(sales) over(Partition by ProductId) TSalesByProduct,
(((SUM(sales) over(Partition by ProductId)) * 100) / (SUM(sales) over() )) As [Percentage each Product  ( % ) ] 
From Sales.orders


--                         8-3-3 WINDOW AGGREGATION FUNCTIONS: AVG()
--  Return the average of values within a window 
/*
    Task    1:
        -   Find the Averges Salse across all orders 
        -   Find the Averges Salse for each product
*/

SELECT
    OrderId,
    OrderDate,
    ProductID,
    Sales,
    AVG(sales) over() AvgSales,
    AVG(COALESCE(Sales, 0)) Over(PARTITION BY PRODUCTID) AvgSalesByProducts
From Sales.Orders


/*
    Task    2:
        -   Find the Averges Score of cutomers. 
        -   Additionaly, provide details as CustomerId and LastName
*/

SELECT
    CustomerID,
    LastName,
    score,
    AVG(COALESCE(Score, 0)) over() AvgScore -- We have one NULL in Score so AVG it's calc just AVG of 4 Value cause of that we use COALESCE(score, 0) 
From Sales.Customers


/*
    Task    3:
        -   Find all orders where sales are higher than the avg sales across all orders.
*/
SELECT 
*
FROM(
    SELECT
        OrderId,
        OrderDate,
        ProductID,
        Sales,
        AVG(sales) over() AvgSales
   
    From Sales.Orders
)t where sales > AvgSales

--                         8-3-4 WINDOW AGGREGATION FUNCTIONS: MAX() | MIN()

/*
        +   MIN(): 
                Return the lowest value within a window
        +   MAX():
                Returns the highest Value within a window 
*/

/*
    Task    1:
        -   Find the highest & lowest sales across all orders and 
            the highest & lowest sales for each product.
        -   Additionally, Provide details suach as order ID and Order date.
*/

Select 
OrderID,
OrderDate,
Sales,
Max(sales) over() MaxSales,
Min(Sales) over() MinSales,
ProductID,
sales,
Max(sales) over(Partition by productId) MaxSalesByProduct,
Min(Sales) over(Partition by productId) MinSalesbyProduct
From Sales.orders

/*
    Task    2:
        -   Find the employees who have a highest salaries.
*/
Select *
From(
    Select *, 
    Max(salary) over() MaxSalarie
    From sales.Employees
    )t
where Salary = MaxSalarie


/*
    Task    3:
        -   Calculate the deviation of eache sale from both the minimum and maximum sales amount.
*/
use SalesDB
Select 
OrderID,
OrderDate,
Sales,
Max(sales) over() MaxSales,
Min(Sales) over() MinSales,
Sales,
Sales  - MIN(sales) over() Dev_min ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 
Max(sales) over() - Sales Dev_Max                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  
FROM Sales.Orders          


--                         8-3-5 WINDOW AGGREGATION FUNCTIONS: ANALYTICAL USE CASE

      

/* 
    ANALYSIS OVER TIME: 

    They aggregate sequance of members, and the aggregation is updated
    each time a new member is added

    RUNNING TOTAL: 

    Aggregate all value frome the beginning up to the current point
    without dropping off older data.
    e.g : 
 */
 use SalesDB
 Select
    OrderID,
    Sales, 
    SUM (Sales) over( order by orderDate) TotalByDate -- We'll have dfault frame clause : ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW 
From Sales.Orders
 
/*
    ROLLING  TOTAL (SHIFTING WINDOW):

    Aggregate all values within a fixed time window (e.g: 30 days)
    As new data is added, the oldest data point will be dropped
    e.g : 

 */
  use SalesDB
 Select
    OrderID,
    Sales, 
    SUM (Sales) over( order by orderDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW ) TotalByDate     -- 
From Sales.Orders
 

/*
    Task: 
        -   Calculate moving averge of sales for each product over time
        -   Calculate moving averge of sales for each product over time, Including only the next order
*/

 use SalesDB
 Select
    OrderID,
    ProductID,
    Sales,
    
    AVG (Sales) over( Partition by ProductId ) AVG_By_Product,    --  this is running AVG
    ProductID,
    OrderDate,
    Sales,
    AVG (Sales) over( Partition by ProductId ORDER BY orderDate ) Moving_AVG,   --  this is Rolling AVG by orderDate 
    /*
        NOTE:
                Over Time Analysis means sorting dates in scending order
    */
    AVG (Sales) over( Partition by ProductId ORDER BY orderDate ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING ) ROLLING_AVG    --  this is Rolling total by orderDate and including only the next order


From Sales.Orders


--                         8-3-5 WINDOW AGGREGATION FUNCTIONS:SUMMARY


/*
    
    Window aggregate functions : Aggregate set of values and return a single aggregated value 
   +--------+
   | RULES: |
   +--------+    
        |
        |                     |-->    Numbers (All Functions)
        |--->   Expressions --|
        |                     |-->    Any Data Type- COUNT()
        |
        |--->   All clouses are optional.
   
   +------------+
   | Use Cases: |
   +------------+    
        |
        |--->   Overall Analysis
        |
        |--->   Total per groubs Analysis. 
        |
        |--->   Part-to-whole Analysis.
        |
        |                              |-->   Average
        |--->   Comparison Analysis  --|
        |                              |-->   Extreme Highest/Lowest
        |
        |--->   Identify Duplicate 
        |
        |--->   Outlier Detection  
        |
        |--->   Running Total  
        |
        |--->   Roling Total  
        |
        |--->   Moving Averge 
*/


--                  8-4 WINDOW RANKING FUNCTIONS

/*
    INTRO:
            Q: 
                What are Ranking Window function ? 
            A:
                Assign a rank or position to each row within a defined partition (group) 
                and ordered result set — without reducing the number of rows.

            Q: 
                Why do we need ranking functions if SQL can already sort data?

            A: 
                Sorting only arranges rows, but ranking functions allow you to add numbers,
                ranks, and positions to rows inside your result — something ORDER BY alone cannot do.

          
            Q:
                How do ranking functions know how to rank the rows?

            A:
                Every ranking function requires an ORDER BY inside the OVER() clause,
                which tells SQL how to assign the ranks.

            Q:
                What are the main types of SQL ranking functions?
            A:
                -   ROW_NUMBER() — unique sequential numbers

                -   RANK() — ties get same rank, gaps appear

                -   DENSE_RANK() — ties get same rank, no gaps

                -   NTILE(n) — divides rows into equal groups
*/

                
--                  8-4-1 WINDOW RANKING FUNCTIONS: ROW_NUMBER()

--                      8-4-1-1 ROW_NUMBER(): DEF & SYNTAX


/*
    -   Assign a unique number to each row.
    -   It doesn't handle ties.
*/

/*
    Task:
        RANK the orders based on their sales From Highest to Lowest
*/

SELECT 
    OrderID,
    ProductID,
    Sales,
    ROW_NUMBER() over(Order by Sales DESC) SalesRank_Row
FROM Sales.Orders

--                      8-4-1-2 ROW_NUMBER(): USE CASE

--                          8-4-1-2-1 ROW_NUMBER() USE CASE : TOP-N ANALYSIS

--  Analysis the top performers to do targeted marketing
   
 /*
    Task:
        Find the top highest Sales for each Product.
*/
Select * 
From (
    SELECT 
        OrderID,
        ProductID,
        Sales,
        ROW_NUMBER() over(Partition by productID Order by Sales DESC) SalesRank_Row
    FROM Sales.Orders
)t
where SalesRank_Row = 1

--                          8-4-1-2-2 ROW_NUMBER() USE CASE : BOTTOM-N ANALYSIS

--  Help analysis underperformence to manage risks and to do optimizations
   
 /*
    Task:
        Find the Lowest 2 customers based on thier total Salses.
*/
Select * From(
SELECT 
    CustomerID,
    SUM(sales) [Totale Sales],
    ROW_NUMBER() Over(order by Sum(sales)) Rank_Customers
From Sales.orders
Group by CustomerID
)t
Where Rank_Customers <=2

--                          8-4-1-2-3 ROW_NUMBER() USE CASE : GENERATE UNIQUE IDs

-- Help to assign unique identifier for each row to help paginating 
--  Paginating: The process of breaking down a large date into smaller, more manageable chunks. 

/*
    Task:
        Assigne Unique IDS to the rows of the 'Orders Arcive' table
*/

Select
    ROW_NUMBER() over(order by OrderID) rankId,
     *
From sales.OrdersArchive

--                          8-4-1-2-4 ROW_NUMBER() USE CASE: IDENTIFY DUPLICATES.

--  Identify and remove dublicate rows to improve data quality 

/*
    Task:
        Identify the duplicate rows in the table 'Orders Archive'
        and Return a clean result without any duplicates
*/
Select * From
(
    Select
        ROW_NUMBER() over(Partition by OrderID order by CreationTime desc ) rankId,
         *
    From sales.OrdersArchive 
)t
Where rankId = 1


                            
--                  8-4-2 WINDOW RANKING FUNCTIONS: RANK()
/*
    -   Assign a rank to each row.
    -   It handles ties. ( if two rows have a same values they share same Rank)
    -   It leaves gaps in ranking ( possibilitie missing rank numbers e.g : Rank=> 1, 2,2, 4) 
*/
/*
    Task:
        RANK the orders based on their sales From Highest to Lowest
*/

SELECT 
    OrderID,
    ProductID,
    Sales,
    ROW_NUMBER() over(Order by Sales DESC) SalesRank_Row,
    RANK() over(Order by Sales DESC) SalesRank_RANK
FROM Sales.Orders

--                  8-4-3 WINDOW RANKING FUNCTIONS: DENSE_RANK()
/*
    -   Assign a rank to each row.
    -   It handles ties.
    -   It doesn't leaves gaps in ranking ( !! NO GAPS )
*/

/*
    Task:
        RANK the orders based on their sales From Highest to Lowest
*/

SELECT 
    OrderID,
    ProductID,
    Sales,
    ROW_NUMBER() over(Order by Sales DESC) SalesRank_Row,
    RANK() over(Order by Sales DESC) SalesRank_RANK,
    DENSE_RANK() over(Order by Sales DESC) SalesRank_DenseRank
FROM Sales.Orders
        
--                  8-4-4   INTEGER-BASED RANKING: COMPARISON
/*
            
                +-----------------------------------+-----------------------------------+-----------------------------------+
                |                                   |                                   |                                   |
                |           ROW_NUMBER()            |             RANK()                |           DENSE_RANK()            |
                |                                   |                                   |                                   |
                +-----------------------------------+-----------------------------------+-----------------------------------+
                |                                   |                                   |                                   |
                |            Unique Rank            |            Shared Rank            |           Shared Rank             |
                |                                   |                                   |                                   |
                +-----------------------------------+-----------------------------------+-----------------------------------+
                |                                   |                                   |                                   |
                |       Doesn't Handle Ties         |          Handles Ties             |           Handles Ties            |
                |                                   |                                   |                                   |
                +-----------------------------------+-----------------------------------+-----------------------------------+
                |                                   |                                   |                                   |
                |       No Gaps In Ranks            |          Gaps In Rank             |         No Gaps In Ranks          |
                |                                   |                                   |                                   |
                +-----------------------------------+-----------------------------------+-----------------------------------+
*/

--                  8-4-5   RANK WINDOW FUNCTION: NTILE

--                      8-4-5-1   RANK WINDOW FUNCTION NTILE: DEF & SYNTAX

/*
    Divided the rows into a specfied number of 
    approximately equal groups (Buckets)

                     Number of Rows 
    Bucket Size = -------------------
                   Number Of Buckets

    SQL RULE !!! : Larger groups come first 

*/ 
use SalesDB
Select 
    OrderId,
    Sales,
    NTILE(1) Over (Order By Sales Desc) OneBucket,
    NTILE(2) Over (Order By Sales Desc) TwoBucket,
    NTILE(3) Over (Order By Sales Desc) ThreeBucket,
    NTILE(4) Over (Order By Sales Desc) FoorBucket
From sales.Orders


--                      8-4-5-2   RANK WINDOW FUNCTION NTILE: USE CASE
--                          8-4-5-2-1 NTILE USE CASE: DATA SEGMENTATION
/*
        - For Data Analyst : Data Segmentation:
                Divides a dataset into distinc subsets based on certain criteria.
        - For Data Engineer: Equalizing load processing :

*/

/*
    Task:
        Segment all orders in 3 categories: high, medium and low sales.
*/
Select *,
case 
    when bucket = 1 then 'High' 
    when bucket = 2 then 'Medium'
    when bucket = 3 then 'Low' 
    else 'Null'
   end categories, 
   Sum(sales) over(Partition by bucket) TotalSalesbyCat
From(
    Select 
        OrderID,
        ProductID,
        Sales,
        NTILE(3) over(Order by Sales Desc)  bucket 
    From sales.Orders
    )t

--                          8-4-5-2-2 NTILE USE CASE: EQUALIZING LOAD

--It helps evenly distribute rows into equal-sized groups to balance workload or load across multiple teams or processes.

/*
    Task:
        In order to export the data divide the orders into 2 groups.
*/
Select * from (
Select *, 
NTILE(2) over(Order by orderId) Bucket
 From Sales.Orders) t
 where bucket=2


--                  8-4-5   RANK WINDOW FUNCTION: PERCENTAGE BASED RANKING
/*
        It assigns a position to each row based on ordering, allowing percentage-based ranking 
                to measure how far each row stands within the entire dataset.
        - CUME_DIST 
                Returns the cumulative distribution of a value within a dataset, 
                showing the percentage of rows less than or equal to the current row.
                 -  Inclusiive: The current row is included 

                                   Position Nr
              -      CUME_DIST = ---------------
                                  Number Of Rows

          - PERCENT_RANK
                   Returns the relative rank of a row as a percentage between 0 and 1, 
                   showing how a row ranks compared to the minimum and maximum ranking in the dataset.
                 -  Exclusive: The current row is Excluded 

   
                                     Position Nr - 1
               -     PERCENT_RANK = ------------------                                      
                                    Number Of Rows - 1                                                                                  */

 --                     8-4-5-1   PERCENTAGE BASED RANKING: CUME_DIST
 
 -- Cumulitive Distrubition calculates the distribution of data points within a window

 Select 
    OrderId,
    OrderStatus,
    Sales,
    CUME_DIST() Over(Order by sales DESC ) dist 
From Sales.Orders
where OrderStatus = 'Delivered'

 --                     8-4-5-1   PERCENTAGE BASED RANKING: PERCENT_RANK

 -- Calculate the relative position of eache row 

 Select 
    OrderId,
    OrderStatus,
    CustomerID,
    Sales,
    Round(PERCENT_RANK() Over( Order by sales Asc ), 2) dist 
From Sales.Orders
   

    /*
        Tie Rule: 
            The position of the first accurence of the same value 
    */

/* 
    Task:
        Find the products that fall within the highest 40% of price
*/

SELECT * ,
CONCAT(PricPercentage*100 , '%') as PercentagePrice

FROM (
SELECT 
    ProductID,
    product,
    Price,
    Round(PERCENT_RANK() Over( Order by Price DESC ), 2) PricPercentage 
FROM Sales.Products
)t
Where PricPercentage < = 0.4

--                  8-4-6  RANK WINDOW FUNCTION: SUMMARY
/*

    Assign a RANK for each row within a window

                                                        
                                                          |--> ROW_NOMBER()     
                                                          |--> RANK()     
    +----------+     |--> Integer-based-Ranking    ------>|--> DENSE_RANK()               
    |  TYPES   |-----|                                    |--> NTILE()
    +----------+     |
                     |                                    |-->  CUME_DIST()
                     |--> Percentage-based-Ranking ------>|
                                                          |-->  PERCENT_RANK()
    
                                                                        
    +----------+                                        +-----------+
    |  RULES   |                                        | USE CASES |
    +----------+                                        +-----------+
         |                                                    |
         |--> Expression --> Empty                            |-->  TOP N Analysis     
         |                                                    |
         |--> ORDER BY   --> Required                         |-->  Bottom N Analysis
         |                                                    |
         |--> Frame      --> Not Allowed                      |-->  Identify & Remove Duplicate 
                                                              |
                                                              |-->   Assing Unique IDs and Pagination
                                                              |
                                                              |-->   Data Segmentation
                                                              |
                                                              |-->   Data Distribution Analysis 
                                                              |
                                                              |-->   Equalizing Load Processing                                                                                                                                                                                 */




--                  8-5 WINDOW VALUE FUNCTIONS:

/* 
    INTRO:
            Q:
                What are value window fonction?
            A:
                Access a value from other rows. Like get the next or the precedent value row or the first, the laste value .
            Q:
                When to Use Value Window Functions?
            A:
                -   Compare current vs previous value

                -   Calculate differences or trends

                -   Get first/last event per group

                -   Time-series analysis

                -   Financial & business reporting


--                      8-5-1 WINDOW VALUE FUNCTIONS SYNTAX:

                                                                +--------------------+    +--------------------+    +--------------------+    +--------------------+ 
                                                                |     Expression     |    |   Parttion Clause  |    |    Order Clause    |    |    Frame Clause    |
                                                                +--------------------+    +--------------------+    +--------------------+    +--------------------+  
    +-----------------------+   +---------------------------+   +--------------------+    +--------------------+    +--------------------+    +--------------------+                                      
    |                       |   | LEAD(expr,offset,default) |   |                    |    |                    |    |                    |    |                    |   
    |                       |   +---------------------------+   |                    |    |                    |    |                    |    |                    |
    |         Value         |   +---------------------------+   |                    |    |                    |    |                    |    |     Not allowed    |        
    |                       |   | LAG(expr,offset,default)  |   |                    |    |                    |    |                    |    |                    |            
    |      (Analytics)      |   +---------------------------+   |    All Data Type   |    |      Optional      |    |      Required      |    +--------------------+                                           
    |                       |   +---------------------------+   |                    |    |                    |    |                    |    +--------------------+                                       
    |       Functions       |   | FIRST_VALUE(expr)         |   |                    |    |                    |    |                    |    |       Optional     | 
    |                       |   +---------------------------+   |                    |    |                    |    |                    |    +--------------------+
    |                       |   +---------------------------+   |                    |    |                    |    |                    |    +--------------------+                                          
    |                       |   | LAST_VALUE(expr)          |   |                    |    |                    |    |                    |    |   Should be Used   |            
    +-----------------------+   +---------------------------+   +--------------------+    +--------------------+    +--------------------+    +--------------------+
*/



--                      8-5-1 WINDOW VALUE FUNCTIONS LAG() LEAD():

--                          8-5-1-1  LAG(), LEAD() DEF & SYNTAX:

/*
        LEAD() :    Access a value from the next row within a window.
        LAG()  :    Access a value from the previous row within a window.
*/
use SalesDB
Select
    OrderId,
    ProductID,
    orderDate,
    Sales,
    LEAD(sales, 2, 10) over(partition by ProductID Order by OrderDate) next_sales,  
     --  Sales: Expression is required (any data type)
             -- 2 : Number of rows forward or backward from current row, default: 1
               -- 10: Returns default value if next/previous row is not avalaible!, Default: NULL.
                            -- Partition by is optional 
                                                -- Order by is Required 
    LAG(sales) over(partition by ProductID Order by OrderDate) previous_sales  
    
From Sales.Orders

 /*
    Task: 
        Analyze the month-over-month (MoM) Performance by finding the percentage change in sales 
                        between the current and the previous month
*/
/*
            Time Series Analysis :
                The process of analyzing the data to understand patterns, 
                 trends, and behaviors over time:
                    - YoY: Year-over-Year
                    - MoM: Month-over-Month
*/
Select *,
    CurrentMonthSales - previousSales As MoM_Change,
    Concat(Round(Cast((CurrentMonthSales - previousSales) as float)/ previousSales * 100, 2) ,' %') as MoMSales
From(
    Select 
       Month(OrderDate) OrderMonth,
        Sum(Sales)  CurrentMonthSales,
        Lag(Sum(sales)) Over(order by Month(orderDate)) previousSales,
        Lead(Sum(sales)) over(order by Month(OrderDate))NextSales
      From sales.orders
      Group by 
        Month(OrderDate)
        )t

--                          8-5-1-2  LAG(), LEAD() USE CASE: CUSTOMER RETENTION ANALYSIS
/*
    Measure cutomer's behavior and loyalty to help 
    businesses build a strong relationships with customers
*/
/*OrderDate
    Task:
        Analyse customers loyalty by ranking customers based
        on the averge number of days between orders
*/
use SalesDB

Select CustomerID,
AVG(DaysUntilNextto) As AvgDays,
RANK() Over( order by Coalesce(AVG(DaysUntilNextto), 999999999)) As RnkAvg
From(
    SELECT 
        OrderID,
        CustomerID,
        orderDate CurrentDate,
        LEAD(OrderDate) Over(Partition by CustomerID Order by OrderDate) NextOrder,
        DATEDIFF(Day, OrderDate, LEAD(orderDate) Over( Partition by CustomerID Order by OrderDate)) DaysUntilNextTo
    From sales.Orders)t
Group by
    CustomerID


--                      8-5-2 WINDOW VALUE FUNCTIONS: FIRST_VALUE(), LAST_VALUE() 

--                              8-5-2-1 FIRST_VALUE(), LAST_VALUE(): DEF & SYNTAX
/*
        -   FIRST_VALUE()   : Access a value from the first row within a window.
        -   LAST_VALUE()    : Access a value from the last row whithin a window.
*/
SELECT *,
    FIRST_VALUE(Score) OVER(ORDER BY COALESCE(Score, 999999)) firstScore,
    LAST_VALUE(Score) OVER(ORDER BY Score) LastScore
FROM Sales.Customers

/* 
   SQL Task:
        Find the lowest and highest sales for each product 
*/

SELECT
    OrderID,
    ProductID,
    Sales,
    FIRST_VALUE(sales) OVER(PARTITION BY ProductID  ORDER BY SALES ) LowestSales,
    LAST_VALUE(sales) OVER(PARTITION BY ProductID  ORDER BY SALES
        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING ) HighestSales

FROM Sales.Orders 

--                              8-5-2-2 FIRST_VALUE(), LAST_VALUE(): USE CASE

--      Compare to Extremes : how well a value is performing relative to the extremes.
/* 
   SQL Task:
        -   Find the lowest and highest sales for each product, 
        -   Find the difference in sales between the current and the lowest sales.
*/
SELECT *,
    Sales,
    Sales - LowestSales as Def_Lowe,
    highestSales - Sales as Def_Hight
From(
    SELECT 
        OrderID,
        ProductID,
        Sales,
        FIRST_VALUE(sales) Over( Partition by ProductID order by Sales) LowestSales,
        
        FIRST_VALUE(sales) Over( Partition by ProductID order by Sales DESC) HighestSales
    FROM sales.Orders
    )t

--                      8-5-2 WINDOW VALUE FUNCTIONS: SUMMARY()

/*
    
        -   Def: Allow Access specific Value from Another Row

                            |-->    Previous Value  --->    LAG()                       +------------------+
                            |                                                           |    USE CASES     |
        +-----------+       |-->    Next Value      --->    LEAD()                      +------------------+
        |   TYPES   |-------|                                                                   |
        +-----------+       |-->    First Value     --->    FIRST_VALUE()                       |-->    Time Serie Analysis: MoM + YoY
                            |                                                                   |
                            |-->    Last Value      --->    LAST_VALUE()                        |-->    Time Gaps Analysis: Customer Retention
                                                                                                |                                       |--> Highest
                                                                                                |-->    Comparison Analysis: Extreme -->|
                                                                                                |                                       |--> Lowest
         +-----------+
         |    RULES  |------|-->    Expression      --->    Any Data Type
         +-----------+      |
                            |-->    ORDER BY        --->    Required
                            |
                            |-->    FRAM            --->    Optional

        

========================================================================================================================================================================

	                                                        CHAPTER 9: ADVANCED SQL TECHNIQUES 

========================================================================================================================================================================
*/

--                          9-1 SUBQUERIES
/* 
    -   A Query Inside Another Query
    -   The resul of subqueries it return only one value
*/
 --                                 9-1-1  SUBQUERY RESULT TYPES:

 /*

                              |-->      SCALAR SUBQUERY: Single Value
    +----------------+        |
    |  RESULT TYPES  |--------|-->      ROW SUBQUERY: Multiple Rows + Single Column
    +----------------+        |
                              |-->      TABLE SUBQUERY: Multiple rows and Multiple Columns          */


--  SCALAR SUBQUERY
Select 
    AVG(sales)            -- Scalar Subquer : Only one Row & only one Column
From Sales.orders


--  ROW SUBQUERY
Select 
    CustomerID          -- Row Subquer : Multiple Rows & only one Column
From Sales.orders
 

--  Table SUBQUERY
Select 
    orderID,
    CustomerID, 
    Sales
                    -- Table Subquer : Multiple Rows & Multiple  Columns
From Sales.orders
 
 --                                 9-1-2  SUBQUERY IN FROM CLAUSE:
 Select 3571 - 3347 Lignes
 /*
    Task:
        Find the products that have a price 
        higher than the averge price off all products
*/
use SalesDB

--------------Main Query ---------------------
SELECT                                      --    
    *                                       --
 FROM                                       --
    (                                       --
 ---- Subquery ---------------------------  -- 
    Select                              --  --
        ProductID,                      --  --
        price,                          --  --
        AVG(Price) Over()As avgPrice    --  --
    From Sales.Products                 --  --
------------------------------------------  --
    )t                                      --
Where price > avgPrice                     --
                                            --
----------------------------------------------
/*
    TIP: 
            To check the intermediate results of a subquery, highlight it and execute 
*/

/* 
    Task : 
        Rank Customers based on thier total amount of sales 
*/
Select *,
    DENSE_RANK() Over(ORDER BY TotalSales DESC) rankCustomer
FROM (
    Select 
  
            OrderId,
            CustomerID,
            Sales,
            Sum(Sales) over(Partition by CustomerID) totalSales
    From Sales.Orders
    )t

 --                                 9-1-3  SUBQUERY IN SELECT CLAUSE:

 --     Used to aggregate data side by side with the main query's data, allowing for direct comparison.

 /* SYNTAX:
    
                Select 
                    Column1,
                    (Select column From table1 WHERE condition) As alias 
                From table1

    Rules:
                - Only scalar subquery are allowed to be used
*/

 /*
        Task:
            Show the productIDs, product Name, prices and total number of orders 
*/
   
SELECT 
    ProductID,
    Product AS ProductName,
    Price,
    (SELECT COUNT(OrderID)  FROM Sales.Orders ) AS TotalOrders  --Subquery !!! Only the scalar  subquery allowed Inside the select clause 
 From Sales.Products

 --                                 9-1-4    SUBQUERY IN JOIN CLAUSE:

 --     Used to prepare the data (filtring or aggregation) before joining it With other tables.
/*
    Task: 
         show all customer details and find the total orders of each customer.
*/
--  Main Query
Select *
From sales.Customers c
LEFT JOIN(
    Select 
        CustomerID,
        Count(*) TotalOrders
    From Sales.Orders  
    Group BY CustomerID) O
On C.CustomerID = O.CustomerID

 --                                 9-1-5   SUBQUERY IN WHERE CLAUSE:

 -- Used for complex filtering logic and makes query more flexible and dynamic.
/* 
    We have: 
        
            -  Comparsion Operators: < , > , = , != , >= , <=.

            -  Logical Operators: IN , ANY , ALL , EXISTS.
*/

/*
    Task:
        Find the products that have a price higher than the averge price of all products 
*/

SELECT  
    ProductID,
    Price
From Sales.Products
WHERE price > (Select avg(price) As AvgPrice From Sales.Products )

 --                                 9-1-5-1  SUBQUERY IN WHERE CLAUSE: IN OPERATOR

 -- Remeber IN Operator: Is for Filtring Data Using Liste Of Value 

 -- Checks whether a value matches any value from a list.
 /* 
    SYNTAX:
        SELECT column1, Column2,...
        FROM table1
        WHERE Column IN (SELECT column FROM table2 WHERE condition)

    RULES: 
        NO RULES it can have a Liste of multiple Values  
*/

/*
    Task : 
        Show the details of orders made by Customers in Germany
*/

    Select *,
    (Select Country From Sales.Customers)
    From Sales.Orders
    Where CustomerID in (
                            Select CustomerID 
                            From Sales.Customers 
                            Where Country = 'Germany'
                        ) 
   
   
/*
    Task : 
        Show the details of orders from Customers who are not in Germany
*/
   Select *
    From Sales.Orders
    Where CustomerID in (
                       Select CustomerID 
                       From Sales.Customers 
                       Where Country != 'Germany'
                       ) 


                       Select *  from Sales.Customers

 --                                 9-1-5-2  SUBQUERY IN WHERE CLAUSE: ANY | ALL OPERATOR
 /*
        ANY OPERATOR: 
            -   Checks if a value matches ANY value within a list.
            -   Used to Check if a vvalue is true for AT LEAST one of the value in a liste 
            -   SYNTAX:
                    SELECT column1, Column2,...
                    FROM table1
                    WHERE Column  <  ANY(SELECT column FROM table2 WHERE condition)
  */

  /* 
    Task : 
        Find female employees whose salaries are greater than 
        the salaries of any male employees.
    */

  Select 
    EmployeeID,
    FirstName,
    Gender,
    Salary
From Sales.Employees
Where Gender='F' AND Salary > ANY(
                    Select Salary 
                      from Sales.Employees 
                      where Gender = 'M'
                      );

     
 --     ALL OPERATOR : Checks a Value matches ALL values within a list.

 /* 
        Task:
            Find a female employees whose salaries are greater 
            than the salaries of all male employees.
*/

Select 
    EmployeeID,
    FirstName,
    Gender,
    Salary
From Sales.Employees
Where Gender='F' AND Salary > ALL(
                    Select Salary 
                      from Sales.Employees 
                      where Gender = 'M'
                      );

 --                                 9-1-6   NON-CORRELATED AND CORRELATED SUBQUERIES :
 /*
        NON-CORRELATED SUBQUERIES :
            -   A subqueries that can run independtly from the Main Query 
        CORRELATED SUBQUERY:
            -   A subquery that relays on values from The Main Query 
*/

/*
        Task: 
            Show all Customers details and find the total orders for each Customer.
*/

Select *,
    (
        Select 
            Count(*) 
        From Sales.Orders O 
        Where O.CustomerID = C.CustomerID
   ) As TotalOrders
From Sales.Customers c
 
--                          SUMMARY

/*
                                     +----------------------------------------------+-----------------------------------------------+
                                     |           NON-Correlated Subquery            |              Correlated Subquery              |
        +----------------------------+----------------------------------------------+-----------------------------------------------+
        |        Definition          |   Subquery is independent of main query      |    Subquery is dependant of main query        |
        +----------------------------+----------------------------------------------+-----------------------------------------------+
        |                            |   -   Executed once and its result its used  |   -     Executed for eache row processed by   | 
        |        Execution           |   by the main query                          |    the main query.                            |
        |                            |   -   Can be excuted on its Own.             |    -   Cant't be executed on its Own          |
        +----------------------------+----------------------------------------------+-----------------------------------------------+
        |        Easy To Use         |               Easier to read                 |       Harder to reade and more complixe       |
        +----------------------------+----------------------------------------------+-----------------------------------------------+
        |        Performance         |       Executed only once leads to better     |       Excuted multiple times leads to bad     |                                       |
        |                            |                  Performance                 |                    Performance                |
        +----------------------------+----------------------------------------------+-----------------------------------------------+
        |          Usage             |  Static Comparisons, Filtering with Constant |   Row-by-Row Comparaisons, Dynamic Filtering  |   
        +----------------------------+----------------------------------------------+-----------------------------------------------+        */

 --                                 9-1-7 CORRELATED SUBQUERIES EXISTS :

 /*
        DEF:
            Check if a subquery return any rows 
        SYNTAX:
                
                SELECT Column1, Column2, ...
                FROM Table2
                WHERE EXISTS( SELECT 1
                              FROM Table1
                              WHERE Table1.ID = Table2.ID  )
*/

/*
        Task:
            Show the details of orders made by Customers in Germany
*/
use SalesDB
Select  *
From Sales.Orders o
WHERE NOT EXISTS( Select 1
               From Sales.Customers c
               Where O.CustomerID = c.CustomerID And C.Country='Germany')

 --                                 9-1-8   SUBQUERIE SUMMARY :

 /*
        -   Query Inside another Query.
        -   Breaks complex query into smaller, manageable pieces.

        +   +--------------+
            |  USE CASES   |
            +--------------+
                |
                |--->   Creat Temporary Result set.
                |
                |--->   Prepare Data Befor Joining Tables.   
                |
                |--->   Dynamic and complexe Filttring.
                |
                |--->   Checlk the Existence of rows From  another Table. (EXISTS) 
                |
                |--->   Row By Row Comparison - Correlated Subquery - .
*/


--                9-2 COMMON TBALE EXPRESSION CTE:
/*
   -   DEF: 
        Temporary, named result set (Virtual table), that can be use multible times within your Query  
            to simplify nd organize complex query.

   -    CTE TYPES:                                                                             
                                                                                 +-------------------+ 
                                                                           |-----|   Standalone CTE  |     
                                                +-------------------+      |     +-------------------+
            +-------------------+      |--------|None-Recursive CTE |------|     
            |        CTE        |------|        +-------------------+      |     +-------------------+
            +-------------------+      |        +-------------------+      |-----|     Nested CTE    |
                                       |--------|    Recursive CTE  |            +-------------------+
                                                +-------------------+
*/


--                      9-2-1 NON-RECURSIVE  CTE:

--     DEF: Is excuted only once without any repetition.
--                               9-2-1-1 STANDALONE CTE:
/*
    Def: 
        Defined and used independently. Runs independently as it's self-contained and 
         doen't relay on other CTEs or requires .
    Syntax: 
       +----------------------------------+ 
       |     WITH CTE-Name AS             |
       |     (                            |               
       |         SELECT ...               |------------> CTE Query - CTE Definition -               
       |         FROM ...                 |           
       |         WHERE ...                |                   
       |     )                            |                                                 
       +----------------------------------+ 

       +----------------------------------+ 
       |     SELECT ....                  |         
       |     FROM CTE-Name                |------------> Main Query - CTE Usage -               
       |     WHERE ...                    |                                                  
       +----------------------------------+ 
*/
--          
   
 /*  
        Project:
            -   Step 1: Find the total sales per costomers
            -   Step 2: Find the last order Date per Customer.
            -   Step 3: Rank Customers based on totale Sales Per Costomers  -- ( See  9-2-1-2 Nested CTE)
            -   Step 4: Segment  Customers based on thier totale Sales   -- ( See  9-2-1-2 Nested CTE)
*/
                                                                                                                                                                    
   

--  Step 1:  Find the total sales per costomers     --                       +-----------+                                                     
WITH CTE_Total_Sales AS                             --                       | CTE-RULE  |              
(                                                   --             +---------+-----------+----------------------------------+                                                                      
    SELECT                                          --             |    You cannote use ORDER BY directly within the CTE    |                                              
        CustomerID,                                 --             +--------------------------------------------------------+                                        
        Sum(sales)  As TotalSales
    FROM Sales.Orders
    GROUP BY CustomerID
)
-- Step 2:  Find the last order Date per Customer.
,   CTE_last_Order_Date AS
(
    SELECT
        CustomerID,
        Max(OrderDate) As Last_Order
    FROM Sales.Orders
    Group by CustomerID
 )

--  Step 3: Rank Customers based on totale Sales Per Costomers  -- ( See  9-2-1-2 Nested CTE)
 ,CTE_Rank_Sales AS
 (
    SELECT 
        CustomerID, 
        TotalSales,
        RANK() over(Order By TotalSales DESC) As RankSales
    From CTE_Total_Sales
 )

--  Step 4: Segment  Customers based on thier totale Sales   -- ( See  9-2-1-2 Nested CTE)
, CTE_Segemnt_Salen AS 
(
    Select 
        CustomerID,
        Case 
            WHEN TotalSales > 100 THEN 'High'
            WHEN TotalSales > 50 THEN 'Medium'
            ELSE 'Low'
        END Customer_Segment 
    From CTE_Total_Sales
)

-- Main Query 
SELECT 
    C.CustomerID,
    C.FirstName,
    C.LastName,
    cts.TotalSales,     --  Step 1
    clo.Last_Order,      --  Step 2 
    CRS.RankSales,
    css.Customer_Segment
FROM Sales.Customers c
LEFT JOIN CTE_Total_Sales cts
ON cts.CustomerID = c.CustomerID 
LEFT JOIN CTE_last_Order_Date clo
ON clo.CustomerID = c.CustomerID
LEFT JOIN CTE_Rank_Sales CRS
ON CRS.CustomerID = C.CustomerID
LEFT JOIN CTE_Segemnt_Salen css
ON css.CustomerID = c.CustomerID

--                                   9-2-1-2 STANDALONE CTE: Nested CTE

/*
    +   DEF: 
        -   CTE inside another CTE
        -   A nested CTE uses the result of another CTE, so it can't run independently

    +   SYNATX:
+---------------------+-----------------------------------+
|                     |      WITH CTE-Name1 AS            |        
|                     |      (                            |                          
|    Standalone       |          SELECT ...               |-------> CTE QUERY - CTE Definition -                       
|       CTE           |          FROM ...                 |           
|                     |          WHERE...                 |                   
|                     |      )                            |                                       
+---------------------+-----------------------------------+
+---------------------+-----------------------------------+
|                     |      , CTE-Name2 AS               |        
|                     |      (                            |                          
|       NESTED        |          SELECT ...               |-------> CTE QUERY - CTE Definition -                       
|         CTE         |          FROM CTE-Name1           |           
|                     |          WHERE...                 |                   
|                     |      )                            |                                       
+---------------------+-----------------------------------+

+---------------------+-----------------------------------+
|                     |      SELECT ...                   |        
|     Main Query      |      FROM CTE-Name2               |-------> MAIN  QUERY  
|                     |      WHERE ...                    |                      
+---------------------+-----------------------------------+

*/

--                                  9-2-1-3 STANDALONE CTE: BEST PRACTICES
/*
        -   REMEMBER : Very Powerful But With Power Comes Great Responsibility
        -   Rethink and refactor Your CTEs Befor Starting A New One.
        -   Don't Use More Than 5 CTEs In One Query; Othrwise Your Code Will Be Hard To Understand and Maintain.
*/

--                          9-2-2   RECURSIVE CTE:

/*
        -   DEF: Self-refrencing Query that repeatedly processes data until a specific condition is met.
        +   SYNTAX:
             
                +------------------+----------------------------------+-------------------------------+          
                |                  |    WITH CTE-Name AS              |                               |
                |                  |     (                            |                               |              
                |                  |        SELECT ...                |           Anchor              |                          
                |                  |        FROM ...                  |            Query              |      
                |                  |        WHERE ...                 |                               |                  
                |    CTE QUERY     |----------------------------------+-------------------------------+                                                                             
                | -CTE Definition- |        UNION ALL                 |                                    
                |                  |----------------------------------+-------------------------------+                                 
                |                  |        SELECT ...                |                               |          
                |                  |        FROM CTE-Name             |          Recursive            |          
                |                  |        WHERE [Break Condition]   |            Query              |      
                |                  |    )                             |                               |
                +------------------+----------------------------------+-------------------------------+                    
                |    Main Query    |    SELECT ....                   |                                 
                |                  |    FROM CTE-Name                 |                                     
                |  - CTE Usage -   |    WHERE ...                     |                                           
                +------------------+----------------------------------+ */

/*  
        Task: 
             Generate a sequance of Numbers From 1 to 20
*/

WITH Series AS
(
    -- Anchor Query
    SELECT 
        1 AS num

    UNION all

    -- Recursive Query 
    SELECT 
        num + 1 
    FROM Series
    WHERE num + 1  < = 20
)
select *
 From Series ;

 /* 
    Task :
        Show the employee hierarchy by displaying each employee's level within the orginization
*/

With Hierarchy AS
(
-- Anchore Query 
    Select 
        EmployeeID,
        FirstName,
        ManagerID,
        1 As LevelOF
    From Sales.Employees
    Where ManagerID IS NULL
    UNION ALL
    -- Recursive Query
     Select 
       e.EmployeeID,
        e.FirstName,
        e.ManagerID,
        LevelOF +1
    From Sales.Employees e
    INNER JOIN Hierarchy ceh    
    ON e.ManagerID = ceh.EmployeeID
    
)

--Main Query
Select * 
From Hierarchy ;

--                          9-2-3  CTE SUMMARY:
/*
        -   Common Table Expression (CTE) is Temporary, named result set that can be used multiple times within the Query.
        +
            +----------------------+
            |       Advantages     |
            +----------------------+
                        |
                        |---->  Readibility: Breaks down Compex Queries into smaller Pieces. 
                        |
                        |---->  Modularity: Pieces are easy to manage, develop, and self-
                        |
                        |---->  Resulability: Reduce Redundancy in Query.
                        |
                        |---->  Recursive:Iternations & looping SQL

        -   Result of CTE is Like Table but can't be used from multible Queries.
        
        -   TIP: Don't creat more than 5 CTEs in one Query.


--                      9-3 VIEWS 


==> DATABASE STRUCTURE: 



                                          +--------------------+                                                                                                                     +--------------------+
                                     |--->|      DATABASE      |                                                                                                                |--->|         NAME       |
        +--------------------+       |    +--------------------+               +--------------------+                                              +--------------------+       |    +--------------------+
        |    SQL SERVER      |------>|                                    |--->|       SHCEMA       |                                        |---->|        COLUMNS     |------>|
        +--------------------+       |    +--------------------+          |    +--------------------+            +--------------------+      |     +--------------------+       |    +--------------------+
                                     |--->|      DATABASE      |--------->|                                 |--->|       TABLE        |----->|                                  |--->|      DATA TYPE     |
                                          +--------------------+          |    +--------------------+       |    +--------------------+      |     +--------------------+            +--------------------+
                                                                          |--->|       SCHEMA       |------>|                                |---->|        KEYS        | . . .
                                                                               +--------------------+       |                                      +--------------------+
                                                                                                            |
                                                                                                            |    +--------------------+            +--------------------+                                                       
                                                                                                            |--->|       VIEW         |----------->|       COLUMNS      | . . .
                                                                                                                 +--------------------+            +--------------------+
--       + DEFs.:                                                                                                           
                -   DATABASE SERVER: 
                        Stores, Manages, and provides access to databases for users or applications.

                -   DATABASE: 
                       Collection of informations that is stored in a structured way.

                -   SCHEMA: 
                        Logical layer that groups related objects together.

                 -  TABLE:
                        A place where data is stored and organized into rows and columns.

                -   VIEW:
                        Is a Virtual table that shows data without storing it physically.
                
       +  DDL( DATA DEFINITION LANGUAGE ):  
                        A set of commande that allows us to define and manage the structure of a database.
                        
                        +--------------------+
                        |         DDL        |
                        +--------------------+
                                 |
                                 |   +--------------------+
                                 |-->|       CREATE       |
                                 |   +--------------------+
                                 |
                                 |   +--------------------+
                                 |-->|        ALTER       |
                                 |   +--------------------+
                                 |
                                 |   +--------------------+
                                 |-->|        DROP        |
                                 |   +--------------------+
*/

--                      9-3-1 VIEW DEF.:
/*
        +   VIEW:
                -   Virtual Table based on the result set of a query, without storing the data in Database.
                -   VIEWS are presisted SQL queries in the database 
                -   Abstraction Layer between me and Real Data.

        +   VIEWS vs TABLES:

                 +--------------------------------------------+--------------------------------------------+
                 |                   VIEW                     |                    TABLE                   |                       
                 +--------------------------------------------+--------------------------------------------+
                 |            NO Persistance                  |             Persisted Data                 |
                 |--------------------------------------------+--------------------------------------------|
                 |            Easy To Maintain                |             Hard to Maintain               |
                 |--------------------------------------------+--------------------------------------------|
                 |              Slow Response                 |               Fast Reponse                 |
                 |--------------------------------------------+--------------------------------------------|
                 |               Read Only                    |                Read / Write                |
                 +--------------------------------------------+--------------------------------------------+                                          |                                            |


        +   VIEW vs CTE:

                +--------------------------------------------+--------------------------------------------+
                |                   VIEW                     |                    CTE                     |                       
                +--------------------------------------------+--------------------------------------------+
                |      Reduce Redundancy in Multi-Queries    |       Reduce Redundancy in 1 Query         |
                |--------------------------------------------+--------------------------------------------|
                |      Improve Resuability in Multi-Queries  |        Reduce Reusability in 1 Query       |
                |--------------------------------------------+--------------------------------------------|
                |             Presisted Logic                |         Temporary Logic -On the Fly        |
                |--------------------------------------------+--------------------------------------------|
                |       Need to Maintain - CREATE/DROP -     |        No Maintenance - Auto cleanup-      |
                +--------------------------------------------+--------------------------------------------+          

*/                          
                          
--                         9-3-2 VIEW USE CASE:
/*          
        -   Central Complex Query Logic:  T(Task II)
                Store central, Complex query logic in the database for access
                by multiple queries, reducing project complexity.
        -   Data Security: (Task III)
                Use views to enforce security and protect sensitive data,
                by hiding columns and/Or rows from tables.
        -   Flexibility & Dynamic:
                A SQL View provides flexibility and dynamic behavior 
                by acting as a virtual table that automatically reflects 
                changes in the underlying data.
        -   Multiple Languages:
                A SQL View supports multiple languages 
                by presenting the same data with translated or 
                localized labels based on user needs.
        -   Virtual Data Marts in DWH:
                A SQL View enables virtual data marts in a Data Warehouse 
                by logically organizing and exposing curated subsets of 
                warehouse data without physically duplicating it.
        
  */
  
--                         9-3-3 SQL VIEWS CREAT & UPDATE & DROP:
/*
            +   SYNATX:
                
                       +-------------------------------------------------------------+
                       |    CREATE VIEW VIEW-NAME AS                                 |
                       |    (                                                        |   
                       |        +----------------------------+                       |   
                       |        |   SELECT ...               |                       |
                       |        |   FROM ...                 |--> QUERY              |----------> DDL COMMANDE      
                       |        |   WHERE ...                |                       |   
                       |        +----------------------------+                       |   
                       |    )                                                        |    
                       +-------------------------------------------------------------+   

            +   Task I:
                    Find the Running total of sales for each month
*/
WITH CTE_Month_Summary AS(
    Select 
        DATETRUNC(month,OrderDate) OrderMonth,
        SUM(sales) Total_Sales, 
        COUNT(OrderID) countOrder,
        SUM(Quantity) TotalQuantity
    From Sales.Orders
    Group By DATETRUNC(Month, orderDate)
)
Select 
    
    OrderMonth,
    Total_Sales,
    CountOrder,
    TotalQuantity,
    Sum(Total_Sales) over(Order by orderMonth) as RunningTotal
From CTE_Month_Summary;

/*
    Notes:
        -   If a table or View iss created without specifying a schema, it defaults to the DBO.
        -   For Delete View we use Drop function hiq syntax is : 
                Drop View SchemaName.ViewName   .
        -   T-SQL:  Transact-Sql is an extention of SQL that adds programming features 
*/


-- file View Task : C:\Users\THINKPAD\Desktop\SQL\Task_Viewsql.sql

/*
    Task II: 
        Provide a View that combines details from orders, products, customers and employees 
*/
CREATE View Sales.v_Order_Details AS(
    Select 
        O.OrderID,
        O.OrderDate,
        p.Product,
        p.Category,
        O.ProductID,             
        o.CustomerID,
        Coalesce(c.FirstName, '') + ' ' + Coalesce(c.LastName, '') CustomerName,
        C.Country CustomerCountry,
        O.SalesPersonID,
        Coalesce(e.FirstName, '') + ' ' + Coalesce(e.LastName, '') SalesName,
        e.Department,
        O.Sales,  
        O.Quantity
    From Sales.Orders O
    Left Join Sales.Products p 
    ON p.ProductID = O.ProductID
    LEFT JOIN Sales.Customers c
    ON C.CustomerID = O.CustomerID
    LEFT JOIN Sales.Employees e
    ON e.EmployeeID = O.SalesPersonID
  )

/*
    Task III: 
        -   Provide a view for EU  sales Team.
        -   That combines details from all tables.
        -   And Excludes Data related to the USA
*/
CREATE VIEW Sales.v_Order_Details_EU AS(
 Select 
        O.OrderID,
        O.OrderDate,
        p.Product,
        p.Category,
        O.ProductID,             
        o.CustomerID,
        Coalesce(c.FirstName, '') + ' ' + Coalesce(c.LastName, '') CustomerName,
        C.Country CustomerCountry,
        O.SalesPersonID,
        Coalesce(e.FirstName, '') + ' ' + Coalesce(e.LastName, '') SalesName,
        e.Department,
        O.Sales,  
        O.Quantity
    From Sales.Orders O
    Left Join Sales.Products p 
    ON p.ProductID = O.ProductID
    LEFT JOIN Sales.Customers c
    ON C.CustomerID = O.CustomerID
    LEFT JOIN Sales.Employees e
    ON e.EmployeeID = O.SalesPersonID
    Where c.Country != 'USA'
    )

--                         9-3-4 SQL VIEWS SYNTAX:
/*        
            -   Virtual table based on result of Query without storing data.
            -   We use Views to presist complex SQL  Query in Database.
            -   Views are better than CTE - improves reusability in multiple Queries.
           
           +-----------+
           | USE CASES |
           +-----------+
                |
                |-->    Store Centrale complexe Business logic to be reused.
                |
                |-->    Hide complexitity by offering friendly views to users.
                |
                |-->    Data Security by hiding senstive rows + Column
                |
                |-->    Flexibilty & Dynamic 
                |
                |-->    Offer your objects in Multiple Languages.
                |
                |-->    Virtual layer (Data Marts) in Data Warehouses.



--                         9-4  CTAS & TEMP: 
    
        +----------------+
        |   Definitions  |
        +----------------+
                |                           +------------------------------------------------+
                |                           |   A table is a structured collection of data,  |
                |                       |-->|   similar to a spreadsheet or grid (Excel)     |
                |                       |   +------------------------------------------------+ 
                |                       |                                                          +-------------------+
                |                       |                                                      |-->|  CREAT / SELECT   |
                |   +--------------+    |                            +-----------------+       |   +-------------------+
                |-->|   DB TABLE   |--->|                        |-->| PERMANENT TABLE |------>|   
                |   +--------------+    |   +--------------+     |   +-----------------+       |   +-------------------+
                |                       |-->| TABLE TYPES  |-----|                             |-->|        CTAS       |    
                |                           +--------------+     |   +-----------------+           +-------------------+
                |                                                |-->| TEMPORARY TABLE |
                |                                                    +-----------------+
                |

*/
--             9-4-1 PERMANENT TABLE
--                              9-4-1-1   CREATE/INSERT vs    CTAS:
/*
            CREATE/INSERT: 
                1.  Create | Define the structure of table.
                2.  Insert | Insert Data into the table.
            CTAS -Create Table As Select- :
                -   Creat a New Table Based On the result of an SQL query
                
*/
--                              9-4-1-2   CTAS vs  VIEWS:
/*
            +---------------------------------------------------+---------------------------------------------------+
            |                       CTAS                        |                       VIEWS                       |
            +---------------------------------------------------+---------------------------------------------------+
            |   .  Create A physical Table                      |   .   Create a Virtual Table                      |
            +---------------------------------------------------+---------------------------------------------------+
            |   .   Stores actual data at creation Time         |   .   Stores Only the Query, not the Data         |
            +---------------------------------------------------+---------------------------------------------------+
            |   .  Fast for reads                               |   .   Depends on query                            |
            +---------------------------------------------------+---------------------------------------------------+
            |   .  Create A physical Table                      |   .   Create a Virtual Table                      |
            +---------------------------------------------------+---------------------------------------------------+
            |   .   Acs a snapshot                              |   .   Always reflects real-time data.             |
            +---------------------------------------------------+---------------------------------------------------+
            |   .   Use Storage                                 |   .   No Storage                                  |
            +---------------------------------------------------+---------------------------------------------------+
*/
--                              9-4-1-3   CTAS SYNTAx:
        
/*
                                                +-------------------------+
                                                |     CREATE / INSERT     |
                                                +-------------------------+
                                                            |
                                                            |
                                    +----------------------------------------------+
                                    |   CREATE TABLE Table_Name                    |
                                    |   (                                          |
              DDL Statement ------->|       ID INT,                                |
                                    |       Name VARCHAR (50)                      |
                                    |   )                                          |
                                    +----------------------------------------------+


                                                +-------------------------+                     
                                                |         CTAS            |
                                                +-------------------------+
                                                            |
                                                            |
                                   +------------------------+------------------------+
                                   |                                                 |
                                   v                                                 v
               +----------------------------------------------+   +-----------------------------------------------+
               |   CREATE TABLE NAME AS                       |   |                                               |
               |   (                                          |   |                                               |  
   DDL    ---->|      +-----------------------+               |   |     SELECT ...                                |
Statement      |      |    SELECT ...         |               |   |     INTO New_Table                            |
Query  --------+----->|    FROM ...           |               |   |     FROM ..                                   | 
               |      |    WHERE ..           |               |   |     WHERE                                     |
               |      +-----------------------+               |   |                                               |                              
               |   )                                          |   |                                               |
               +----------------------------------------------+   +-----------------------------------------------+
               |           MYSQL | Postgres |Oracle           |   |                   SQL Server                  |
               +----------------------------------------------+   +-----------------------------------------------+
*/  

--                              9-4-1-4 CTAS USE CASE :
    
--                                  9-4-1-4-1 CTAS USE CASE: OPTIMIZE PERFORMANCE

--          e.g:
use SalesDB
-- We check if the table is already in  Our DataBase

IF OBJECT_ID('Sales.MonthlyOrders', 'U' ) IS NOT NULL               -- 'U': User defained Table

        -- For deelete Table we use Drop 
    Drop Table Sales.MonthlyOrders
GO 
    Select   
        DATENAME(Month, OrderDate) MonthOrder,
        Count(OrderID)  TotalOrders
    INTO Sales.MonthlyOrders
    FROM Sales.orders
    Group By DATENAME(Month, OrderDate)

-- Show the result
Select * From Sales.MonthlyOrders

--                                  9-4-1-4-2 CTAS USE CASE: CREATING A SNAPSHOT
/*
        -   Point-in-time capture: Preserves historical data for auditing or analysis.

        -   High performance:   than inserting row-by-row.

        -   Data stability: Snapshot data does not change even if the source table is updated.

        -   Safe experimentation: Allows testing and analysis without affecting production data.

        +   Typical Use Cases:
                
                 -  End-of-day / end-of-month reporting
                 -  Financial or regulatory audits
                 -  Data validation before transformations
                 -  Backup before major data updates
                 -  Machine learning or analytical baselines

*/
--                                  9-4-1-4-3 CTAS USE CASE: PHYSICAL DATA MARTS IN DWH

/*
             Preisting the Data Marts Of a DWH improves the speed of data retrieval compared to using Views.

           
*/
           
--                      9-4-2 TMP -TEMPORARY TABLES- :
/*
        DEFs:
            -   TMP Tables Stores intermidiate result in temporary storage within the database during the session.
            -   Session is the time between connecting to end disconnecting from the database.
            -   Tha database wil drop all temporary tables once the session ends.
        
        USE CASE:
            -   Intermediate Results
            -   Automatic cleanup of data after session ends.
        SYNTAX:

                +-----------------------------------------------+
                |                                               |
                |     SELECT ...                                |
                |     INTO #New_Table                           | We just add '#' for the begin of the name of table.
                |     FROM ..                                   | 
                |     WHERE                                     |
                |                                               |                              
                |                                               |
                +-----------------------------------------------+
                |                   SQL Server                  |
                +-----------------------------------------------+

 
      E.g :
 */
 use SalesDb
 Select 
    *
INTO #tmpOrders
From Sales.Orders       -- Schema for tempOrders : DB/DB systeme/Tempdb/tmp Tables/dbo.#tmpOrders

Select *
From dbo.#tmpOrders


--                  9-4-3  SUBQUERY vs CTE vs TMP vs CTAS vs VIEW
/*  
                            
                              +-------------------+-------------------+-------------------+-------------------+-------------------+
                              |      SUBQUERY     |         CTE       |       TEMP        |        CTAS       |       VIEWS       |
          +-------------------+-------------------+-------------------+-------------------+-------------------+-------------------+
          |      STORAGE      |                MEMORY                 |                 DISK                  |     No Storage    |             
          +-------------------+-------------------+-------------------+-------------------+-------------------+-------------------+
          |     LIFE TIME     |                           TEMPORARY                       |                PERMANENT              |
          +-------------------+-------------------+-------------------+-------------------+-------------------+-------------------+
          |   WHEN DELETED    |              END OF QUERY             |  END OF SESSION   |              DDL - DROP               |             
          +-------------------+-------------------+-------------------+-------------------+-------------------+-------------------+
          |       SCOPE       |             SINGLE QUERY              |                         MULTI QUERY                       |             
          +-------------------+-------------------+-------------------+-------------------+-------------------+-------------------+
          |                   |      LIMITED      |      LIMITED      |      MEDIUM       |                  HIGH                 |
          |    REUSABILITY    |                   |    Multi PLACES - |   Multi Queries   |                                       |
          |                   | 1 PLACE - 1 QUERY |      1 QUERY      |  During Session   |              MULTI QUERIES            |
          +-------------------+-------------------+-------------------+-------------------+-------------------+-------------------+
          |      UP2DATE      |                  YES                  |                  NO                   |        YES        |             
          +-------------------+-------------------+-------------------+-------------------+-------------------+-------------------+

*/

--                        9-5   STORED PROCEDURE

--                              9-5-1   STORED PROCEDURE: BASICS
/*

           +    DEF:
                    -   A stored procedure in SQL is a precompiled set of SQL statements that is saved 
                        in the database and can be executed repeatedly by name.

           +    USE CASES:

                    -   Reusability – write once, use many times
                    -   Performance – precompiled → faster execution
                    -   Security – users can execute it without direct table access
                    -   Maintainability – business logic is centralized
                    -   Supports logic – IF, WHILE, variables, parameters

           +    KEY FEATURES:
           
                    -   Accepts input parameters
                    -   Can return result sets
                    -   Can return output parameters
                    -   Can contain transactions and error handling

            +   Stored Procedure vs SQL Query:
                
                       +--------------------------------------------+-------------------------------------------+
                       |              Stored Procedure              |                 SQL QUERY                 |
                       +--------------------------------------------+-------------------------------------------+
                       |  Stored and reused                         |   Written and executed each time          |
                       +--------------------------------------------+-------------------------------------------+
                       |  More secure                               |   Less secure                             |
                       +--------------------------------------------+-------------------------------------------+
                       |  Faster & optimized                        |   Slower for copmplex Logic               |
                       +--------------------------------------------+-------------------------------------------+
                       |  Supports logic                            |   No business Logic                       |
                       +--------------------------------------------+-------------------------------------------+
            +   SYNTAX:

                    +------------------------------------------------------------------+
                    |                                                                  |
                    |    CREATE PROCEDURE ProcedurName AS                              |
                    |                                                                  |
                    |    BEGIN                                                         |
                    |                                                                  |-----------> Stored Procedure 
                    |        -- SQL STATEMENT GO HERE                                  |                Definition 
                    |                                                                  |
                    |    END                                                           |
                    |                                                                  |
                    +------------------------------------------------------------------+


                    +------------------------------------------------------------------+
                    |                                                                  | 
                    |   EXEC ProcedureName                                             |-----------> Stored Procedure 
                    |                                                                  |             Execution (Call)
                    +------------------------------------------------------------------+


            +   E.G:
                    -   Step 1: Write a Query For Us Customers Find the Total Number of Cutomers and the Average Score 
                    -   Step 2: Turning the Query Into a Stored Procedure
*/
use SalesDB
CREATE PROCEDURE GetCustomerSummary AS
BEGIN
    Select
        Count(CustomerID) TotalCustomers ,
        Avg(Score) as AvgScore   
    From Sales.Customers 
    Where Country = 'USA'
END 
-- This is SCHEMA FOR Our Procedure:    DB/SalesDB/Programmabilty/StoredProcedure/dbo.GetCustomerSummary

-- Step 3: Excute the storred Procedure

EXEC GetCustomerSummary

--                              9-5-2 STORED PROCEDURE: PARAMETRS
/*
        Task:
            For Germany Customers find the totale Number of Customers and the aVerge Score
*/
Alter PROCEDURE GetCustomerSummary @Country NVARCHAR(50) = 'usa'     -- we use alter for updut our Procedure
                                                                  -- We can also give a value For default to our parametr like USA in our e.G
AS 
Begin 
    Select
        Count(CustomerID) TotalCustomers ,                      --  Avoid Repitition:
        Avg(Score) as AvgScore                                  --                If you repeted code in your project it's
    From Sales.Customers                                        --                      a sign that your code can be improved 
    Where Country = @Country
END 
 Exec GetCustomerSummary 
 


--                              9-5-3 STORED PROCEDURE: MULTIPLE STATEMENTS
/*
    Task: 
        Find the Totale Nr. of orders and Totale Sales
*/
Alter PROCEDURE GetCustomerSummary @Country NVARCHAR(50) = 'usa'     -- we use alter for updut our Procedure
                                                                  -- We can also give a value For default to our parametr like USA in our e.G
AS 
Begin 
    Select
        Count(CustomerID) TotalCustomers ,                      --  Avoid Repitition:
        Avg(Score) as AvgScore                                  --                If you repeted code in your project it's
    From Sales.Customers                                        --                      a sign that your code can be improved 
    Where Country = @Country;           -- Note: Add SemiColone  ; at the end of each SQL Statement.
    Select 
    Count(OrderID) TotaleOrders,
    Sum(Sales) TotalSales
    From SaleS.orders   O
    JOIN sales.Customers    c
    ON c.CustomerID=O.CustomerID
    Where C.Country = @Country
END 

Exec GetCustomerSummary         -- We Will have Two Result Cause of two Queries 



--                              9-5-4 STORED PROCEDURE: VARIABLES
/*
        DEF: 
                -   Variables is Placeholder used to store values to be used later in the procedure.
                -   Parametrs pass values into a stored procedure or return values back to the caller.
                -   Variables temporarily store and manipulate data during its execution.
        Task: 
                +   Print variable that content :
                    -   Total Customers From Germany: 2
                    -   Averge Score from Germany: 425                   

*/


Alter PROCEDURE GetCustomerSummary @Country NVARCHAR(50) = 'usa'     
AS 

Begin                                                   --      Form make a Variables we have 3 Steps:


DECLARE @TotaleCustomers INT, @avgScore FLOAT;          --      Step 1: Declare Variables
    Select                                                          
        @totaleCustomers = Count(*) ,                   --      Step 2: Assign Values to Variables                   
        @avgScore = Avg(Score)                               
    From Sales.Customers                                         
    Where Country = @Country;
    
    PRINT 'Total Customers From ' + UPPER(@Country) + ' : ' +Cast (@TotaleCustomers AS NVARCHAR ) ;  --      Step 3: Use Variables 
    PRINT 'Averge Score From  ' + UPPER( @Country )  + ' : ' + CAST ( @avgScore AS NVARCHAR);

    Select 
    Count(OrderID) TotaleOrders,
    Sum(Sales) TotalSales
    From SaleS.orders   O
    JOIN sales.Customers    c
    ON c.CustomerID=O.CustomerID
    Where C.Country = @Country
END 

Exec GetCustomerSummary  @Country = 'Germany'


--                              9-5-5 STORED PROCEDURE: CONTROL FLOW -IF ELSE-

/*
    Task:
        -   Handle a nulls in the procedure GetCustomerSummary before aggregating to nesure accurate results 

        _ Controle Flow :
                                                         /\ 
                                                        /  \
                                                       /    \            YES
                                          . Start ___ /Value \________________________
                                                      \  is  /                        |
                                                       \NULL/                         |   
                                                        \  /                          |       
                                                         \/                           v     
                                                         |                     +---------------+
                                                         |                     | Update To Zero|
                                                   NO    |                     +---------------+
                                                         |                            |   
                                                         |                            |           
                                                         v                            |
                                                      . END  <------------------------+

*/
Select * From Sales.Customers
Select * From Sales.Orders
Alter PROCEDURE GetCustomerSummary @Country NVARCHAR(50) = 'usa'     
AS 

Begin                                                   


DECLARE @TotaleCustomers INT, @avgScore FLOAT;  
-- Prepare & Cleanup Data
IF   Exists (Select 1 From Sales.Customers Where Score IS NULL And Country= @Country)                                                           -- Condition: Check if there are any Nulls in Scores 
BEGIN 
    Print('Updating Null Score to 0 ')
    UPDATE Sales.Customers
    SET Score= 0 
    Where Score Is NULL and Country=@country;
END
ELSE
BEGIN 
    Print('No Nulls Score Founde ')

END;

    Select                                                          
        @totaleCustomers = Count(*) ,                                  
        @avgScore = Avg(Score)                               
    From Sales.Customers                                         
    Where Country = @Country;
    
    PRINT 'Total Customers From ' + UPPER(@Country) + ' : ' +Cast (@TotaleCustomers AS NVARCHAR ) ; 
    PRINT 'Averge Score From  ' + UPPER( @Country )  + ' : ' + CAST ( @avgScore AS NVARCHAR);

    Select 
    Count(OrderID) TotaleOrders,
    Sum(Sales) TotalSales
    From SaleS.orders   O
    JOIN sales.Customers    c
    ON c.CustomerID=O.CustomerID
    Where C.Country = @Country
END 

Exec GetCustomerSummary  @Country = 'USA'

--                              9-5-6 STORED PROCEDURE: ERROR HANDLING -TRY CATCH-
/*
        SYNTAX:
                    BEGIN TRY 
                        -- SQL Statements that might Cause en error
                    END TRY
                     
                    BEGIN CATCH 
                        -- SQL Statements to Handle The Error
                    END CATCH 

*/

Alter PROCEDURE GetCustomerSummary @Country NVARCHAR(50) = 'usa'     
AS 

Begin 
BEGIN TRY
DECLARE @TotaleCustomers INT, @avgScore FLOAT;  
-- Prepare & Cleanup Data
IF   Exists (Select 1 From Sales.Customers Where Score IS NULL And Country= @Country)                                                           -- Condition: Check if there are any Nulls in Scores 
BEGIN 
    Print('Updating Null Score to 0 ')
    UPDATE Sales.Customers
    SET Score= 0 
    Where Score Is NULL and Country=@country;
END
ELSE
BEGIN 
    Print('No Nulls Score Founde ')

END;

    Select                                                          
        @totaleCustomers = Count(*) ,                                  
        @avgScore = Avg(Score)                               
    From Sales.Customers                                         
    Where Country = @Country;
    
    PRINT 'Total Customers From ' + UPPER(@Country) + ' : ' +Cast (@TotaleCustomers AS NVARCHAR ) ; 
    PRINT 'Averge Score From  ' + UPPER( @Country )  + ' : ' + CAST ( @avgScore AS NVARCHAR);

    Select 
    Count(OrderID) TotaleOrders,
    Sum(Sales) TotalSales,
    1/0 dividebZero
    From SaleS.orders   O
    JOIN sales.Customers    c
    ON c.CustomerID=O.CustomerID
    Where C.Country = @Country
END TRY 
BEGIN CATCH 
    Print('AN error occured.')
    Print('Error Message: ' +Error_Message())
    Print('Error Number : ' +CAST(Error_NUMBER() AS VARCHAR))
    Print ('Error Line: '+ Cast(Error_Line() AS VARCHAR)) 
    Print ('Error Procedure: ' +Error_Procedure())
END CATCH
END 
GO 
Exec GetCustomerSummary  @Country = 'USA'

--                              9-5-7 STORED PROCEDURE: STYLING
/*
        Style Tips: 
                    -   Use TABS inside BEGIN and END Bloks.
                    -   Include comments to explain the purpoe of code Section
*/


Alter PROCEDURE GetCustomerSummary @Country NVARCHAR(50) = 'usa'     
AS 

Begin 
    BEGIN TRY
        DECLARE @TotaleCustomers INT, @avgScore FLOAT;  

        --=================================
        -- Step 1: Prepare & Cleanup Data
        --=================================
        IF   Exists (Select 1 From Sales.Customers Where Score IS NULL And Country= @Country)                                                           -- Condition: Check if there are any Nulls in Scores 
        BEGIN 
            Print('Updating Null Score to 0 ')
            UPDATE Sales.Customers
            SET Score= 0 
            Where Score Is NULL and Country=@country;
        END
        ELSE
        BEGIN 
            Print('No Nulls Score Founde ')

        END;

        
        --====================================
        -- Step 2: Generating Summary Reports 
        --=====================================
        -- Calculate Totale Customers and Average score for specific Country
        Select                                                          
            @totaleCustomers = Count(*) ,                                  
            @avgScore = Avg(Score)                               
        From Sales.Customers                                         
        Where Country = @Country;
    
        PRINT 'Total Customers From ' + UPPER(@Country) + ' : ' +Cast (@TotaleCustomers AS NVARCHAR ) ; 
        PRINT 'Averge Score From  ' + UPPER( @Country )  + ' : ' + CAST ( @avgScore AS NVARCHAR);
        -- Calculate Totale Number of Orders an dtotale Sales for specific Country  
        Select 
            Count(OrderID) TotaleOrders,
            Sum(Sales) TotalSales,
            1/0 dividebZero
        From SaleS.orders   O
        JOIN sales.Customers    c
        ON c.CustomerID=O.CustomerID
        Where C.Country = @Country
    END TRY 
    BEGIN CATCH 
    --================================
    --  Error Handling 
    --================================
        Print('AN error occured.')
        Print('Error Message: ' +Error_Message())
        Print('Error Number : ' +CAST(Error_NUMBER() AS VARCHAR))
        Print ('Error Line: '+ Cast(Error_Line() AS VARCHAR)) 
        Print ('Error Procedure: ' +Error_Procedure())
    END CATCH
END 
GO 
Exec GetCustomerSummary  @Country = 'USA'

--                  9-4 TRIGGERS 
/*
    DEF:
        Speciale Stored procedure (Set of Statements) tha automatically 
        runs in response to a specific event on a table or view.
    SYNTAX: 
           +--------------------------------------------------+ 
           |                                                  | 
           |     CREATE TRIGGER TriggerName ON TableName      |         
   WHEN >--+---> AFTER  INSERT, UPDATE, DELETE                |
           |     BEGIN                                        |
           |                                                  |
   WHAT >--+--->    -- SQL STATEMENT GO HERE                  |
           |                                                  |
           |     END                                          |
           |                                                  |
           +--------------------------------------------------+ 
            
    TRIGGER TYPES:
                                                                  +-------------------+
                                                                  |   Trigger Types   |
                                                                  +---------+---------+
                                                                            |
              +-----------+                                                 |
              |   AFTER   |----+                                            |
              +-----------+    |            +-------------------------------|-------------------------------+
             Run After Event   |            |                               |                               |
                               |            V                               V                               V
                               |       +-----------+                  +-----------+                   +-----------+
                               |<======|    DML    |                  |    DDL    |                   |  LOGGON   | 
                               |<======| Triggers  |                  |  Triggers |                   +-----------+ 
                               |       +-----------+                  +-----------+
                               |    -   INSERT                      -   CREATE
                               |    -   UPDATE                      -   ALTER
                               |    -   DELETE                      -   DROP
           +--------------+    |
           |  INSTEAD OF  |----+
           +--------------+
           Runs During Event

 + USE CASE : LOGGING                                                                                                                               */
    
-- Step 1: Create Log Table
Use SalesDB
CREATE TABLE Sales.EmployeeLogs (
    LogID INT IDENTITY(1,1) PRIMARY KEY,
    EmployeeID INT,
    LogMessage VARCHAR(255),
    LogDate DATE
) 

-- Step 2: Create Trigger On Employees Table:
CREATE TRIGGER trg_AfterInsertEmployee ON Sales.Employees
AFTER INSERT 
AS 
BEGIN
    INSERT INTO Sales.EmployeeLogs ( EmployeeID, LogMessage, LogDate) 
    SELECT 
        EmployeeID,
        'New Employee Added =' + cast (EmployeeID As varchar(10)),
        GETDATE() 
    FROM INSERTED 

 /* 
INSERTED: Virtual table that holdSelect * From Sales.EmployeeLogs
s a copy
         of the rows that are being inserted 
         into the target table  */
END

-- Step 3: Insert New Data Into Employees
Select * From Sales.Employees
INSERT INTO Sales.Employees
VALUES 
(6, 'Maria', 'Doe', 'HR', '1997-09-10', 'M', 8000, 3)

-- Step 4: Check the Trigger
SELECT * FROM Sales.EmployeeLogs


/*
========================================================================================================================================================================

	                                                        CHAPTER 10: PERFORMANCE OPTIMIZATION 

========================================================================================================================================================================                                */

--                          10-1 SQL INDEXES: 
/* 
        DEF:
            Index is Data Structure provides quick access to data ,
            optimizing the speed of your queries.

        INDEX TYPES:
              + Structure:
                    -   Clustered Index
                    -   Non-Clustered index
              + Storage:
                    -   Rowstore Index
                    -   Columnstore Index 
              + Functions:
                    -   Unique Index
                    -   Filtred Index
               (Trade-Off: Some Indexes  are better for Reading, 
                others for writing performance.)

        SYNTAX:
                       Default is NON-CLUSTERED
                                   |
                                   V
                       |__________________________|
                CREATE [CLUSTERED | NON-CLUSTERED ] INDEX index_name ON table_name ( Column1, Column2, ...)  
              
        INFO:
            A Primary Key (PK) automatically creates a Clustered index by default.
                        

        HEAP STRUCTURE:
                Page: The Smallest unit of data storage in a database (8kb)
                      It Stores  anything (Data, MetaData, Indexes, etc.)
                      Types:
                        -   Data Page
                        -   Index Page
                Heap = Table WITHOUT Clustered Index
                
                Table Full Scan: Scans the entire table page by page and row by row
                                  Searching for Data.
      
*/

--                                  10-1-1 SQL INDEXES: CLUSTRED INDEX
/*
        B-TREE (BALANCE TREE):
            Hierarchical structure storing data at leaves, 
            to help quickly locate data.

                                                        +---------------+
                                                        |   Root Node   | Index Page 
                                                        +-------+-------+   Key --> Value(pointer to Intermediate Node)
                                                                |           
                                                                V
                                  +-----------------------------+-------------------------------+
                                  |                                                             |
                                  V                                                             V
                           +---------------+                                             +---------------+
                           |  Intermediate |                INDEX PAGE                   |  Intermediate |
                           |      Node     |    Key --> Value(pointer to data page)      |      Node     |
                           +---------------+                                             +---------------+
                                  |                                                               |
                                  V                                                               V
                 +------------------------------------+                         +------------------------------------+
                 |                                    |                         |                                    |
                 V                                    V                         V                                    V
        +-----------------+                   +-----------------+       +-----------------+                  +-----------------+   
        |    Leaf Noode   |     Data Pages    |    Leaf Noode   |       |    Leaf Noode   |    Data Pages    |    Leaf Noode   |
        +-----------------+                   +-----------------+       +-----------------+                  +-----------------+   
        

        Index Page :    
            It Stores key Values (Pointers) to another page.
            It doesn't Store the actual Rows 
        E.G:                                                                                                                                            */

-- Create a copy of Customers table without index 
Select *
INTO Sales.DBCustomers 
From Sales.Customers                                                                                                                                    
                                                            --   +--------------+                                                                         
                                                            --   |     RULE     |
-- Create Clusterd index                                    --   |--------------+----------------------------------------------+                                                                                       
CREATE CLUSTERED INDEX idx_DBCustomers_CustomerID           --   |  Only ONE Clustered index can be created per table          |                                                                 
ON Sales.DBCustomers(CustomerID)                            --   +-------------------------------------------------------------+                                               
                                                                                                                                

--                                  10-1-2 SQL INDEXES: NON-CLUSTRED INDEX
/*
        A Non-Clustered Index is an index that is stored separately from 
        the table data and helps SQL find rows quickly without changing 
        the physical order of the table.

        E.g :
            
*/ 

CREATE NONCLUSTERED INDEX idx_DBCustomers_LastName
ON Sales.DBCustomers(LastName)

CREATE INDEX idx_DBCustomers_FirstName
ON Sales.DBCustomers(FirstName)



--                                  10-1-3 SQL INDEXES: CLUSTRED INDEX  vs NON-CLUSTRED INDEX
/*
        +-----------------------------+------------------------------------------+------------------------------------------+
        |           Feature           |              Clustered Index             |            Non-Clustered Index           |   
        +-----------------------------+------------------------------------------+------------------------------------------+
        |   Definition                | Physically sorts and stores rows         |  Seperate Structure with pointers        |
        |                             |                                          |  to the data                             |
        +-----------------------------+------------------------------------------+------------------------------------------+
        |   Data Storage              | Physically sorts and stores table data   |  Stored separately from table data       |
        +-----------------------------+------------------------------------------+------------------------------------------+
        |   Physical order of rows    | Yes (Data Is ordered)                    |  NO (data order unchanged)               |
        +-----------------------------+------------------------------------------+------------------------------------------+
        |   Number per table          | Only one                                 |  Multiple allowed                        |
        +-----------------------------+------------------------------------------+------------------------------------------+
        |   Default with PRIMARY KEY  | Yes (by Default)                         |  NO                                      |
        +-----------------------------+------------------------------------------+------------------------------------------+
        |   Speed for SELECT          | Very Fast                                |  Fast                                    |
        +-----------------------------+------------------------------------------+------------------------------------------+
        |   INSERT / UPDATE / DELETE  | Slower (data reordering)                 |  Slower (index maintenance)              |
        +-----------------------------+------------------------------------------+------------------------------------------+
        |   Disk space usage          | Less (data  + index together )           | More ( extra index Storage)              |
        +-----------------------------+------------------------------------------+------------------------------------------+
        |   Row Locator               | Actual dataa row                         |  Clustered key or RID                    |
        +-----------------------------+------------------------------------------+------------------------------------------+
        |   Bested Used foor          | Range queries, primary keys              |   Searches, Joins, filtering             |
        +-----------------------------+------------------------------------------+------------------------------------------+
        |   Table type witout it      | Heap Table                               |  Can exist on Heap or clustered tables   |
        +-----------------------------+------------------------------------------+------------------------------------------+
        |   Use Cases                 | .   Unique Column                        |  .   Columns frequently used in search   |
        |                             | .   Not frequently modified Column       |      and joins                           |
        |                             | .   Improve range query performance      |  .   Exact match Queries                 |
        +-----------------------------+------------------------------------------+------------------------------------------+
*/

--                                  10-1-4 SQL INDEXES: COMPOSITE INDEX
/*
    DEF: 
        It's index that have multible columns inside the same index  
    E.G:            */

SELECT *
FROM Sales.DBCustomers  
WHERE country='USA' 
CREATE Index idx_DBCustomers_CountryScore 
ON Sales.DBCustomers (Country, Score)                   --      +--------------+
                                                        --      |  !! RULE !!  |                                    
                                                        --      +--------------+----------------------------------------------------+                        
                                                        --      |   The Columns of index order must match the order in your Query   |
                                                        --      +-------------------------------------------------------------------+
/*          +-----------------------+
        -   | LEFTMOST PREFIXE RULE |
            +-----------------------+-------------------------------------------------------------------+
            |   Index works only if your query filters start from the first column in the index and     |
            |   follow its order.                                                                       |
            +-------------------------------------------------------------------------------------------+                                                               */

SELECT *
FROM Sales.DBCustomers  
WHERE Score > 500

--                                  10-1-5 SQL INDEXES: COLUMNSTORE INDEX
/*
    DEF: 
        A Columnstore Index is a type of index in SQL (especially in SQL Server) 
        that stores data column by column instead of row by row. 
        It is designed for analytics, reporting, and data warehouse workloads.
        -   Rowstore index ➜ stores full rows together
        -   Columnstore index ➜ stores each column separately
    COLUMNSTORE INDEX vs ROWSTORE INDEX:
                                            
                                +---------------------------------------------------------------------------------------+
                                |            Rowstore Index              |                 Columnstore Index            |          
        +-----------------------+----------------------------------------+----------------------------------------------+
        |     Defintion         |   Organizes and stores data row by row |  Organizes and store data column by Column   |
        +-----------------------+----------------------------------------+----------------------------------------------+
        |  Storage Efficiency   |   Less Efficient in storage            |  Highly efficient with Compression           |
        +-----------------------+----------------------------------------+----------------------------------------------+
        |     Read/ Write       |   Fair Speed for read                  |  Fast read performance                       |
        |    Optimazation       |   & write operations                   |  Slow write performance                      |
        +-----------------------+----------------------------------------+----------------------------------------------+
        |   I/O Efficiency      |  Lower (retrieves all Columns)         |  Higher (retrieves specific Columns)         |
        +-----------------------+----------------------------------------+----------------------------------------------+
        |                       |  OLTP (Transactional)                  |  OLAP (Analytical)                           |
        |     Best For  ...     |  Commerce, banking, Financial systemes |  Data Warehouse, Business intelligence,      |
        |                       |  order processing                      |  Reporting, Analytics                        |
        +-----------------------+----------------------------------------+----------------------------------------------+
        |                       |   -   High-Frequency transaction       |  -   Big Data Analytics                      |
        |      Use Case         |   applications                         |  -   Scanning of large datasets              |
        |                       |  -    Quick access to complete records |  -   Fast aggregation                        |
        +-----------------------+----------------------------------------+----------------------------------------------+
     
     
     SYNTAX:                                   +----------------+
                                               |  Default  is   |
                                               |    ROWSTORE    |
                                               +________________+
            +-------------------------------------------|---------------------------------------+
            |                                           v                                       |
            |   CREATE [CLUSTERED | NONCLUSTERED] [COLUMNSTORE] INDEX index_name                |
            |           ON table_name(column1, Column2, ...)                                    |
            |                                                                                   |            
            +-----------------------------------------------------------------------------------+
                                   
                                    
            +---------------+      |    CREATE NONCLUSTORED INDEX IX_Customers_Country ON Customers (Country)             
            |   Rowstore    |----->| 
            +---------------+      |    CREATE CLUSTERED INDEX IX_Customers_ID  Customers (ID) 

                                               
            +---------------+      |    CREATE NONCLUSTORED COLUMNSTORE INDEX IX_Customers_Country ON Customers (Country)             
            |  Columnstore  |----->| 
            +---------------+      |    CREATE CLUSTERED COLUMNSTORE INDEX IX_Customers_ID  Customers   -- !!!! Not Allowed to use Columns


            +-------------+
            |   RULES     |
            +-------------+---------------------------------------------------------+
            |       .   You CAN'T Specific Columns in Clustered Index Columnstore   |
            +-----------------------------------------------------------------------+
        
     E.G:
        */

-- Create a Clustered  Columnstore Index : 
CREATE CLUSTERED COLUMNSTORE INDEX idx_DBCustomers_CS
ON Sales.DBCustomers

DROP Index [idx_DBCustomers_CustomerID] ON Sales.DBCustomers    -- We Delete this one because we must only have one Clustered index in Table 


-- Create a nonClostered columnstore Index 
CREATE NONCLUSTERED COLUMNSTORE INDEX idx_DBCustomers_CS_First_Name
ON Sales.DBCustomers(FirstName)
--  NB: We have to dalete the previous Columnstore for create another one.


USE AdventureWorksDW2022
-- HEAPE
SELECT * 
INTO FactInternetSales_HP
FROM FactInternetSales

-- ROWSTORE
SELECT * 
INTO FactInternetSales_RS
FROM FactInternetSales

CREATE CLUSTERED INDEX idx_FactInternetSales_RS_PK      --AS a default it will be Rowstore
ON FactInternetSales_RS (SalesOrderNumber, SalesOrderLineNumber)


-- ColumnStore
SELECT * 
INTO FactInternetSales_CS
FROM FactInternetSales

CREATE CLUSTERED COLUMNSTORE INDEX idx_FactInternetSales_CS_PK      
ON FactInternetSales_CS


/* 
        STORAGE EFFFICIENCY
            1-  Columnstore Index.
            2-  Heap Table.
            3-  Rowstore Clustered Index */

            
--                                  10-1-6 SQL INDEXES: UNIQUE INDEX
/*
        DEF:
            Ensures no duplicate values exist in specific column.

        BENEFITS:
            1.  Enforce uniqueness.
            2.  Slightly incress query performance.

        PERFORMANCE:
           Writing to an unique index is slower than non-unique.

        SYTAX:
            CREATE [UNIQUE] [CLUSTERED | NONCLUSTERED] [COLUMNSTORE] INDEX index_name   -- BY default is NOT UNIQUE 
            ON table_name (Column1, Column2, ...)
                    
        E.G:                                                                                                                                                                                            */
Use SalesDB                                                         --  +------------+
SELECT * FROM Sales.Products                                        --  |   RULE     |                                                                  
CREATE UNIQUE NONCLUSTERED INDEX idx_Products_Product               --  +------------+------------------------------------------+
ON Sales.Products (Product)                                         --  |   Duplicates in the Columns will prevent creating a   | 
                                                                    --  |                      unique index                     |
INSERT INTO Sales.Products (ProductID, Product)                     --  +-------------------------------------------------------+
    VALUES(106, 'Caps')                                             -- We can not inseerte duplicate products in Table with index nique                       
                                  

--                                      10-1-7 SQL INDEXES: FILTERED  INDEX
/*
        DEF: 
            An index that includes only rows meeting the specified conditions.

        BENEFITS:
            -   Targeted Optimization.
            -   Reduce Storage: Less data in the index.

        SYNTAX:

            CREATE [UNIQUE] [NONCLUSTERED] INDEX index_name
            ON table_name (column1, column2, ...)
            WHERE [Condition]

        RULES:
            -   You cannot create a filtered index on a Clustered index.
            -   You Cannot create a filtered index on a Columnstore index.
    
        E.G:
*/
   
Select * From Sales.Customers 
Where Country='USA'

CREATE NONCLUSTERED INDEX idx_Customers_Country
ON sales.Customers (Country)
Where Country='USA'
SELECT 

--                                      10-1-8 SQL INDEXES: CHOOSE THE RIGHT INDEX
/*
                                                          +---------------------------------+
        +-------------------------------------------------|           When to use           |-------------------------------------------------+
        |                                                 +---------------------------------+                                                 |  
        |             +-----------+                        +--------------------+                        +--------------------+               |                          
        |             |   HEAP    |                        |   Clustered Index  |                        | Columnstored Index |               |                          
        |      +------+-----------+--------+     +---------+--------------------+---------+    +---------+--------------------+---------+     |          
        |      |        Fast Insert        |     |             For Primary Keys           |    |         For Analytical Queries         |     |          
        |      |  (For Staging Tables)     |     |       If not, then for date Columns    |    |       Reduce Size Of large Table       |     |          
        |      +---------------------------+     +----------------------------------------+    +----------------------------------------+     |                          
        |                                                  +---------------------+                                                            |                                  
        |                                                  | Non-Clustered Index |                                                            |                          
        |                                      +----------+---------------------+----------+                                                  |                  
        |                                      |                 For non-PK                |                                                  |                                      
        |                                      |        (Foreign Keys, Joins and Filtre)   |                                                  |                          
        |                                      +-------------------------------------------+                                                  |                                                      
        |                                                                                                                                     |                                      
        |                          +---------------------+                           +---------------------+                                  |                                                      
        |                          |    Filtered Index   |                           |     Unique Index    |                                  |                              
        |               +----------+---------------------+----------+     +----------+---------------------+----------+                       |                                                          
        |               |           Target Subnet of Data           |     |             Enforce Uniqueness            |                       |                                          
        |               |            Reduce Size Of Index           |     |             Improve Query Speed           |                       |                                                  
        |               +-------------------------------------------+     +-------------------------------------------+                       |                                              
        +-------------------------------------------------------------------------------------------------------------------------------------+                                              
*/
--                                      10-1-9 SQL INDEXES: MANAGEMENT & MONITORING
/*
                  10-1-9 -1-  Monitor Index Usage.
                            +   Task 1: 
                                 -   List all indexes on a specific table:                                                                                                              */
                                             sp_helpindex'Sales.DBCustomers'
                                                                                                                                                                        /*
                                    -   Monitoring Index Usage:
                                        Note: Sys System Schema Contains metadata about database tables, views, indexes, etc.                                             */
                                        SELECT 
                                            tbl.name As tableName,
                                            idx.name AS IndexName,
                                            idx.type_desc As IndexType,
                                            idx.is_primary_key As IsPrimaryKey,
                                            idx.is_Unique As IsUnique,
                                            idx.is_disabled As IsDisabled,
                                            s.user_seeks    As userSeeks,
                                            s.user_scans As UserScans,  
                                            s.user_lookups  As userLokups,
                                            s.user_updates  AS userUpdate,
                                            COALESCE (s.last_user_seek, s.last_user_scan) LastUpdate
                                           
                                        From sys.indexes idx
                                        JOIN sys.tables tbl
                                        ON idx.object_id = tbl.object_id 
                                        LEFT JOIN  sys.dm_db_index_usage_stats  s
                                        ON  s.object_id = idx.object_id
                                        AND s.index_id = idx.index_id
                                        Order by tbl.name, idx.name   ;                                                                                                                    /*
                            +   Dynamic Management View (DMV):
                                        -   Provide Real-Time insights into database performance and system health                                                                      */
                                                SELECT * FROM sys.dm_db_index_usage_stats ; -- VIEW the previeu querie for more details
                                                                                                                                                                            /*                          
                    10-1-9-2-  Monitor Missing Indxes                                                  */
                            
                                
                                SELECT
                                    fs.SalesOrderNumber,
                                    db.EnglishProductName,
                                    db.color
                                From        FactInternetSales fs
                                INNER JOIN  DimProduct db
                                ON          fs.ProductKey = db.ProductKey
                                WHERE       db.color = 'Black'
                                AND         fs.OrderDateKey  Between 20101229 AND 20101231
                                
                                SELECT * From sys.dm_db_missing_index_details 
                                                                                                                                            /*
                          +---------+                                                                                                                           
                          |   TIP   |
                          +---------+----------------------------------------------+
                          | Evalute the recommandations before creating any index  |
                          +--------------------------------------------------------+
                                                                                                    
                    10-1-9-3-  Monitor Duplicate Indexes                                                                                           */
                            
                            SELECT  
                                tbl.name As TableName,
                                col.name AS IndexColumn,
                                idx.name AS IndexName,
                                idx.type_desc AS indexType,
                                COUNT(*) Over(Partition by tbl.name, col.name) ColumnCount
                            FROM sys.indexes idx
                            JOIN sys.tables tbl ON idx.object_id = tbl.object_id
                            JOIN sys.index_columns ic ON idx.object_id  =   ic.object_id AND idx.index_id = ic.index_id
                            JOIN sys.columns col ON ic.object_id = col.object_id AND ic.column_id = col.column_id
                            ORDER BY columnCount DESC 
                  
                                                                                                                                                /*
                  10-1-9-4-  Update Statistics.                                                                                                                        */
                    
                           SELECT
                                SCHEMA_NAME(t.schema_id)    AS SchemaName,
                                t.name  AS tableName,
                                s.Name AS StatisticName,
                                sp.last_updated AS lastUpdate,
                                DATEDIFF(day, sp.last_updated, GETDATE()) AS LastUpdateDay,
                                sp.rows AS 'Rows',
                                sp.modification_counter AS ModificationsSinceLasUpdate
                            FROM sys.Stats AS s
                            JOIN sys.tables t
                            ON s.object_id = t.object_id
                            CROSS APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) AS sp
                            ORDER BY sp.modification_counter DESC                                               /*
                        +   Updating Statics:
                            1.  Weekly job to update statics on weekends 
                            2.  After Migrating Data                                                        */

                            EXEC sp_updatestats
                                                                                                                                                                    /*   
                 10-1-9-5-  Monitor Fragementations :
                        +   DEF
                            -   Unused spaces in data pages
                            -   Data page are out of order 
                        +   Fragementation Methods:

                                +   Reorganize:
                                        -   Defragments leaf nodes to keep them sorted 
                                        -   "Light" Operation

                                +   Rebuild:
                                        -  Recreates Index from Scratch
                                        -   "Heavy" Operation                                                                                               */
                        SELECT *
                        FROM sys.dm_db_index_physical_Stats(DB_ID(), NULL, NULL, NULL, 'LIMITED')                                                       /*

                        +------------------------------+
                        | Avg_fragmentation_in_percent |
                        +------------------------------+-------------------------+
                        |   Indicate how out-of-order pages are within the index |
                        |           0% means no fragmentation (perfect)          |
                        +--------------------------------------------------------+
                        +   when to defragment ?
                                -       < 10%  No acion needed
                                -       10-30% Reorganize                                                                                                                   */
                                            
                                            ALTER INDEX idx_Customers_CS_Country ON Sales.Customers REORGANIZE                                                              /*
                                -       > 30%  Rebuild                                                                                          */
                                             ALTER INDEX idx_Customers_Country ON Sales.Customers REBUILD 
                                                                                                                                                                                                                           */
--                   10-1-9-6-   EXECUTION PLAN 
                                                                                                                                                                    /*
                                DEF: 
                                    Roadmap generated by a database on how it will execute your query step by step

                                BENEFITS: 
                                    -   Understand how SQL executes your query
                                    -   How many resources your query consumes?
                                    -   Check if your new Indexes are used
                                    -   Testing & Experimenting Indexes 
            
                                TYPE OF EXECUTION PLAN:
                                    1.  Estimated Execution Plan:
                                        -   Generated without exucuting the query.
                                        -   Shortcut:   Ctrl  + L.
                                        -   Based on Statics.
                                        -   Good For: 
                                            .   Safe Analysis
                                            .   Understanding optimizer logic.
                                        -   Risk:
                                            .   May differ from real behavior
                                    2.  Actual Execution Plan 
                                        -   Generated after Execution Plan
                                        -   ShortCut: Ctrl + M
                                        -   Shows real rows Counts vs estimation
                                        -   Good For:
                                            .   Performance troubleshooting
                                            .   Detecting bad cardinality estimation.
                                    3.  Cached Excution Plan 
                                        -   Stored in Plan Cache
                                        -   Reused for future executions
                                        -   !!!! Can Cause !!! : 
                                               .    Parameter sniffing problems
                                E.G Basics:                                                                                                                                        */
                 
                                     SELECT *
                                     FROM FactResellerSales
                                     SELECT *
                                     FROM   FactResellerSales_Hp
                                                                                                                                                                                            /*
                                 Estimated vs Actual Plans:
                                            If the predictions don't match the Actual Excution Plan,
                                        this indicate issues like inaccurate statistics  or outdated indexes ,
                                                    leading to poor performance.
                    
                                 Table Scan
                                                            Reads the entire table,
                                                   page by page and row by row "everything"
                                            which can lead to slower query performance on large tables.

                                  Heap vs Clustered index                          
                        
                                           Sorting Data :
                                              Heap is Slower than Custered, because Database must perform extra work
                                                                            to store Rows                                           */
                                        use AdventureWorksDW2022
                                        Select *
                                        FROM FactResellerSales
                                        Order by  SalesOrderNumber      
                                        /*
                                  TIPS:
                                        -   After Crating a new index, check the execution plan to see if your query uses the index
                                   
                                  NONCLUSTRED INDEX :*/
                                        SELECT *
                                        FROM FactResellerSales
                                        WHERE CarrierTrackingNumber='4911-403C-98'

                                        CREATE NONCLUSTERED INDEX idx_FactResseller_CTA
                                        ON FactresellerSales(CarrierTrackingNumber) 

                                    -- Index Seek : A trageted searche within an index,
                                    --              retrieving only a specific rows.                          
                                                                                                                                                                                                                        /*
                                  JOIN ALGORITHMS:
                                        
                                           -    Nested Loop:    Compares tables row by row; best for small tables.
                                           -    Hash Match:     Matches rows using a hash table; best for large tables.
                                           -    Merge Join:     Merge two sorted tables; efficient when both are sorted 

                                  ROWSTORE vs COLUMNSTORE                                                                                               */
                                        
                                          Select 
                                                p.EnglishProductName as ProductName,
                                                SUM(s.SalesAmount) As TotalSales
                                          FROM FactResellerSales s
                                          JOIN DimProduct p
                                          ON p.productKey = s.productKey
                                          Group By  p.EnglishProductName
                                                                                                                                                                            /* 
                                   BAD EXECUTION PLAN:
                                        1.  Outdated Statics 
                                        2.  Too Many Indexes                                                                                                               */


--                       10-1-9-7- SQL HINTS: 
/*                    
                            DEF:
                                Commands you add to a query to force the database to run 
                                it in a specific way for better performance 
                            E.G:                                                                */
                                 
                                SELECT 
                                    o.Sales,
                                    c.country 
                                FROM Sales.Orders o
                                LEFT JOIN Sales.Customers c WITH (FORCESEEK) 
                                        -- FORCESEEK: Scanning a particular range of rows from 
                                        --            a Clustered Index.
                                ON c.CustomerID = O.CustomerID

                                 OPTION (HASH JOIN)     -- Use each row from the top input to build a hash table, and
  /*                                                      --  each row from the bottom input to probe into the hash table,
                                                        --  outputting all matching rows 
                            TIPS   : 
                                    1.  Test hints in all project environments (DEV, PROD)
                                        as performance may vary.
                                    2.  Hints are quick fixes (Workaround not Solution)
                                        you still have to find the cause and fix it.        */

--                       10-1-9-8- THE GOLDEN RULE:
    /*  
                                Avoid Over Indexing: 
                                                    -   Indexes Slow Down Write Performance: When Data is inserted, Updated or Deleted
                                                                                            database has to update indexes.
                                                    -   Too many indexes can confuse Execution Plan:    Increase Planing Time 
                                                                                                        Choose a suboptimal plan                     */
--                       10-1-9-9- INDEXING STRATEGY: 
/*
                +----+                            +-------------------------+                                       +----+                            +-----------------------+ 
                | #1 |----------------------------|Initial Indexing Startegy|---------------------------------+     | #2 |----------------------------|Usage Patterns Indexing|----------------------------+
                +----+                            +-------------------------+                                 |     +----+                            +-----------------------+                            |                    
                |                     +--------+                               +--------+                     |     |                 +-+  +--------------------------------------------+                  |                                           
                |            +--------|  OLAP  |--------+             +--------|  OLAP  |--------+            |     |                 |1|  | Identify frequently used Tables & Columns  |                  |
                |            |        +--------+        |             |        +--------+        |            |     |                 +-+  +--------------------------------------------+                  |                                                                                           
                |            |         Optimaze         |             |         Optimize         |            |     |                                                                                      |                              
                |            |           Read           |             |           Write          |            |     |                 +-+  +--------------------------------------------+                  |                                                                                               
                |            |        Performance       |             |        Performance       |            |     |                 |2|  | Choose Right Index                         |                  |                                                                                         
                |            |                          |             |                          |            |     |                 +-+  +--------------------------------------------+                  |                                                                                           
                |            |  Switch Large frequently |             |       Clustered Index    |            |     |                                                                                      |                                      
                |            |     Used tables into     |             |          Primary Key     |            |     |                 +-+  +--------------------------------------------+                  |                    
                |            |       ColumnStore        |             |                          |            |     |                 |3|  | Test Index                                 |                  |                                                                  
                |            +--------------------------+             +--------------------------+            |     |                 +-+  +--------------------------------------------+                  |                                                                                           
                +---------------------------------------------------------------------------------------------+     +--------------------------------------------------------------------------------------+                                                                                                                           
    
                +----+                               +-------------------------+                                    +----+                          +--------------------------+ 
                | #3 |-------------------------------| Senario-Based Indexing  |------------------------------+     | #4 |--------------------------| Monituring & Maintenance |---------------------------+
                +----+                               +-------------------------+                              |     +----+                          +--------------------------+                           |
                |                  +-+  +---------------------------------------------------+                 |     |                 +-+  +-------------------------------------------+                   |
                |                  |1|  |  Identify Slow Queries                            |                 |     |                 |1|  |   Monitor Index Usage                     |                   | 
                |                  +-+  +---------------------------------------------------+                 |     |                 +-+  +-------------------------------------------+                   | 
                |                  +-+  +---------------------------------------------------+                 |     |                 +-+  +-------------------------------------------+                   |        
                |                  |2|  |  Check Execution Plan                             |                 |     |                 |2|  |   Monitor Missing Indexes                 |                   |
                |                  +-+  +---------------------------------------------------+                 |     |                 +-+  +-------------------------------------------+                   |
                |                  +-+  +---------------------------------------------------+                 |     |                 +-+  +-------------------------------------------+                   |
                |                  |3|  |  Choose Right Index                               |                 |     |                 |3|  |   Moitor Duplicate Indexes                |                   |                                      
                |                  +-+  +---------------------------------------------------+                 |     |                 +-+  +-------------------------------------------+                   | 
                |                  +-+  +---------------------------------------------------+                 |     |                 +-+  +-------------------------------------------+                   | 
                |                  |4|  |  (Test) Compare Execution Plans                   |                 |     |                 |4|  |    Update Statics                         |                   |
                |                  +-+  +---------------------------------------------------+                 |     |                 +-+  +-------------------------------------------+                   | 
                |                                                                                             |     |                 +-+  +-------------------------------------------+                   |
                |                                                                                             |     |                 |5|  |    Monitor Fragementations                |                   |
                |                                                                                             |     |                 +-+  +-------------------------------------------+                   |
                 +--------------------------------------------------------------------------------------------+     +--------------------------------------------------------------------------------------+          */   
                        

--                          10-2 PARTITIONS:
/*
            DEF:
                -   Divides Big Table into Smaller Partitions While still being 
                    treated as a single logical table.

                -   Define the Logic on how to divide your data into partitions !
                    Based on Partition Key Like (Column, Region, ...)
                                                 +-----------------------------+
                                                 |          Boundries          |
                                                 |_____________________________|
                                                  /             |             \
                                                 /              |              \
                                <---------------|---------------|---------------|--------------->
                                           2023-12-31      2024-12-31      2025-12-31
                                <---------------|   ------------|   ------------|   ------------>
                                    Partition 1     Partition 2      Partition3      Partition 4
                                   Rows for 2023   Rows of 2024     Rows for 2025   Rows from 2026
                                 and earlier years                                       anward 
             STEP1: Create a partiton Function                                                                                                                                                      */

CREATE PARTITION FUNCTION partitionByYear (DATE)
AS RANGE LEFT  FOR VALUES ('2023-12-31','2024-12-31','2025-12-31') 
 
 
 -- Query lists all existing partition function
 SELECT
    name,
    function_id,
    type,
    type_desc,
    boundary_value_on_Right
FROM sys.partition_functions

--           STEP2: Create Filegroups
/*
    DEF: 
        Logical Container of one or more data files to help 
        organize partitions.
*/
ALTER DATABASE SalesDB ADD FILEGROUP FG_2023;
ALTER DATABASE SalesDB ADD FILEGROUP FG_2024;
ALTER DATABASE SalesDB ADD FILEGROUP FG_2025;
ALTER DATABASE SalesDB ADD FILEGROUP FG_2026;
--==========================================
SELECT * 
FROM sys.filegroups
WHERE type= 'FG'
-- Primary : Default Filegroup where all objects of Database is stored


--           STEP3: Create Data Files to each Filegroup
ALTER DATABASE SalesDB ADD FILE 
(
    NAME = P_2023, -- Logical Name
    FILENAME = 'C:\Program Files\Microsoft SQL Server\MSSQL16.SQLEXPRESS\MSSQL\DATA\P_2023.ndf'
)  TO FILEGROUP FG_2023

ALTER DATABASE SalesDB ADD FILE 
(
    NAME = P_2024, -- Logical Name
    FILENAME = 'C:\Program Files\Microsoft SQL Server\MSSQL16.SQLEXPRESS\MSSQL\DATA\P_2024.ndf'
)  TO FILEGROUP FG_2024

ALTER DATABASE SalesDB ADD FILE 
(
    NAME = P_2025, -- Logical Name
    FILENAME = 'C:\Program Files\Microsoft SQL Server\MSSQL16.SQLEXPRESS\MSSQL\DATA\P_2025.ndf'
)  TO FILEGROUP FG_2025

ALTER DATABASE SalesDB ADD FILE 
(
    NAME = P_2026, -- Logical Name
    FILENAME = 'C:\Program Files\Microsoft SQL Server\MSSQL16.SQLEXPRESS\MSSQL\DATA\P_2026.ndf'
)  TO FILEGROUP FG_2026

--==============================================================================================
SELECT 
    fg.name As FilegoupeName,
    mf.name AS LogicalFileName,
    mf.physical_name As physicalFilePath,
    mf.size / 128 As SizeInMB
FROM sys.filegroups fg
JOIN 
    sys.master_files mf  ON fg.data_space_id = mf.data_space_id
WHERE
    mf.database_id = DB_ID('SalesDB')

--           STEP4: Create Partition Schema

CREATE PARTITION SCHEME SchemePartitionByYear
AS PARTITION PartitionByYear
TO (FG_2023, FG_2024, FG_2025, FG_2026)
/*

            +-------+
            | NOTES |
+-----------+-------+---------------------------------------------------------+
| - Sort the filegroups according to the result of the function's Partitions. |
| - 3 Boundaries = 4 Partitons = 4 FileGroups                                 |
+-----------------------------------------------------------------------------+     */


--=============================================
SELECT  
    ps.name AS PartitionSchemaName,
    pf.name AS PartitionFunctionName,
    ds.destination_id AS PartitionNumber,
    fg.name As FilegroupName
FROM sys.partition_schemes ps
JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
JOIN sys.destination_data_spaces ds ON ps.data_space_id = ds.partition_scheme_id
JOIN sys.fileGroups fg ON ds.data_space_id = fg.data_space_id



--           STEP5: Create Partitioned Table

CREATE TABLE Sales.Order_Partitioned 
(
    OrderId INT,
    OrderDate DATE,
    Sales INT
) ON SchemePartitionByYear (OrderDate)

--           STEP6: Insert Data Into the Partitioned Table
INSERT INTO sales.Order_Partitioned VALUES(
    1, '2023-05-15', 100
);
INSERT INTO sales.Order_Partitioned VALUES(
    2, '2024-07-23', 100
);
INSERT INTO sales.Order_Partitioned VALUES(
    3, '2025-11-10', 100
);
INSERT INTO sales.Order_Partitioned VALUES(
    4, '2026-04-12', 100
);
SELECT * FROM Sales.Order_Partitioned

SELECT 
    p.partition_number AS partitionNumber,
    f.name AS PartitiionFileGroup,
    p.rows AS NumbersOfRows
FROM sys.partitions p
JOIN sys.destination_data_spaces dds ON  p.partition_number = dds.destination_id
JOIN sys.filegroups f ON dds.data_space_id = f.data_space_id
WHERE OBJECT_NAME(p.object_id) = 'Order_Partitioned';



--                          10-3 30 x PERFORMANCE TIPS:
/*       +--------------+
   +-----| GOLDEN RULE: |--------------------------------------------------------------------------------+
   |     +--------------+                                                                                |
   |  -    For small-medium tables, the query optimizer may react similary to different query styles.    |
   |  -    Always check the execution plan to confirm performance improvements when optimizing your      |
   |                                                  query.                                             |
   |        ->  If there's no improvement, then just focus on readability.                               | 
   +-----------------------------------------------------------------------------------------------------+              */
--                              10-3-1   FETCHING DATA:
/*
      +--------+
   +--| #1 TIP |-------------------+
   |  +--------+                   |
   |  Select only what you needed  |
   +-------------------------------+
*/

-- Bad Practice 
    SELECT * FROM  Sales.Customers             


--GOOD Practice
    SELECT CustomerID, FirstName,  LastName FROM  Sales.Customers           

/*
      +--------+
   +--| #2 TIP |----------------------------+
   |  +--------+                            |
   |  Avoid unnecessary DISCINT & ORDER BY  |
   +----------------------------------------+
*/ 


-- Bad Practice 
SELECT 
    FirstName
FROM Sales.Customers
ORDER BY FirstName

-- Good Practice
SELECT FirstName
FROM Sales.Customers

       

/*
      +--------+
   +--| #3 TIP |----------------------------+
   |  +--------+                            |
   |  For Exploration Purpose, Limite Rows  |
   +----------------------------------------+
*/ 

-- Bad Practice 
SELECT 
    orderId,
    Sales
FROM Sales.Orders

-- good Practice 
SELECT TOP 10 
    orderId,
    Sales
FROM Sales.Orders

--                              10-3-2   FELTRING DATA:

/*
      +--------+
   +--| #4 TIP |-------------------------------------------------------------+
   |  +--------+                                                             |
   |  Create nonclustered Index on Frequently used Columns in WHERE clause   |
   +-------------------------------------------------------------------------+
*/ 

SELECT * FROM Sales.orders  WHERE OrderStatus = 'Delivered'

CREATE NONCLUSTERED INDEX idx_orders_orderStatus ON Sales.orders(OrderStatus)

/*
      +--------+
   +--| #5 TIP |--------------------------------------------+
   |  +--------+                                            |
   |  Avoid applying Functions to columns in WHERE Clause   |
   +--------------------------------------------------------+
*/ 
--  Bad Practice: 
SELECT * FROM Sales.Orders 
WHERE LOWER(OrderStatus) = 'delivered'  -- Note: Functions on Columns Can Block Index Usage 

--Good Practice
SELECT * FROM Sales.Orders
WHERE OrderStatus = 'Delivered'

-- Bad Practice: 
SELECT * 
From Sales.Orders
WHERE YEAR(orderDate) = 2025

--Good Practice 
SELECT * 
FROM Sales.Orders
WHERE OrderDate BETWEEN '2025-01-01' AND '2025-12-31'


/*
      +--------+
   +--| #6 TIP |--------------------------------------------+
   |  +--------+                                            |
   |   Avoid leading wildcards as they prevent index usage  |
   +--------------------------------------------------------+
*/


-- Bad Practice
SELECT * 
FROM Sales.Customers
WHERE LastName  LIKE '%GOLD%'

-- Good Practice
SELECT * 
FROM Sales.Customers
WHERE LastName  LIKE 'GOLD%'


/*
      +--------+
   +--| #7 TIP |---------------------------------+
   |  +--------+                                 |
   |   Use IN instead of Multiple OR Conditions  |
   +---------------------------------------------+
*/

-- Bad Practice 
SELECT *
FROM Sales.Orders
WHERE CustomerID = 1 OR CustomerID = 2 OR CustomerID = 3

-- Good Practice 
SELECT *
FROM Sales.Orders
WHERE CustomerID IN (1, 2,3)

--                              10-3-3   JOINING DATA:


/*
      +--------+
   +--| #8 TIP |------------------------------------------------------+
   |  +--------+                                                      |
   |   Understand The Speed of Joins & Use Inner JOIN when possible   |
   +------------------------------------------------------------------+
*/

-- Best Performance 
SELECT c.FirstName , O.OrderID FROM Sales.Customers c INNER JOIN sales.Orders o ON C.CustomerID = O.CustomerID

-- Slightly Slower Performance 
SELECT c.FirstName, o.OrderID FROM Sales.Customers c RIGHT JOIN Sales.Orders o ON c.CustomerID = o.CustomerID
SELECT c.FirstName, o.OrderID FROM Sales.Customers c LEFT JOIN Sales.Orders o ON c.CustomerID = o.CustomerID

-- Worst Performance 
SELECT c.FirstName, o.OrderID FROM Sales.Customers c OUTER JOIN  Sales.Orders o ON c.CustomerID = o.CustomerID


/*
      +--------+
   +--| #9 TIP |-----------------------------------------------------------------+
   |  +--------+                                                                 |
   |   Use Explicit Join (ANSI Join) Instead of Implicit Join ( non-ANSI Join)   |
   +-----------------------------------------------------------------------------+
*/

--  Bad Practice
SELECT o.OrderID, C.FirstName   
FROM Sales.Customers c, Sales.Orders o
where c.CustomerID = O.CustomerID

-- Good Practice
SELECT o.OrderID, c.FirstName
FROM Sales.Customers c 
INNER JOIN Sales.Orders o 
ON C.CustomerID = O.CustomerID

/*
      +---------+
   +--| #10 TIP  |------------------------------------------------------+
   |  +---------+                                                       |
   |   Ensure that the Columns used in the ON  clause are indexed       |
   +--------------------------------------------------------------------+
*/

SELECT o.OrderID, c.FirstName
FROM Sales.Customers c 
INNER JOIN Sales.Orders o 
ON C.CustomerID = O.CustomerID  -- We don't have index for O.CutomerID

CREATE NONCLUSTERED INDEX IX_Order_CustomerID ON Sales.Orders(CustomerID)

/*
      +----------+
   +--| #11 TIP  |---------------------------+
   |  +----------+                           |
   |   Filter Before Joining (Big Tables)    |
   +-----------------------------------------+
*/

-- Filter After Join (WHERE) // For Small & Medium Size Tables 
SELECT c.FirstName, O.OrderID
FROM Sales.Customers c
INNER JOIN Sales.Orders o
ON C.CustomerID = o.OrderID
WHERE o.OrderStatus = 'Delivered'

-- Filter During Join (ON)
SELECT c.FirstName, o.OrderID 
FROM SaleS.Customers c 
INNER JOIN Sales.Orders o 
ON c.CustomerID = O.CustomerID
AND o.OrderStatus = 'Delivered'

--Filter Before Join (SUBQUERY) // For Large Tables 
SELECT C.FirstName, o.OrderID
FROM Sales.Customers c
INNER JOIN (SELECT OrderID, CustomerID FROM Sales.Orders WHERE OrderStatus = 'Delivered') o
ON c.CustomerID = O.CustomerID
-- tip : TRY to isolate the preparation step in a CTE or Subquery

/*
      +----------+
   +--| #12 TIP  |------------------------------+
   |  +----------+                              |
   |   Aggregate Before Joining (Big Tables)    |
   +--------------------------------------------+
*/

-- Best Practice for Small-Medium Tabes
-- Grouping and Joining
SELECT c.CustomerID, c.FirstName, COUNT(o.OrderID) AS OrderCount 
FROM Sales.Customers c
INNER JOIN Sales.Orders o 
ON C.CustomerID = o.CustomerID 
GROUP BY  c.CustomerID , c.FirstName

--Best Practices  for Big Tables
--Pre-aggregated Subquery 
SELECT c.CustomerID, c.FirstName, o.OrderCount 
FROM Sales.Customers c 
INNER JOIN (
    SELECT CustomerID , Count(OrderID) AS OrderCount
    FROM Sales.Orders
    GROUP BY CustomerID 
) o 
ON c.CustomerID = O.CustomerID

-- Bad Practices 
--Correlated Subquery 
SELECT 
    c.CustomerID,
    c.FirstName,
    ( SELECT Count(o.ORderID)
      FROM Sales.Orders o 
      WHERE o.CustomerID = c.customerID) AS OrderCount 
FROM Sales.Customers c
-- Correlated Queries are inefficient because SQL 
--  execute Aggregations for every row



/*
      +----------+
   +--| #13 TIP  |------------------------------+
   |  +----------+                              |
   |   Use Union Instead of OR in Joins         |
   +--------------------------------------------+
*/
-- Bad Practice 
SELECT o.OrderID, c.FirstName
FROM Sales.Customers c 
INNER JOIN Sales.Orders o 
ON c.CustomerID = o.CustomerID
OR c.CustomerID = o.SalesPersonID

-- Good Practice 
SELECT o.OrderID, c.FirstName
FROM Sales.Customers c
INNER JOIN Sales.Orders o 
ON c.CustomerID = o.SalesPersonID
UNION
SELECT o.OrderID, c.FirstName 
FROM Sales.Customers c
INNER JOIN Sales.Orders o 
ON c.CustomerID = o.SalesPersonID
 

 
/*
      +----------+
   +--| #14 TIP  |-----------------------------------------------+
   |  +----------+                                               |
   |   Check for Nested Loops and use SQL HINTS When necessary   |
   +-------------------------------------------------------------+
*/

SELECT o.OrderID, c.FirstName
FROM Sales.Customers c
INNER JOIN Sales.Orders o 
ON C.CustomerID = o.CustomerID

--Good Practice for Having a Big Table & Small Table
SELECT  o.OrderID, c.FirstName
FROM Sales.Customers c
INNER JOIN Sales.Orders o 
ON o.CustomerID = c.CustomerID
OPTION (HASH JOIN)

/*
      +----------+
   +--| #15 TIP  |-------------------------------------------------------+
   |  +----------+                                                       |
   |   Use UNION ALL instead of using UNION | duplicate are acceptable   |
   +---------------------------------------------------------------------+
*/

-- Bad Practice 
SELECT CustomerID  FROM Sales.Orders
UNION 
SELECT CustomerID FROM Sales.OrdersArchive

--Best Practice 
SELECT CustomerID FROM Sales.Orders
UNION ALL
SELECT CustomerID FROM Sales.OrdersArchive


-- 06/02/2026




/*
      +----------+
   +--| #16 TIP  |----------------------------------------------------------------------+
   |  +----------+                                                                      |
   |   Use UNION ALL + Distinct instead of using UNION | duplicate are not acceptable   |
   +------------------------------------------------------------------------------------+
*/


-- Bad Practice 
SELECT CustomerID From Sales.Orders
UNION 
SELECT CustomerID FROM Sales.OrdersArchive

-- Good Practice 
SELECT DISTINCT CustomerID 
 FROM (
    SELECT CustomerID FROM Sales.Orders
    UNION ALL 
    SELECT CustomerID FROM Sales.OrdersArchive
) AS CombinedData

--                              10-3-4   AGGREGATING DATA:

/*
      +----------+
   +--| #17 TIP  |---------------------------------------------+
   |  +----------+                                             |
   |   Use Columnstore Index for Aggregations on Large Table   |
   +-----------------------------------------------------------+
*/

SELECT CustomerID, COUNT (OrderID) As OrderCount
FROM Sales.orders
GROUP BY CustomerID 

CREATE CLUSTERED COLUMNSTORE INDEX Idx_Orders_Columnstore ON Sales.Orders


/*
      +----------+
   +--| #18 TIP  |---------------------------------------------------+
   |  +----------+                                                   |
   |   Pre-Aggregate Data and Store it in new Table for Reporting    |
   +-----------------------------------------------------------------+
*/


SELECT MONTH(OrderDate) OrderYear, SUM(Sales) AS TotalSales
INTO Sales.SalesSummary 
FROM Sales.Orders
GROUP BY MONTH(OrderDate) 

SELECT OrderYear, TotalSales FROM Sales.SalesSummary

--                              10-3-5  BEST PRACTICES FOR SUBQUEREIS :

/*
      +----------+
   +--| #19 TIP  |------------+
   |  +----------+            |
   |   JOIN vs EXISTS vs IN   |
   +--------------------------+
*/

--JOIN ( Best Practice: if the performance equal to EXISTS ) :
SELECT o.OrderID , o.Sales
FROM Sales.Orders o 
INNER JOIN Sales.Customers c
ON o.CustomerID = c.CustomerID 
WHERE c.Country = 'USA'         -- We Join Customers Table Only for Filtring USA Orders 

--  EXCISTS (Best Practice: Use It For Large Tables):
--  EXISTS better than JOIN because, It Stop at first match and avoid data duplication 
SELECT O.OrderID, o.Sales
FROM Sales.Orders o 
WHERE EXISTS(
    SELECT 1 
    FROM Sales.Customers c
    WHERE c.CustomerID = o.CustomerID
    AND Country= 'USA'
)

--  IN (Bad Practice ) 
--  The IN Operator process and evaluates all rows. It Lacks an early exit mechanism
SELECT o.OrderID, o.Sales
FROM Sales.Orders o 
WHERE o.CustomerID IN(
    SELECT CustomerID 
    FROM Sales.Customers
    WHERE Country = 'USA'
)


/*
      +----------+
   +--| #20 TIP  |--------------------------+
   |  +----------+                          |
   |   Avoid Redundant Logic in Your Query  |
   +----------------------------------------+
*/
-- Bad Practice
SELECT EmployeeID, FirstName, 'Above Average' Status
FROM Sales.Employees
WHERE Salary > (SELECT AVG(Salary) From Sales.Employees)
UNION ALL 
SELECT EmployeeID, FirstName, 'Below Average' Status
FROM Sales.Employees
WHERE Salary < (SELECT AVG(Salary) FROM Sales.Employees) 

-- Good Practice 

SELECT 
    EmployeeID,
    FirstName,
    CASE
        WHEN Salary > AVG(Salary) OVER() THEN 'Above Average'
        WHEN Salary < AVG(Salary) OVER() THEN 'Below Average'
        ELSE 'Average'
    END AS Status 
FROM Sales.Employees
ORDER BY Status
/*
    +------+
    | NOTE |
    +------+-------------------------------------+
    |Spot redundant queries? Review and fix them |
    +--------------------------------------------+
*/

--                              10-3-6   BEST PRACTICES FOR CREATING TABLES (DDL):


/*
      +----------------------------------+
   +--| #21 + #22 + #23 + #24 + #25 TIPs |--------------------------------------------------+
   |  +----------------------------------+                                                  |
   |    -   #21 TIP: Avoid Data Type VARCHAR & TEXT                                           |
   |    -   #22 TIP: Avoid(MAX) unecessarily large lenghts in data types                      |
   |    -   #23 TIP: Use NOT NULL constraint where applicable                                 |
   |    -   #24 TIP: Ensure all your tables have a Clustered Primary Key                      |
   |    -   #25 TIP: Create a non-clustered index for foreign keys that are used frequently   |
   +----------------------------------------------------------------------------------------+
*/ 

-- Bad Practice 
CREATE TABLE CustomerInfo (
    CustomerID INT,
    FirstName VARCHAR(MAX),
    LastName TEXT,
    Country VARCHAR (255),
    TotalPurchases FLOAT,
    Score VARCHAR(255),
    BirthDate VARCHAR (255),
    EmployeeID INT,
    CONSTRAINT FK_CustomersInfo_EmployeeID FOREIGN KEY (EmployeeID)
        REFERENCES Sales.Employees(EmployeeID)
);

-- Good Practice Practice 
CREATE TABLE CustomersInfo (
    CustomerID INT PRIMARY KEY CLUSTERED,
    FirstName VARCHAR(50) NOT NULL,
    LastName VARCHAR(50) NOT NULL,
    Country VARCHAR (50) NOT NULL,
    TotalPurchases FLOAT,
    Score INT,
    BirthDate DATE,
    EmployeeID INT,
    CONSTRAINT FK_CustomersInfo_EmployeeID FOREIGN KEY (EmployeeID)
        REFERENCES Sales.Employees(EmployeeID)
)

CREATE NONCLUSTERED INDEX IX_Good_Customers_EmployeeID
ON CustomersInfo(EmployeeID)


--                              10-3-7   BEST PRACTICES FOR INDEXING:


/*
      +----------------------------------+
   +--| #26 + #27 + #28 + #29 + #30 TIPs |------------------------------------+
   |  +----------------------------------+                                    |   
   |    -   #26 TIP: Avoid Over Indexing                                      |
   |    -   #27 TIP: Drop unused Indexes                                      |
   |    -   #28 TIP: Update Statics (Weekly)                                  |
   |    -   #29 TIP: Reorganize & Rebuild Indexes (Weekly)                    |
   |    -   #30 TIP: Partition Large Tables (Facts) to improve Performance    |
   |                    Next apply a Columnstore Index for the best Results   |
   +--------------------------------------------------------------------------+
*/


--                              10-3-8   BEST PRACTICES FINALE THOUGHTS ... :
/*
        -   Focus On Writing Clear Queries.
        -   Optimize Performance only When necessary
        -   Always Test Using Execution Plan

*/
/*
========================================================================================================================================================================

	                                                        CHAPTER 11: AI & SQL

========================================================================================================================================================================                                */
/* 
        + RECOMMANDATION: 
            FIND SOLUTION ON YOUR OWN!!!
            USE AI ONLY IF STUCK                */

--                      11-1 CHATGPT VS CAPILOT:
/*
        + DEFS:
            -   CHATGPT: AI Programme developped by OpenAI, designed to understand questions 
                          and provide human-like answers.
                          GPT: Generative Pre-training Transformer
                            
            -   GITHUB COPILOT: Devlopped by GitHub using OpenAI's model
         
         +--------------------------------+--------------------------------+
         |             ChatGPT            |             Copilot            |
         +--------------------------------+--------------------------------+
         |      -   Standalone            |     -   Integrated directly    |
         |      -   Application           |               in IDEs          |   
         |                                |                                |
         |      -   Conversation about    |    -    For  Software          |
         |              any topic         |          development           |
         +--------------------------------+--------------------------------+

         + When to Use ChatGPT vs Copilot:

                 +--------------------------------+--------------------------------+
                 |             ChatGPT            |             Copilot            |
                 +--------------------------------+--------------------------------+
                 |      Brainstorming & ideas     |        Codding Assistance      |
                 |        Project Planning        |            Debugging           |   
                 |  Learning Knowledge & Research |           Refactoring          |
                 |     Generate Documentations    |         Inline Comment         |
                 |     Discussing Architecture    |    Styling & Formatting Code   |
                 |    Exploring Best  Practices   |                                |
                 |    Complexe Probleme-Sloving   |                                |
                 +--------------------------------+--------------------------------+
           
           + How To Copilot
                -   GHOST TEXT: Showing code or text AI thinks you might want to add next 
                                -   Accept the Entire Suggestion USE TAB
                                -   Accept part of code  CTRL + R Arrow
                -   We Can Just Create what we want do in comments and  the copilot 
                        will suggest the code in the next line
                -   Inline Chat: Ctrl + I
                -   Copilot it might suggest table names and columns from your DB
                -   The highlighted code is just a suggestion and won't change your code 
                        unless you accept it.
                -   TIP: Keep Your eye on Sparkle, if it appears that means Copilot can 
                        suggest something.                                                                              */

--                      11-2 CHATGPT PROMPT   
--                                  11-2-1  6x Component:
            
/*
                -   [ Tasks ]           >-->    Mandatory

                -   [ Context ]         >--+ 
                                           |--> Important
                -   [ Specifications ]  >--+

                -   [ Role ]            >--+
                                           |--> Nice To Have  For getting Better Result
                -   [ Tone ]            >--+


                e.g: 
                    Task : Explain SQL Window Functions 
                    |        [ Role ]       |                       [ Context ]                                 | 
                    You are a senior expert , and i am a data analyst working on an SQL project using SQL SERVER.
                    |                    Task                 |
                    Explain the Concept of SQL Window Functions and do the following:
                     |                Specification                                 |            
                     |   -    Explain eache Window Function and show the Syntax.    |
                     |   -    Descripe why they are important and when to use them. |
                     |   -    List the top 3 use Cases.                             |

                    |                                     Tone                                          |                                             
                    The Tone should be conversational and direct, as if you're speaking to me one-on-one.                                               */

--                                  11-2-2  FOR DEVELOPERS 10x TOP PROMPTS:

/*
            +------------+
            | ALERT!!!!! |
            +------------+--------------------------------------------------+
            | Before Using ChatGPT : Ask About the rules for using ChatGPT  |
            |   in Your project at your company                             |
            +---------------------------------------------------------------+
*/


--                                      11-2-2-1  #1 SLOVE AN SQL TASK:
/*
        [PROMPT... ]
            In my SQL Server database, we have two tables:
                -   The First Table is `Orders` with the following Columns: Order_id, Sales, Customer_id, product_id.
                -   The second Tble is `Costomers` with the following Columns:  customer_id, first_name, last_name, country.
            Do the following: 
                    Write a query to rank customers based on their sales.
                    The result should include the customer's customer_id, full name, country, total slaes, and thier rank.
                    Include comments but avoid commenting on obvious parts.
                    Write three different versions of the query to achive this task.
                    Evaluate and Expline which version is best in terms of readability and performance   
       [ SOLITION ... ]

              + Version 1 — Using GROUP BY + window function (classic & clear)                                                                          */
                            SELECT
                                c.customer_id,
                                CONCAT(c.first_name, ' ', c.last_name) AS full_name,
                                c.country,
                                SUM(o.sales) AS total_sales,
                                RANK() OVER (ORDER BY SUM(o.sales) DESC) AS sales_rank
                            FROM Customers c
                            JOIN Orders o
                                ON c.customer_id = o.customer_id
                            GROUP BY
                                c.customer_id,
                                c.first_name,
                                c.last_name,
                                c.country;                                                                                                              /*
                        
                        Why this works ? 

                            -   Aggregation (SUM) is done once per customer.
                            -   RANK() uses the aggregated result directly.
                            -   Simple structure, no nesting.

              + Version 2 — Using a CTE (more readable for complex logic):                                                                                */
                            
                            WITH CustomerSales AS (
                                SELECT
                                    o.customer_id,
                                    SUM(o.sales) AS total_sales
                                FROM Orders o
                                GROUP BY o.customer_id
                            )
                            SELECT
                                c.customer_id,
                                CONCAT(c.first_name, ' ', c.last_name) AS full_name,
                                c.country,
                                cs.total_sales,
                                RANK() OVER (ORDER BY cs.total_sales DESC) AS sales_rank
                            FROM CustomerSales cs
                            JOIN Customers c
                                ON c.customer_id = cs.customer_id;                                                                                          /*

                                Why this works

                                    -   Separates business logic (sales calculation) from presentation.
                                    -   Easier to maintain and extend (filters, date ranges, etc.).
                                    -   Very readable for teams.

               + Version 3 — Using a derived table (subquery)                                                                                                */
                            
                            SELECT
                                c.customer_id,
                                CONCAT(c.first_name, ' ', c.last_name) AS full_name,
                                c.country,
                                s.total_sales,
                                RANK() OVER (ORDER BY s.total_sales DESC) AS sales_rank
                            FROM (
                                SELECT
                                    customer_id,
                                    SUM(sales) AS total_sales
                                FROM Orders
                                GROUP BY customer_id
                            ) s
                            JOIN Customers c
                                ON c.customer_id = s.customer_id;   
                                
                                GO/*

                        Why this works

                            -   Same logic as the CTE version.
                            -   Everything is in one query block.
                            -   Common in older SQL codebases.

                +   Comparison: Readability & Performance:

                | Version                  | Readability  | Performance | Notes                                   |
                |--------------------------|--------------+------------ | --------------------------------------- |
                | **Version 1 (GROUP BY)** | ⭐⭐⭐⭐☆      | ⭐⭐⭐⭐⭐    | Short, efficient, best for simple cases |
                | **Version 2 (CTE)**      | ⭐⭐⭐⭐⭐      | ⭐⭐⭐⭐⭐   | Best balance for real projects         |
                | **Version 3 (Subquery)** | ⭐⭐⭐☆☆      | ⭐⭐⭐⭐⭐  | Slightly harder to read                 |

                +   Final Verdict 🏆:
                    +   Best overall: Version 2 (CTE)
                        -   Most readable
                        -   Easy to debug
                        -   Scales well when logic becomes more complex

                    +   Best for short queries or interviews: Version 1                                                                         
                        
                        ⚠️ Performance-wise, SQL Server usually generates the same execution plan for all three
                                assuming proper indexes (Orders.customer_id).                                                                                                           */

--                                      11-2-2-2  #2 IMPROVE THE READABILITY:
/*
                [PROMPT...]: 
                    
                    The following SQL Server query is long and hard to understand.
                    Do the following:
                        Improve its readability.
                        Remove any redundancy in the query and consolidate it.
                        Include comments but avoid commenting on obvious parts.
                        Explain each improvement to understand the reasoning behind it.
                    [SQL QUERY GOES HERE ]

*/
--                                      11-2-2-3  #3 OPTIMIZE PERFORMANCE QUERY:
/*

               [PROMPT ... ]
                    
                    The following SQL Server Query is slow.
                    Do the following:
                        Propose optimizations to improve its performance.
                        Provide the improved SQL Query.
                        Explain each improvement to understand the reasoning behind it.
                    [SQL QUERY GOES HERE ]
*/

--                                      11-2-2-4  #4 OPTIMIZE EXECUTION PLAN:
/*
               [PROMPT ... ]

                    The image is the execution plan of SQL Server query.
                    Do the Following: 
                        Describe the execution plan step by step.
                        Identify performance bottlenecks and issues.
                        Suggest ways to improve performance and optimize the execution Plan
                    [SQL QUERY GOES HERE ]
*/

--                                      11-2-2-5  #5 DEBUGGING:

/*
               [PROMPT ... ]
                        
                     The following SQL Server Query causing this error: [ Error Message GOES HERE ] 
                     Do the Following:
                            Explain the error message.
                            Find the root cause of this issue.
                            Suggest how to fix it.
                    [SQL QUERY GOES HERE]
*/

--                                      11-2-2-6  #6 EXPLAIN THE RESULT:
/*

               [PROMPT ... ]

                    I didn't understand the result of the following SQL Server Query.
                    Do the following:
                            Break down how SQL processes the following query Step By Step.
                            Explain each stage and how the result is formed.
                    [SQL QUERY GOES HERE]
*/

--                                      11-2-2-7  #7 SYLING & FORMATTING :
/*

               [PROMPT ... ]

                    The following SQL Server Query hard to understand.
                    Do the following: 
                            Restyle the code to make it easier to read.
                            Align Column Aliases.
                            Keep it Compact - do not introduce unecessary new Lines.
                            Ensure the formmating follows best practices.
                    [SQL QUERY GOES HERE]

*/

--                                      11-2-2-8  #8 DOCUMENTATION & COMMENTS:
/*

               [PROMPT ... ]

                    The following SQL Server Query lacks comments and documentation.
                    Do the following: 
                            Insert a leading comment at the start ot the query describing its overall purpose.
                            Add comments only where clarification is necessary, avoiding obvious statement.
                            Create a separate document explaining the business rules implemented by the query.
                            Create another separate document describing how the query works.
                    [SQL QUERY GOES HERE]

 */

--                                      11-2-2-9  #9 IMPROVE DATABASE DDL:
/*

               [PROMPT ... ]
                
                    The following SQL Server DDL Script hat to be optimized.
                    Do the following:
                            Naming: Check the consistency of table/column names, prefixes, standards.
                            Data Type: Ensure data types are appropriate and optimized.
                            Integrity: Verfiy the integrity of primary keys and foreign keys.
                            Indexes: Check that indexes are sufficient and avoid redundancy.
                            Normalization: Ensure proper normalization and avoid redundancy.
                    [SQL QUERY GOES HERE]

 */

--                                      11-2-2-10  #10 GENERATE TEST DATABASE:
/*
               [PROMPT ... ]
                    
                        I need dataset for testing for the following SQL Server DDL: 
                        Do the following: 
                                Generate test dataset as insert Statements.
                                Dataset should be realstic.
                                Keep the dataset small.
                                Ensure all primary/foreign key relationships are valid (use matching IDs)
                                Dont introduce any Null values.
                    [SQL QUERY GOES HERE]

 */

--                                  11-2-2  FOR LEARNING 5x TOP PROMPTs:


--                                      11-2-3-1 #11 CREATE SQL COURSE FOR LEARNING:
/*
               [PROMPT ... ]
                        
                         Create a comprehensive SQL course with a detailed roadmap and agenda.
                         Do the following: 
                                Start with SQL fundamentals and aadvance to Complex topics.
                                Make it beginner-friendly.
                                Include topics relevent to data analytics.
                                Focus on real-world data analytics use cases ans scenarios
*/

--                                      11-2-3-2 #12 UNDERSTAND SQL CONCEPT:
/*
               [PROMPT ... ]
                    
                        I want detailed explanation about SQL Window Functions.
                        Do the following: 
                                Explain what Window Functions are.
                                Give an analogy.
                                Descripe why we need them and when to use them.
                                Explain the syntax.
                                Provide simple examples 
                                List the Top 3 use Cases.

*/

--                                      11-2-3-3 #13 COMPARING SQL CONCEPTs:
/*
               [PROMPT ... ]
                        
                        I want to understand the differences between SQL Winndows and GROUP BY.
                        Do the Following: 
                                Explain the Key differences between the two concepts.
                                Describe when to use each concept, with examples.
                                Provide the pros and cons of each concept.
                                Summarize the comparison in a cleare side-by-side table
 */

--                                      11-2-3-4 #14 PRACTICE SQL:
/*
               [PROMPT ... ]                                           
                        
                        Act as an SQL trainer and help me practice SQL Window Functions.
                        Do the following:
                                Make it interactive Practing, you provide task and give solution.
                                Provide a sample dataset.
                                Give SQL tasks that gradually increase in difficulty.
                                Act as an SQL Server and show the result of my Queries.
                                Review my queries, procide feedback, and suggest improvements.
 */

--                                      11-2-3-5 #15 PREPARE FOR A SQL INTERVIEW:
/*
               [PROMPT ... ] 
                        
                         Act as interviewer and prepate me for a SQL interview.
                         Do the following: 
                                Ask common SQL interview questions.
                                Make it interactive Practicing questions and give answer.
                                Gradually progress to advanced topics.
                                Evalute my answer and give me a feedback
*/ 


/*
========================================================================================================================================================================

	                                                        CHAPTER 12: SQL PROJECTS

========================================================================================================================================================================                                */
*/

--                                12-1 DATA WAREHOUSE PROJECTS:

/*
            +   INTRO: 
                        +-----------------------------+        +-----------------------------+        +-----------------------------+   
                        |            DATA             |        |  Exploratory Data Analysis  |        |           Advanced          |
                        |           WERHOUSE          |        |            (EDA)            |        |        Data Analytics       |
                        +-----------------------------+        +-----------------------------+        +-----------------------------+
                            " Orginze, Structure, Prepare,              " Understand Data "                " Answer Business Questions "

                            |-> Data Architecture                  |-> Basics Queries                   |-> Complex Queries
                            |-> Data Integration                   |-> Data Profiling                   |-> Window Functions 
                            |-> Data Cleansing                     |-> Simple Aggregations              |-> CTE 
                            |-> Data Load                          |-> Subquer                          |-> Subqueries
                            |-> Data Modeling                                                           |-> Reports

            + DEFs : 

                        +   Data Warhouse:  
                                -   A sbject-Oriented, integrated, time-variant, and non-volatile collection
                                        of data in support of management's decision-making process.
                                -   A data warehouse is a centralized database designed for:
                                            .   Reporting.
                                            .   Analysis
                                            .   Business Inteligence(BI)
                                -   it is: 
                                            .   Read-heavy
                                            .   Historical
                                            .   Optimized for queiries, not transactions
                                -   The Data Warehouse is the destination.

 
                        +   ETL : (Extract Transform Load)
                                -   Extract: Get Data for Source Systems( ERP, CRM, flat files, APIs, OLTP databases...)
                                -   Transform: Clean, Validate, enrich, and reshape data.
                                -   Load: Store the transformed data into a target system( usually a Data Warehouse)

--                                    12-1-1 DATA WAREHOUSE PROJECTS: ETL



                                                                                                                                    +----------------+
                                                                                                                                    |       ETL      |
                                                                                                                                    +----------------+
                                                                                                                                             |
                                                               +-----------------------------------------------------------------------------+-----------------------------------------------------------------------------+
                                                               |                                                                             |                                                                             |
                                                      +--------+--------+                                                            +---------------+                                                             +-------+-------+
                                                      |   Extraction    |                                                    +-------|   Transform   |                                                             |      LOAD     |
                                                      +--------+--------+                                                    |       +---------------+                                                             +--------+------+ 
                                                               |                                                             |                                                                                              |
                        +--------------------------------------+--------------------------------------+                      |  +-----------------+                                  +--------------------------------------+--------------------------------------+ 
                        |                                      |                                      |                      |  | Data            |                                  |                                      |                                      |
                +-------+-------+                      +-------+-------+                      +-------+-------+              |->|     Enrichement |                          +-------+-------+                      +-------+-------+                      +-------+-------+  
                |   Extraction  |                      |    Extract    |                      |    Extract    |              |  +-----------------+                          |   Processing  |                      |Slowly Changing|                      |     Load      | 
        +-------|    Methodes   |              +-------|     Types     |              +-------|   Technique   |              |  +-----------------+                  +-------|     Types     |              +-------|   Techniques  |              +-------|   Methodes    |
        |       +-------+-------+              |       +-------+-------+              |       +-------+-------+              |  | Data            |                  |       +-------+-------+              |       +-------+-------+              |       +-------+-------+ 
        |                                      |                                      |                                      |->|     Integration |                  |   +---------------+                  |   +---------------+                  |  +---------------+               
        |   +---------------+                  |   +---------------+                  |  +-------+-------+                   |  +-----------------+                  |   | Batch         |                  |-->| SCD 0 : No    |                  |  | Full          |--------+
        |   | Push          |                  |   | Full          |                  |  | Manual  Data  |                   |  +------------------+                 |-->|    Processing |                  |   | Historization |                  |->|          Load |        |   +---------------+
        |-->|    Extraction |                  |-->|    Extraction |                  |->|   Extractions |                   |  |Data Normalization|                 |   +---------------+                  |   +---------------+                  |  +---------------+        |   | Truncate &    |
        |   +---------------+                  |   +---------------+                  |  +---------------+                   |->| & Standardization|                 |   +---------------+                  |   +---------------+                  |                           |-->|        Insert |
        |   +---------------+                  |   +---------------+                  |  +---------------+                   |  +------------------+                 |   | Stream        |                  |-->| SCD 1 :       |                  |                           |   +---------------+
        |   | Pull          |                  |   | Incremental   |                  |  | Database      |                   |  +------------------+                 |-->|    Processing |                  |   |     Overwrite |                  |                           |   +---------------+
        |-->|    Extraction |                  |-->|    Extraction |                  |->|      Querying |                   |  | Business Rules & |                     +---------------+                  |   +---------------+                  |                           |-->|     Upsert    |
            +---------------+                      +---------------+                  |  +---------------+                   |->|       Logic      |                                                        |   +---------------+                  |                           |   +---------------+
                                                                                      |  +---------------+                   |  +------------------+                                                        |-->| SCD 2 :       |                  |                           |   +---------------+
                                                                                      |  | File          |                   |  +------------------+                                                        |   | Historization |                  |                           |   | Drop, Create, |
                                                                                      |->|       Parsing |                   |  | Derived          |                                                        |   +---------------+                  |                           |-->|        Insert |
                                                                                      |  +---------------+                   |->|          Columns |                                                        |   +---------------+                  |                               +---------------+
                                                                                      |  +---------------+                   |  +------------------+                                                        |-->|   SCD .  .  . |                  |  +---------------+ 
                                                                                      |  | API           |                   |  +------------------+                                                            +---------------+                  |  | Incremental   |--------+
                                                                                      |->|         calls |                   |  | Data             |                                                                                               |->|          Load |        |   +---------------+
                                                                                      |  +---------------+                   |->|     Aggregations |                                                                                                  +---------------+        |-->|     Upsert    |
                                                                                      |  +---------------+                   |  +------------------+                                                                                                                           |   +---------------+
                                                                                      |  | Event Based   |                   |  +------------------+                                                                                                                           |   +---------------+
                                                                                      |->|     Streaming |                   |  | Data             |-------+                                                                                                                   |-->|    Append     |    
                                                                                      |  +---------------+                   |->|        Cleansing |       |  +------------------+                                                                                             |   +---------------+
                                                                                      |  +---------------+                      +------------------+       |  | Remove           |                                                                                             |   +---------------+
                                                                                      |->|      CDC      |                                                 |->|       Duplicates |                                                                                             |-->|     Merge     | 
                                                                                      |  +---------------+                                                 |  +------------------+                                                                                                 +---------------+
                                                                                      |  +----------------+                                                |  +------------------+
                                                                                      |  | Wep            |                                                |  | Outlier          |
                                                                                      |->|       Scraping |                                                |->|        Detection | 
                                                                                         +----------------+                                                |  +------------------+
                                                                                                                                                           |  +------------------+
                                                                                                                                                           |  | Data             |
                                                                                                                                                           |->|        Filtering |
                                                                                                                                                           |  +------------------+
                                                                                                                                                           |  +------------------+
                                                                                                                                                           |  | Data Type        |
                                                                                                                                                           |->|         Casting  |
                                                                                                                                                           |  +------------------+
                                                                                                                                                           |  +------------------+
                                                                                                                                                           |  | Handling Missing |
                                                                                                                                                           |->|             Data |
                                                                                                                                                           |  +------------------+
                                                                                                                                                           |  +------------------+
                                                                                                                                                           |  | Handling         |
                                                                                                                                                           |->|   Invalid Values |
                                                                                                                                                           |  +------------------+
                                                                                                                                                           |  +------------------+
                                                                                                                                                           |  |Handling unwanted |
                                                                                                                                                           |->|           Spaces |
                                                                                                                                                           |  +------------------+

                                            NB: 
                                                For our project we will use for: 
                                                    -   Extraction: 
                                                                .   Methode : Pull Extraction 
                                                                .   Type: Full Extraction 
                                                                .   Techniques: File Parsing 
                                                    -   Transformation: All Types.
                                                    -   Load:
                                                                .   Processing Type: Batch Processing.
                                                                .   load Methods: Full Load : Truncte & Insert.
                                                                .   SCD: SCD1 Overwrite.    */


--                                    12-1-2 DATA WAREHOUSE PROJECTS: Project Materials 

/* 
                                    -   #1 Step Downlod Datasets: 
                                                   Link Of Download File : https://academy.datawithbaraa.com/digital-products/enrolled/930511?purchased=6501934&purchased_list_price=0&final_price=0&currency=USD&payment_method=stripe&user_id=126601916&purchased_at=1770745679&is_recurring=false&sale_id=190570034&tax_charge=0&csidebar=false&purchased_digital_product_id=930511&admission_id=2023563
                                    -   #2 St^p : Link to Notion : https://www.notion.com/templates/sql-data-warehouse-project                                                                                                                     
*/

--                                    12-1-3 DATA WAREHOUSE PROJECTS: REQUIREMENTS
/* 

                Building the data werhouse (Data Engineering ) 

                Objective 
                    Develop a modern Data Warehouse using SQL Server to consolidate Sales data, enabling analytical reporting 
                        and informed decision-making.
                Specifications: 
                        -   Data Sources: Importe data from two sources systeme (ERP and CRM) provided as CSV files.
                        -   Data Quality: Cleanse and resolve data quality issues prior to analysis.
                        -   Integration: Combine both sources into a single, user-friendly data model 
                                                    designed for analytical queries.
                        -   Scope: Focus on the latest dataset only; historization of data is not required.
                        -   Documentation: Provide clear documentation of the data model to support both business 
                                                        stakeholders and analytics tables
*/

--                                    12-1-4 DATA WAREHOUSE PROJECTS: DESIGN DATA ARCHITECTURE

--                                              12-1-4-1 DESIGN DATA ARCHITECTURE: CHOOSE THE RIGHT APPROACH
   /*
                                   +    DATA ARCHITECTEUR TYPES: 
                                               -    Data Wherhouse 
                                               -    Data Lake
                                               -    Data Lakehouse 
                                               -    Data Mesh     
                                           
                                   +    Data Wherhouse APPROACHES : 

                                            Inmon  --->   Stage   --->   EDW (3NF)   --->   Data Marts   --->    Power BI ... 
                                            
                                            Kimball  --->   Stage   ------------------->   Data Marts   --->    Power BI ...
                                            
                                            Data Vault  --->   Stage   ---> Raw Vault ---> Business Vault  --->   Data Marts   --->    Power BI ...  
                                            
                                            Medallion Architecture -->  Bronze --->  silver ---> Gold  ---> Power BI ... 
                           */                 
                           +------------->  IN OUR Project we will use Medallion Architecture Approache 
                                
--                                              12-1-4-2 DESIGN DATA ARCHITECTURE: DESIGN THE LAYERS
/*


                                    +-------------------------+     +-------------------------+     +-------------------------+
                                    |       Bronze Layer      |     |       Silver Layer      |     |       Gold Layer        |
                                    +-------------------------+     +-------------------------+     +-------------------------+
        +-------------------------+ +-------------------------+     +-------------------------+     +-------------------------+
        |        Defintion        | |   Row, unprocessed data |     |   Clean & Standardized  |     |     Business-Ready      |
        |                         | |     as-is from sources  |     |           data          |     |          data           |
        +-------------------------+ +-------------------------+     +-------------------------+     +-------------------------+
        +-------------------------+ +-------------------------+     +-------------------------+     +-------------------------+
        |                         | |     Traceabilitiy       |     |   (Intermediate Layer)  |     | Provide data to be cons-|
        |       Objective         | |           &             |     |      Prepare Data       |     |   umed for reporting &  |
        |                         | |       Debugging         |     |      For Analysis       |     |         Analytics       |
        +-------------------------+ +-------------------------+     +-------------------------+     +-------------------------+
        +-------------------------+ +-------------------------+     +-------------------------+     +-------------------------+
        |      Object  Types      | |           Tables        |     |           Tables        |     |          Views          |
        +-------------------------+ +-------------------------+     +-------------------------+     +-------------------------+
        +-------------------------+ +-------------------------+     +-------------------------+     +-------------------------+
        |        Load             | |        Full Load        |     |        Full Load        |     |          NONE           |
        |      Methode            | |   (Truncate & insert )  |     |   (Truncate & insert )  |     |                         |
        +-------------------------+ +-------------------------+     +-------------------------+     +-------------------------+
        +-------------------------+ +-------------------------+     +-------------------------+     +-------------------------+
        |                         | |                         |     |   - Data Cleaning       |     |                         |
        |                         | |                         |     |   - Data Standarization |     |   - Data Integration    |
        |          Data           | |       Non  (as-is)      |     |   - Data Normalization  |     |   - Data Aggregation    |                  
        |     Transformation      | |                         |     |   - Derived Columns     |     |   - Business Logic &    |
        |                         | |                         |     |   - Data Enrichment     |     |           rules         |
        +-------------------------+ +-------------------------+     +-------------------------+     +-------------------------+
        +-------------------------+ +-------------------------+     +-------------------------+     +-------------------------+
        |         Data            | |           None          |     |           None          |     |   - Start schema        |
        |       Modeling          | |         ( as-is )       |     |         ( as-is )       |     |   - Aggregated Objects  |
        |                         | |                         |     |                         |     |   - Flat Tables         |
        +-------------------------+ +-------------------------+     +-------------------------+     +-------------------------+
        +-------------------------+ +-------------------------+     +-------------------------+     +-------------------------+
        |     Target audience     | |    - Data Engineers     |     |   - Data Analysts       |     |   - Data Analysts       |
        |                         | |                         |     |   - Data Engineers      |     |   - Business Users      | 
        +-------------------------+ +-------------------------+     +-------------------------+     +-------------------------+
 */  
--                                              12-1-4-3 DESIGN DATA ARCHITECTURE:DRAW THE ARCHITECTURE -Draw.io-

--                                  Link for file draw : C:\Users\THINKPAD\Desktop\SQL\architucteur.drawio


--                                    12-1-5 DATA WAREHOUSE PROJECTS: PROJECT INITIALIZATION


--                                              12-1-5-1 PROJECT INITIALIZATION: CREATE DETAILS PROJECT TASKS (NOTION)

--                                  Link for file Notion  : https://www.notion.so/Data-Warehouse-Project-303c753809d480b9b3d6fbbd022d5fdb?source=copy_link


--                                              12-1-5-2 PROJECT INITIALIZATION: DEFINE PROJECT NAMING CONVENTIONS
/* 
        Define Naming Conventions: 
            Name Conventions: Set of rules or Guidelines for naming anything in the project:
                    -   Database
                    -   Schema
                    -   Tables
                    -   Store Procedures..
            Generale Principales:
                    .   Naming Convention: Use snake_case, with lowercase letters and underscores(_) to seprate words.
                    .   Language: Use English for all names.
                    .   Avoide Reserved Words: Do not use SQL reserved words as object names.
                    
            Table Naming Conventions: 
                    +   Bronze Rules: 

                            -   All names must start with the source systeme name, and table names must match 
                                    their original names without renaming.
                            -   <sourcesystem>_<entity> : 
                                        .   <sourcesystem> : Name of the source systeme (e.g, crm, erp).
                                        .   <entity> : Exact table name froum the source system.
                                        .   Exemple: crm_customer_info -->  Customer information from the CRM system.


                    +   Silver Rules: 

                            -   All names must start with the source name, and table nammes must match 
                                    thier names without renaming. 
                            -   <sourcesystem>_<entity> : 
                                        .   <sourcesystem> : Name of the source systeme (e.g, crm, erp).
                                        .   <entity> : Exact table name froum the source system.
                                        .   Exemple: crm_customer_info -->  Customer information from the CRM system.
                    +   Gold Rules: 
                            
                            -   All names must use meaningful, business-aligned names for tables, 
                                    starting with the category prefix.
                            -   <category>_<entity>
                                        .   <category>: Describes the role of the table, such as dim (dimension) 
                                                        or fact (fact table).
                                        .   <entity>: Descriptive name of the table, aligned with the business domain
                                                        ( e.g: customers, products, sales).
                                        .   Exemple: 
                                                    .   dim_customers -->   dimension table for customer data.
                                                    .   fact_sales -->  Fact Table Containing sales transactions.
                                        . Glossary of Category Patterns: 
                                            +-----------------+----------------------+-----------------------------------+
                                            |      Pattern    |       Meaning        |           Exemple(s)              |
                                            +-----------------+----------------------+-----------------------------------+
                                            | dim_            |    Dimension table   |     dim_Customer, dim_product     |
                                            +-----------------+----------------------+-----------------------------------+
                                            | fact_           |      Fact Table      |             fact_Sales            |
                                            +-----------------+----------------------+-----------------------------------+
                                            | agg_            |   Aggregated table   | agg_customers , agg_sales_monthly |
                                            +-----------------+----------------------+-----------------------------------+
            Column Naming Conventions: 

                        +   Surrogate Keys:
                                -   All primary keys in dimension tables must use the suffix _key.
                                -   <table_name>_key
                                        .   <table_name>: Refers to the name of the table or entity the key belongs to.
                                        .   _key: A suffix indicating that this column is a surrogate key.
                                        .   Exemple : customer_key -->  Surrogate key in the dim_customers table.
                        +   Technical Columns: 
                                -  Alltechnical columns must start with the prefex dwh_, followed by a descriptive 
                                      name indicating the column's purpose.
                                -  dwh_<column_name>
                                         .  dwh: Prefix exclusively for system-generated metadata.
                                         .  <column_name>:  Descriptive name indicating the column's purpose.
                                         .  Exemple: dwh_load_data  --> system-generated column used to store the data 
                                                                        when the record was loaded.
                        +   Stored Procedure: 
                                -   ll stored procedures used for loading data must follow the naming pattern: load_<layer>
                                         .  <layer>: Represent the layer being loaded, such as bronze, silver, or gold.
                                         .  Exemple:
                                                +   load_bronze --> Stored procedure for loading data into the Bronze Layer.
                                                +   load_silver --> Stored procedure for loading data into the silver  Layer.
 */

--                                           12-1-5-2 PROJECT INITIALIZATION: CREATE GIT REPO & PREPARAR THE REPO STRUCTURE

--          Link for Git Repo : https://github.com/medtayre/Sql_data_warehouse_project/   
    
    
--                                           12-1-5-3 PROJECT INITIALIZATION: CREATE DATABASE & SCHEMA 
                                        
/*
======================================================================================================
 Create Database and Schemas
======================================================================================================
 Script Purpose 
       This script creates a new database named 'DataWarehouse' after checking if it already exists.
       if the database exists, it is dropped and recreated. Additionally, the Script sets up three Schemas 
       Within the database: 'bronze', 'silver', and 'gold'.

WARNING:
       Running this script will drop the entire 'DataWarehouse' database if it exists.
       All data in the database will be prementaly deleted. Proceed with Caution
       and ensure you have proper backups before running this script.
*/

Use master;         --             Swith to master 
   GO
 
 -- Drop and recreate the 'DataWarehouse' database
 IF EXISTS (SELECT 1 FROM sys.databases WHERE name='DataWarehouse')
 BEGIN 
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE; 
    DROP DATABASE DataWerhouse; 
END;
GO

-- Create the 'DataWarehouse' database
 CREATE DATABASE DataWerhouse;
 GO

 USE DataWerhouse; 
 GO
  
-- Create Schemas 
 CREATE SCHEMA bronze;      -- Create the first Schema bronze
 GO
  
 CREATE SCHEMA silver;      -- Create the Second Schema Silver
 GO                         --  Go: Separate batches when working with multiple SQL statements

 CREATE SCHEMA gold;        -- Create the thired Schema Gold
 GO
 
 --                                           12-1-5-4  PROJECT INITIALIZATION: COMMIT CODE IN GIT REPO


 --     Link GIT of file SQL (init_database.sql): https://github.com/medtayre/Sql_data_warehouse_project/blob/main/Scripts/inti_database.sql

--                                    12-1-6 DATA WAREHOUSE PROJECTS: BRONZE LAYER 

/*
            +   #1 Analysing: Interview Source System Experts
            +   #2 Coding: Data Ingestion
            +   #3 Validating: Data Completeness & Schema Checks 
            +   #4  Docs & Version: Dataa Documenting versioning in GIT  
                 
*/
--                                          12-1-6-1    BRONZE LAYER: ANALYSE SOURCE SYSTEMS 
/*

        +   Business Context & Ownership:
                -   Who Owns the Data?
                -   What Business Process it Supports?
                -   System & Data Documentation
                -   Data Model & Data Catalog

        +   Architecture & Technology Stack:
                -   How is data Stored? (SQL Server, Oracle, AWS, AZUR ... )
                _   What are the integration capabilities? (API, KafKa, File Extract, Direct DB, ...)

        +   Extract & Load 
                -   Incremental vs. Full Load ?
                -   Data Scope & Historical Needs ?
                -   What is the expected size of the extracts ?
                -   Are there any data volume mimitations ?
                -   Authentification and authorization (token, SSH keys, VPN, IP whitelisting,... )
*/

--                                          12-1-6-2    BRONZE LAYER: CREATE DDL FOR TABLES 

/*
               +    Remember : DDL -->   Data Definition Language defines the structure of database tables
               
               +------------------+
               |  DATA PROFILING  |
               +------------------+---------------------------------------------+
               |    Explore the data to identify column names and data types    | 
               +----------------------------------------------------------------+

                        +   First table : 
                                -   Name : cust_info
                                -   columns: 
                                        .   cst_id --> INT 
                                        .   cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr -->  VARCHAR
                                        .   cst_create_date --> DATE


               +    Source of tbles system crm: C:\Users\THINKPAD\Desktop\SQL\SQL\sql-data-warehouse-project\datasets\source_crm



--               +    Create Table: #1 file cust_info                                                                                      */
IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL 
    DROP TABLE bronze.crm_cust_info;
GO

CREATE TABLE bronze.crm_cust_info (
    cst_id                  INT ,
    cst_key                 NVARCHAR(50),
    cst_firstname           NVARCHAR(50),
    cst_lastname            NVARCHAR(50),
    cst_material_status     NVARCHAR(50),
    cst_gndr                NVARCHAR(50),
    cst_create_date         DATE
)   

--               +    Create Table: #2 file prd_info 

IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL 
    DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info(
    prd_id          INT,
    prd_key         NVARCHAR(50),
    prd_nm          NVARCHAR(50),
    prd_cost        INT,
    prd_line        NVARCHAR(50),
    prd_start_dt    DATETIME,
    prd_end_dt      DATETIME          
    
)

--               +    Create Table: #3 file sales_details    

IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL 
    DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details(
   sls_ord_num      NVARCHAR(50),
   sls_prd_key      NVARCHAR(50),
   sls_cust_id      INT,
   sls_order_dt     INT,
   sls_ship_dt      INT,
   sls_due_dt       INT,
   sls_sales        INT,
   sls_quantity     INT,
   sls_price        INT
    
)

     --          +    Source of tbles system erp :  C:\Users\THINKPAD\Desktop\SQL\SQL\sql-data-warehouse-project\datasets\source_erp

--               +    Create Table: #4 file CUST_AZ12   

IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL 
    DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12(
   cid      NVARCHAR(50),
   bdate    DATE,
   gen      NVARCHAR(50)
)

--               +    Create Table: #5 file LOC_A101   

IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL 
    DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101(
   cid      NVARCHAR(50),
   cntry    NVARCHAR(50)
)

--               +    Create Table: #6 file PX_CAT_G1V2  

IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL 
    DROP TABLE bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2(
   id           NVARCHAR(50),
   cat          NVARCHAR(50),
   subcat       NVARCHAR(50) ,
   maintenance  NVARCHAR(50)
);
GO

--                                          12-1-6-3    BRONZE LAYER: DEVELOP SQL LOAD SCRIPTS
/*
            -   The methode we gonna use in order to load data for fils csv (source)  to our Datawarehous is:   
                                            BULK INSERT

                            File        ---------------- BLUK INSERT ----------------> Database  
                          txt, Csv                         all Data                      table


                          
                            File        ---------------- INSERT ----------------> Database  
                          txt, Csv                      row by row                table row                                                 */

-- Create Stored Procedure 
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    
    DECLARE @start_time DATETIME, @end_time DATETIME, @whole_batch_start_time DATETIME, @whole_batch_end_time DateTime ;
    BEGIN TRY 
        Set @whole_batch_start_time = GETDATE();
        PRINT '======================================================';
        PRINT 'Loading Bronze Layer';
        PRINT '======================================================';
        PRINT '------------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '------------------------------------------------------';
        
        SET @start_time = GETDATE();
         -- Load Data table #1: 
        PRINT   '>> Truncating Table bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info

        PRINT   '>> Inserting Data Into: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\THINKPAD\Desktop\SQL\SQL\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',  --  +-------------------+                       +---------------+
            TABLOCK                 --  |   File Delimiter  |                       |   TRUNCATE    |
        );                          --  +-------------------+-------------+         +---------------+-----------------------------------+
                                    --  |           , ; | # "             |         |  Quickly delete all rows from a table, resetting  |
                                    --  +---------------------------------+         |               it to an empty state                |
                                    --                                              +---------------------------------------------------+
        SET @end_time = GETDATE() 
        PRINT '>>> The Delay for loading  Table 1 is  ' + CAST (DATEDIFF(second,@end_time ,@start_time) AS NVARCHAR) + ' second'
        PRINT '>> ---------------------------------------------------------------'
        /*
                +----------------+
                |  QUALITY CHECK |
                +----------------+------------------------------------------------------+
                |   Check that the data hase not shifted and is the correct columns     |
                +-----------------------------------------------------------------------+
        */
     
        -- Load Data table #2: 
        SET @start_time = GETDATE();
        PRINT   '>> Truncating Table bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info

        PRINT   '>> Inserting Data Into: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\THINKPAD\Desktop\SQL\SQL\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE() 
        PRINT '>>> The Delay for loading  data of table 2 is  ' + CAST (DATEDIFF(second,@end_time ,@start_time) AS NVARCHAR) + ' Second'
        PRINT '>> ---------------------------------------------------------------'

        -- Load Data table #3: 
        SET @start_time = GETDATE();
        PRINT   '>> Truncating Table bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details

        PRINT   '>> Inserting Data Into: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\THINKPAD\Desktop\SQL\SQL\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE() 
        PRINT '>>> The Delay for loading  Table 3 is  ' + CAST (DATEDIFF(second,@end_time ,@start_time) AS NVARCHAR) + ' Second'
        PRINT '>> ---------------------------------------------------------------'

        PRINT '------------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '------------------------------------------------------';

        -- Load Data table #4: 
        SET @start_time = GETDATE();
        PRINT   '>> Truncating Table bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12

        PRINT   '>> Inserting Data Into: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\THINKPAD\Desktop\SQL\SQL\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE() 
        PRINT '>>> The Delay for loading Table 4  is  ' + CAST (DATEDIFF(second,@end_time ,@start_time) AS NVARCHAR) + ' Second'
        PRINT '>> ---------------------------------------------------------------'

        -- Load Data table #5: 
        SET @start_time = GETDATE();
        PRINT   '>> Truncating Table bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101

        PRINT   '>> Inserting Data Into: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\THINKPAD\Desktop\SQL\SQL\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE() 
        PRINT '>>> The Delay for loading Table 5 is  ' + CAST (DATEDIFF(second,@end_time ,@start_time) AS NVARCHAR) + ' Second'
        PRINT '>> ---------------------------------------------------------------'


        -- Load Data table #6: 
        SET @start_time = GETDATE();
        PRINT   '>> Truncating Table bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2

        PRINT   '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\THINKPAD\Desktop\SQL\SQL\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE() 
        PRINT '>>> The Delay for loading table 6 is  ' + CAST (DATEDIFF(second,@end_time ,@start_time) AS NVARCHAR) + ' Seconds'
        PRINT '=================================================='
        PRINT 'Loading Bronze Layer is Completed'
        SET @whole_batch_end_time = GETDATE();
        PRINT '   - Total Load Duration:  ' + CAST (DATEDIFF(second,@whole_batch_end_time ,@whole_batch_Start_time) AS NVARCHAR) + ' Seconds'
        PRINT '=================================================='
    END TRY
    BEGIN CATCH
        PRINT '==========================================='
        PRINT 'ERROR OCCURED DURING LOADING  BRONZE LAYER'
        PRINT 'Error Message ' + ERROR_MESSAGE();
        PRINT 'Error Message ' + CAST (ERROR_NUMBER() AS NVARCHAR);
        PRINT '==========================================='
    END CATCH
END

--                                          12-1-6-4    BRONZE LAYER: CREATE STORED PROCEDURE
 /*                 +--------+
                    |  HINT  |
                    +--------+----------------------------------------------------------+
                    |   Save frequently used SQL code in stored proceddures in database |
                    +-------------------------------------------------------------------+  
                    
                    +--------------+
                    |  ADD PRINTS  |
                    +--------------+---------------------------------------------------------+
                    |   Add prints to track execution, debug issues and understand its flow  |
                    +------------------------------------------------------------------------+  

                    +---------------------+
                    |  ADD TRY ... CATCH  |
                    +---------------------+-------------------------------------------------------------+
                    |  Ensures error handling, data integrity, and issue logging for easier debugging   |
                    |   SAL run the TRY block, and if fails, it runs the CATCH block to handle the erro |
                    +-----------------------------------------------------------------------------------+ 
                    
                    +---------------------+
                    |  Track ETL Duration |
                    +---------------------+-----------------------------------------------------------------+
                    |  Helps to identify bottlenecks, optimize performance, monitor trends, detecte issues  |
                    +---------------------------------------------------------------------------------------+ 
                    
                   */
                    
--                                          12-1-6-5    BRONZE LAYER:DOCUMENT DATA FLOW      
--          File Link: C:\Users\THINKPAD\Desktop\SQL\Bronze Layer Flow.drawio
                    

--                                    12-1-7    DATA WAREHOUSE PROJECTS: SILVER LAYER 
--                                          12-1-7-1    SILVER LAYER: ANALYSING ( EXPLORE & UNEDRSTAND DATA ( DRAW.IO))

SELECT TOP 1000 * FROM bronze.crm_cust_info
SELECT TOP 1000 * FROM bronze.crm_prd_info
SELECT TOP 1000 * FROM bronze.crm_sales_details

SELECT TOP 1000 * FROM bronze.erp_cust_az12         --birthday
SELECT TOP 1000 * FROM bronze.erp_loc_a101          --country
SELECT TOP 1000 * FROM bronze.erp_px_cat_g1v2       -- Product Categories

-- Patch for Diagram Draw.io ==> C:\Users\THINKPAD\Desktop\SQL\Diagramme_Integration_Model.drawio

--                                          12-1-7-2    SILVER LAYER: CREATE DDL FOR TABLES
/*
        METADATA COLUMNS:

            Extra column added by data engineers that do not originate from the source data.
            -   Create_date: The record's load timestamp.
            -   update_date: The record's last update timestamp.
            -   source_system: The origin System of the record.
            -   file_location: The file source of the record. 
*/

IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL 
    DROP TABLE silver.crm_cust_info;
GO

CREATE TABLE silver.crm_cust_info (
    cst_id                  INT ,
    cst_key                 NVARCHAR(50),
    cst_firstname           NVARCHAR(50),
    cst_lastname            NVARCHAR(50),
    cst_material_status     NVARCHAR(50),
    cst_gndr                NVARCHAR(50),
    cst_create_date         DATE,
    dwh_create_date         DATETIME2 DEFAULT GETDATE()          --  Metadata (Create date)
)   

--               +    Create Table: #2 file prd_info 

IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL 
    DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info(
    prd_id          INT,
    cat_id          NVARCHAR(50),
    prd_key         NVARCHAR(50),
    prd_nm          NVARCHAR(50),
    prd_cost        INT,
    prd_line        NVARCHAR(50),
    prd_start_dt    DATE,
    prd_end_dt      DATE,
    dwh_create_date         DATETIME2 DEFAULT GETDATE()          --  Metadata (Create date)
    
)

--               +    Create Table: #3 file sales_details    

IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL 
    DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details(
   sls_ord_num      NVARCHAR(50),
   sls_prd_key      NVARCHAR(50),
   sls_cust_id      INT,
   sls_order_dt     DATE,
   sls_ship_dt      DATE,
   sls_due_dt       DATE,
   sls_sales        INT,
   sls_quantity     INT,
   sls_price        INT,
    dwh_create_date         DATETIME2 DEFAULT GETDATE()          --  Metadata (Create date)

)

--               +    Create Table: #4 file CUST_AZ12   

IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL 
    DROP TABLE silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12(
   cid                      NVARCHAR(50),
   bdate                    DATE,
   gen                      NVARCHAR(50),
   dwh_create_date          DATETIME2 DEFAULT GETDATE()          --  Metadata (Create date)
)

--               +    Create Table: #5 file LOC_A101   

IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL 
    DROP TABLE silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101(
   cid                      NVARCHAR(50),
   cntry                    NVARCHAR(50),
   dwh_create_date          DATETIME2 DEFAULT GETDATE()          --  Metadata (Create date)

)

--               +    Create Table: #6 file PX_CAT_G1V2  

IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL 
    DROP TABLE silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2(
   id               NVARCHAR(50),
   cat              NVARCHAR(50),
   subcat           NVARCHAR(50) ,
   maintenance      NVARCHAR(50),
   dwh_create_date         DATETIME2 DEFAULT GETDATE()          --  Metadata (Create date)

);
GO


--                                          12-1-7-3    BUILD SILVER LAYER: CLEAN & LOAD 
--                                              12-1-7-3-1    CLEAN & LOAD: crm_cust_info

/*
        #1 Step:
                -   Check For Nulls or Duplicates in Primary Key
                -   Exception: No Result
                >>> Quality Check: 
                                    -   A Primary Key Must Be Unique And Not Null.
                                    -   Check for unwanted spaces in string values.
                                    -   Check the consistency of values in low cardinality columns 
                                    

*/


/*===============================================
                The Main Querie
===============================================*/

PRINT '>> Truncating Table: silver.crm_cust_info'
TRUNCATE TABLE silver.crm_cust_info;
PRINT '>> Inserting Data Into: silver.crm_cust_info'
INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_material_status,
    cst_gndr,
    cst_create_date )

    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname) AS cst_lastname,
    
        CASE WHEN UPPER(cst_material_status) = 'S' THEN 'Single'
             WHEN UPPER(cst_material_status) = 'M' THEN 'Married'
             ELSE 'n/a'
        END cst_material_status,
        CASE WHEN UPPER(cst_gndr) = 'F' THEN 'Female'
             WHEN UPPER(cst_gndr) = 'M' THEN 'Male'
             ELSE 'n/a'
        END cst_gndr,
        cst_create_date 
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL 
    )t  WHERE flag_last = 1        





-- We have the Three Columns share the same cst_id so that's not accepte for solution we use 
                                --          ROW_NUMBER () : Assign a unique number to each row in a result set, 
                                --              based on a defined order
                                

-- Check for unwanted Sapces: 
--  Expectation: No Results 
SELECT cst_lastname 
FROM bronze.crm_cust_info
WHERE cst_lastname  != TRIM(cst_lastname)    --  If the Original value is not equal to the same value after trimming, 
                                                --      it means there are spaces!
-- Data Standarization & consistency 
SELECT DISTINCT cst_gndr 
FROM bronze.crm_cust_info       -- In our Data Warehouse, We aim to store clear and meaningful values 
                                --  rather than using abbreviated terms 
 

--  Quality of silver: Re-run the quality check queries from the bronze layer to verify 
--              the quality of data in the silver Layer
  --    Check for dublicate Primary Key 
SELECT 
    cst_id,
    COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL 

-- Check For Unwanted Space 
SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)



/*
        SYNTAX: 

            +--------------------------+
            |  Remove Unwanted Spaces  |
            +--------------------------+-------------------------------------------------------------------+
            |   Removes unnecessary spaces to ensure data consistency, and uniformity across all records   |
            +----------------------------------------------------------------------------------------------+

            +----------------------------------------+
            |  Data Normalization & Standardization  |
            +----------------------------------------+------------------------+
            |  Maps coded values to meaningful, unser-friendly descriptions   |
            +-----------------------------------------------------------------+

            +-------------------------+
            | Handling Missing Data   |
            +-------------------------+-------------------------+
            |  Fills in the blanks by adding a default value    |
            +---------------------------------------------------+

            +---------------------+
            |  Remove Dublicates  |
            +---------------------+-------------------------------------------------------------------+
            |   Ensure only one record per entity by identifying and retaining the most relevant row. |
            +-----------------------------------------------------------------------------------------+

            
            


*/

--                                              12-1-7-3-2    CLEAN & LOAD: crm_prd_info

/*===============================================
                The Main Querie
===============================================*/

PRINT '>> Truncating Table: silver.crm_prd_info'
TRUNCATE TABLE silver.crm_prd_info;
PRINT '>> Inserting Data Into: silver.crm_prd_info'

INSERT INTO silver.crm_prd_info(
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT 
    prd_id,
    REPLACE(SUBSTRING (prd_key, 1, 5), '-','_') AS cat_id,      -- SUBSTRING: Extract a specific part of a string value 
    SUBSTRING (prd_key, 7, LEN(prd_key)) AS  prd_key,           -- LEN(): Return the number of characters in a string.
    prd_nm,
    ISNULL(prd_cost, 0) AS prd_cost,                            -- ISNULL(): Replace NULL value with a specified replacement
                                                                --              value we can also use COALESCE as well
    CASE UPPER(TRIM(prd_line))
         WHEN 'M' THEN 'Mountain'                               --  Quick Case WHEN :
         WHEN 'R' THEN 'Road'                                   --                      Ideal for simple value mapping
         WHEN 'S' THEN 'Other Sales' 
         WHEN 'T' THEN 'Touring' 
         ELSE 'n/a'
    END AS prd_line,
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    CAST(LEAD( prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info




--  Check For Nulls or Duplicates in Primary key
--  Excpectation: No Result

SELECT 
    prd_id,
    count(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- Check for unwanted spaces 
--  Excpectation: No Result
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- Check for NULLs or Negative Numbers 
--  Excpectation: No Result
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

--  Data Standarization & Consistency
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

-- Check for Invalid Date Orders
SELECT * 
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt

/*=====================================
    Check for invalidate Date orders ||
--===================================*/
SELECT 
    prd_id,
    prd_key,
    prd_nm,
    prd_start_dt,
    prd_end_dt,
    LEAD( prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS prd_end_dt
                --  LEAD(): Access values from the next row within a window 
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R','AC-HE-HL-U509')


/*      SYNTAX: 

                +-----------------+
                | Derived Columns |
                +-----------------+-----------------------------------------------------------------+
                |   Create new columns based on calculationsor transformations of existing ones.    |
                +-----------------------------------------------------------------------------------+

                +------------------+
                | Data Enrichement |
                +------------------+----------------------------------------------+
                |   Add new, relevant data to enhance the dataset for analysis    |
                +-----------------------------------------------------------------+
*/


--                                              12-1-7-3-3    CLEAN & LOAD: crm_sales_details




/*===============================================
                The Main Querie
===============================================*/

PRINT '>> Truncating Table: silver.crm_sales_details'
TRUNCATE TABLE silver.crm_sales_details;
PRINT '>> Inserting Data Into: silver.crm_sales_details'

INSERT INTO silver.crm_sales_details(
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,

    CASE WHEN sls_order_dt = 0  OR LEN(sls_order_dt) != 8 THEN NULL
         ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)   
    END AS sls_order_dt,

    CASE WHEN sls_ship_dt = 0  OR LEN(sls_ship_dt) != 8 THEN NULL
         ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)   
    END AS sls_ship_dt,

    CASE WHEN sls_due_dt = 0  OR LEN(sls_due_dt) != 8 THEN NULL
         ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)   
    END AS sls_due_dt,

    CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales !=ABS( sls_price) * sls_quantity 
                THEN ABS(sls_price) * sls_quantity 
        ELSE sls_sales
    END AS sls_sales,
    sls_quantity,
    CASE WHEN sls_price <= 0 OR sls_price IS NULL 
                    THEN sls_sales / NULLIF(ABS(sls_quantity), 0)
            ELSE sls_price
    END AS sls_price
FROM bronze.crm_sales_details
--==================================================================================================================


-- Check for invalid Dates (sls_order_dt): Negative numbers or zeros can't be cast to a date  
--  Check for outliers by validating the boundaries of the date range 
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt < 0  
      OR sls_order_dt = 0  
      OR LEN(sls_order_dt) != 8 
      OR sls_order_dt > 20500101
      OR sls_order_dt <  19000101


-- Check for  invalidate date (sls_ship_dt)
SELECT *
FROM bronze.crm_sales_details
WHERE sls_ship_dt < 0  
      OR sls_ship_dt = 0  
      OR LEN(sls_ship_dt) != 8 
      OR sls_ship_dt > 20500101
      OR sls_ship_dt <  19000101


-- Check for  invalidate date (sls_due_dt)
SELECT *
FROM bronze.crm_sales_details
WHERE sls_due_dt < 0  
      OR sls_due_dt = 0  
      OR LEN(sls_due_dt) != 8 
      OR sls_due_dt > 20500101
      OR sls_due_dt <  19000101


-- Check for invalid Date Orders
-- Order Date must always be earlier than the shipping Date or   Date 
    /*
    The correct order:

            |>--------------------------->|--------------------------->|
       sls_order_dt                   Ship Date                     Due Date 
     (customer place                (Company sends             (Customer recieves 
          order )                      product)                 product OR deadline)    */

SELECT 
    *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt or sls_ship_dt > sls_due_dt


-- Check Data Consistency: Between Sales, Quantity, and Price.
--  >> Sales = Quantity * Price 
--  >> Value must not be Null, Zero, or Negative 
/*
             +------------------+
             |  Business Rules  |
             +------------------+------------------------+
             |  Sales = Quantity * price                 |
             |  Negative, Zero, Nulls, are NOT ALLOWED ! |
             +-------------------------------------------+                                      */

SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_quantity,
    sls_price AS OLD_PRICE,
    CASE WHEN sls_price <= 0 OR sls_price IS NULL 
                    THEN sls_sales / NULLIF(ABS(sls_quantity), 0)
            ELSE sls_price
    END AS sls_price,
    sls_sales AS old_sales,
    CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales !=ABS( sls_price) * sls_quantity 
                THEN ABS(sls_price) * sls_quantity 
        ELSE sls_sales
    END AS sls_sales,
    (sls_quantity * sls_price) AS check_sales
FROM bronze.crm_sales_details
WHERE sls_quantity * sls_price != sls_sales
      OR sls_sales IS NULL  OR sls_price IS NULL OR sls_quantity IS NULL  
      OR sls_sales <= 0  OR sls_price <= 0  OR sls_quantity <= 0

/*                                                +---------+
             +------------------------------------|  RULES  |------------------------------------+
             |                                    +---------+                                    |
             |      -   If Sales is negative, zero, or null, derive it using Quantity and Price  |
             |      -   If price is Zero or null, calculate it using Sales and Quantity          |
             |      -   If price is negative, convert it to a positive value                     |
             +-----------------------------------------------------------------------------------+
*/


--                                              12-1-7-3-4    CLEAN & LOAD: erp_cust_az12

/*==================================================================
                             MAIN Querie
--=================================================================*/

PRINT '>> Truncating Table: silver.erp_cust_az12'
TRUNCATE TABLE silver.erp_cust_az12;
PRINT '>> Inserting Data Into: silver.erp_cust_az12'

INSERT INTO silver.erp_cust_az12 (
        cid,
        bdate,
        gen
)
SELECT
    CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4, LEN(cid))
         ELSE cid
    END AS cid,
    CASE WHEN  bdate > GETDATE() THEN NULL
        ELSE bdate
    END AS bdate,
    CASE WHEN UPPER(TRIM(gen)) IN( 'F', 'FEMALE') THEN 'Female'
         WHEN UPPER(TRIM(gen)) IN ( 'M' , 'MALE' ) THEN 'Male'
         ELSE 'n/a'
    END as gen
    
FROM bronze.erp_cust_az12

--=====================================================================


-- Identify Out-of-Range Dates
SELECT DISTINCT 
    bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate >  GETDATE()

-- Data Standarization & Consistency:
SELECT DISTINCT 
        CASE WHEN UPPER(TRIM(gen)) IN( 'F', 'FEMALE') THEN 'Female'
         WHEN UPPER(TRIM(gen)) IN ( 'M' , 'MALE' ) THEN 'Male'
         ELSE 'n/a'
    END as gen
FROM bronze.erp_cust_az12


--                                              12-1-7-3-5   CLEAN & LOAD: erp_loc_a101


/*==================================================================
                             MAIN Querie
--=================================================================*/

PRINT '>> Truncating Table: silver.erp_loc_a101'
TRUNCATE TABLE silver.erp_loc_a101;
PRINT '>> Inserting Data Into: silver.erp_loc_a101'
INSERT INTO silver.erp_loc_a101 (
    cid,
    cntry
)
SELECT 
    REPLACE(cid, '-','') AS cid,
    CASE WHEN UPPER(TRIM(cntry)) IN ('USA','US') THEN 'United States'
         WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
         WHEN UPPER(TRIM(cntry)) = '' OR cntry IS NULL  THEN 'n/a'
         ELSE TRIM(cntry)
    END AS cntry
FROM bronze.erp_loc_a101


-- Data Standarization & Consistency
SELECT  DISTINCT 
    cntry AS old_cntry, 
     CASE WHEN UPPER(TRIM(cntry)) IN ('USA','US') THEN 'United States'
         WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
         WHEN UPPER(TRIM(cntry)) = '' OR cntry IS NULL  THEN 'n/a'
         ELSE TRIM(cntry)
    END AS cntry
FROM bronze.erp_loc_a101
ORDER BY cntry



--                                              12-1-7-3-6   CLEAN & LOAD: erp_px_cat_g1v2


/*==================================================================
                             MAIN Querie
==================================================================*/
PRINT '>> Truncating Table: silver.erp_px_cat_g1v2'
TRUNCATE TABLE silver.erp_px_cat_g1v2;
PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2'
INSERT INTO silver.erp_px_cat_g1v2 (
   id,
   cat,
   subcat,
   maintenance
)
SELECT   
   id,
   cat,
   subcat,
   maintenance  
FROM bronze.erp_px_cat_g1v2 


-- Check unwanted spaces 
SELECT subcat 
FROM bronze.erp_px_cat_g1v2 
WHERE cat != TRIM(cat)

GO



--                                          12-1-7-3    BUILD SILVER LAYER: CREATE STORED PROCEDURE 

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN

    DECLARE @start_time DATETIME, @end_time DATETIME, @whole_batch_start_time DATETIME, @whole_batch_end_time DateTime ;
    BEGIN TRY 

        Set @whole_batch_start_time = GETDATE();
        PRINT '======================================================';
        PRINT 'Loading Silver Layer';
        PRINT '======================================================';
        PRINT '------------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '------------------------------------------------------';
        
        SET @start_time = GETDATE();

        -- CLEAN & LOAD: crm_cust_info
        PRINT '>> Truncating Table: silver.crm_cust_info'
        TRUNCATE TABLE silver.crm_cust_info;
        PRINT '>> Inserting Data Into: silver.crm_cust_info'
        INSERT INTO silver.crm_cust_info (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_material_status,
            cst_gndr,
            cst_create_date 
         )

        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname) AS cst_lastname,
    
            CASE WHEN UPPER(cst_material_status) = 'S' THEN 'Single'
                    WHEN UPPER(cst_material_status) = 'M' THEN 'Married'
                    ELSE 'n/a'
            END cst_material_status,
            CASE WHEN UPPER(cst_gndr) = 'F' THEN 'Female'
                    WHEN UPPER(cst_gndr) = 'M' THEN 'Male'
                    ELSE 'n/a'
            END cst_gndr,
            cst_create_date 
        FROM (
            SELECT 
                *,
                ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL 
        )t  WHERE flag_last = 1  
        
        SET @end_time = GETDATE() 
        PRINT '>>> The Delay for loading  Table 1 is  ' + CAST (DATEDIFF(second,@start_time,@end_time ) AS NVARCHAR) + ' second'
        PRINT '>> ---------------------------------------------------------------'
    
        -- CLEAN & LOAD: crm_prd_info
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_prd_info'
        TRUNCATE TABLE silver.crm_prd_info;
        PRINT '>> Inserting Data Into: silver.crm_prd_info'

        INSERT INTO silver.crm_prd_info(
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        SELECT 
            prd_id,
            REPLACE(SUBSTRING (prd_key, 1, 5), '-','_') AS cat_id,       
            SUBSTRING (prd_key, 7, LEN(prd_key)) AS  prd_key,           
            prd_nm,
            ISNULL(prd_cost, 0) AS prd_cost,                            
                                                               
            CASE UPPER(TRIM(prd_line))
                 WHEN 'M' THEN 'Mountain'                            
                 WHEN 'R' THEN 'Road'                                   
                 WHEN 'S' THEN 'Other Sales' 
                 WHEN 'T' THEN 'Touring' 
                 ELSE 'n/a'
            END AS prd_line,
            CAST(prd_start_dt AS DATE) AS prd_start_dt,
            CAST(LEAD( prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
        FROM bronze.crm_prd_info
        SET @end_time = GETDATE() 
        PRINT '>>> The Delay for loading  Table 2 is  ' + CAST (DATEDIFF(second,@start_time, @end_time ) AS NVARCHAR) + ' Second'
        PRINT '>> ---------------------------------------------------------------'

        --CLEAN & LOAD: crm_sales_details
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_sales_details'
        TRUNCATE TABLE silver.crm_sales_details;
        PRINT '>> Inserting Data Into: silver.crm_sales_details'
        INSERT INTO silver.crm_sales_details(
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )
        SELECT 
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,

            CASE WHEN sls_order_dt = 0  OR LEN(sls_order_dt) != 8 THEN NULL
                 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)   
            END AS sls_order_dt,

            CASE WHEN sls_ship_dt = 0  OR LEN(sls_ship_dt) != 8 THEN NULL
                 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)   
            END AS sls_ship_dt,

            CASE WHEN sls_due_dt = 0  OR LEN(sls_due_dt) != 8 THEN NULL
                 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)   
            END AS sls_due_dt,

            CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales !=ABS( sls_price) * sls_quantity 
                        THEN ABS(sls_price) * sls_quantity 
                ELSE sls_sales
            END AS sls_sales,
            sls_quantity,
            CASE WHEN sls_price <= 0 OR sls_price IS NULL 
                            THEN sls_sales / NULLIF(ABS(sls_quantity), 0)
                    ELSE sls_price
            END AS sls_price
        FROM bronze.crm_sales_details

        SET @end_time = GETDATE() 
        PRINT '>>> The Delay for loading  Table 3 is  ' + CAST (DATEDIFF(second,@start_time,@end_time ) AS NVARCHAR) + ' Second'
        PRINT '>> ---------------------------------------------------------------'
        

        PRINT '------------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '------------------------------------------------------';

        -- CLEAN & LOAD: erp_cust_az12
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_cust_az12'
        TRUNCATE TABLE silver.erp_cust_az12;
        PRINT '>> Inserting Data Into: silver.erp_cust_az12'

        INSERT INTO silver.erp_cust_az12 (
                cid,
                bdate,
                gen
        )
        SELECT
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4, LEN(cid))
                 ELSE cid
            END AS cid,
            CASE WHEN  bdate > GETDATE() THEN NULL
                ELSE bdate
            END AS bdate,
            CASE WHEN UPPER(TRIM(gen)) IN( 'F', 'FEMALE') THEN 'Female'
                 WHEN UPPER(TRIM(gen)) IN ( 'M' , 'MALE' ) THEN 'Male'
                 ELSE 'n/a'
            END as gen
    
        FROM bronze.erp_cust_az12

        SET @end_time = GETDATE() 
        PRINT '>>> The Delay for loading Table 4  is  ' + CAST (DATEDIFF(second,@start_time, @end_time ) AS NVARCHAR) + ' Second'
        PRINT '>> ---------------------------------------------------------------'

        --  CLEAN & LOAD: erp_loc_a101
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_loc_a101'
        TRUNCATE TABLE silver.erp_loc_a101;
        PRINT '>> Inserting Data Into: silver.erp_loc_a101'
        INSERT INTO silver.erp_loc_a101 (
            cid,
            cntry
        )
        SELECT 
            REPLACE(cid, '-','') AS cid,
            CASE WHEN UPPER(TRIM(cntry)) IN ('USA','US') THEN 'United States'
                 WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
                 WHEN UPPER(TRIM(cntry)) = '' OR cntry IS NULL  THEN 'n/a'
                 ELSE TRIM(cntry)
            END AS cntry
        FROM bronze.erp_loc_a101

        SET @end_time = GETDATE() 
        PRINT '>>> The Delay for loading Table 5 is  ' + CAST (DATEDIFF(second,@start_time,  @end_time ) AS NVARCHAR) + ' Second'
        PRINT '>> ---------------------------------------------------------------'

        --CLEAN & LOAD: erp_px_cat_g1v2
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_px_cat_g1v2'
        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2'
        INSERT INTO silver.erp_px_cat_g1v2 (
           id,
           cat,
           subcat,
           maintenance
        )
        SELECT   
           id,
           cat,
           subcat,
           maintenance  
        FROM bronze.erp_px_cat_g1v2 
        SET @end_time = GETDATE() 
        PRINT '>>> The Delay for loading table 6 is  ' + CAST (DATEDIFF(second,@start_time, @end_time ) AS NVARCHAR) + ' Seconds'
        PRINT '=================================================='
        PRINT 'Loading Silver Layer is Completed'
        SET @whole_batch_end_time = GETDATE();
        PRINT '   - Total Load Duration:  ' + CAST (DATEDIFF(second ,@whole_batch_Start_time, @whole_batch_end_time) AS NVARCHAR) + ' Seconds'
        PRINT '=================================================='
    END TRY
    BEGIN CATCH
        PRINT '==========================================='
        PRINT 'ERROR OCCURED DURING LOADING  BRONZE LAYER'
        PRINT 'Error Message ' + ERROR_MESSAGE();
        PRINT 'Error Message ' + CAST (ERROR_NUMBER() AS NVARCHAR);
        PRINT '==========================================='
    END CATCH 
END


EXEC silver.load_silver


 /* 
                                                    +-------------+
          +-----------------------------------------| CONSISTENCY |------------------------------------------+
          |                                         +-------------+                                          |
          |         If you interoduce an improvement, like better logging or error handling, in one stored   |
          |     procedures apply it to the others to maintain consistent standards and benefits.             |
          |                                                                                                  |
          +--------------------------------------------------------------------------------------------------+                                  */                                         
    

--                                          12-1-7-5    BUILD SILVER LAYER:DOCUMENT DATA FLOW


--                                          12-1-7-6    BUILD SILVER LAYER:COMMIT CODE IN Git Repo 


--                                    12-1-8     DATA WAREHOUSE PROJECTS: GOLD LAYER 

/*

    Maine Steps:


                  +-------------+                      +----------+                    +------------+                      +-----------------+
            +-----|  Analysing  |-------+        +-----|  Coding  |-----+        +-----| Validating |-------+        +-----| DOCS & Version  |-------+  
            |     +-------------+       |        |     +----------+     |        |     +------------+       |        |     +-----------------+       |
            |  Explore & Understand the |=======>|   Data integration   |=======>|    Data Integration      |=======>|          Documenting          |
            |      Business Objects     |        |                      |        |         Checks           |        |       Versioning in GIT       |
            +---------------------------+        +---+------------------+        +--------------------------+        +-------------------------------+
                                                     |
                                                     |-> Build the Business 
                                                     |       Object
                                                     |
                                                     |-> Choose Type Dimention
                                                     |        vs Fact
                                                     |
                                                     |-> Rename to friendly
                                                              names





--                                          12-1-8  GOLD LAYER: WHAT IS DATA MODELING ?
    /*

                             
        RAW DATA ----->    Organize and Structured    
                                   
                                   
        Organize and Structured : Putting Data in new Friendly and ease tu understand Objects eache one of them focus 
                                    on One information and DESCRIPE the relationship between this objects by connecting 
                                    them using lines ( Logical data Model)

        Stages : 
                    
                -   Conceptual Data Model: Focuses only on entities and relationship, with no details (BIG PICTURE).
                -   Logical Data Model: Focuses on columns, without many details for each column (BLUEPRINT).
                -   Physical Data Model: Includes all technical details (IMPLEMENTATION).
*/





--  Source:https://www.youtube.com/watch?v=SSKVgrwhzus&t=94123                                                                    

