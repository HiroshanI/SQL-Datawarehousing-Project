# PostgreSQL Data-Warehouse Project 



## Setup
```
docker compose up -d
```
## Connect to Postgres
```
docker ps  # get container-id
```
Connect into bash
```
docker exec -it <container-id> bash  # Entry point: bash
$ psql -U username -d datawarehouse
```
Connect into psql
```
docker exec -it <container-id> psql -U <username> -d datawarehouse  # Entry point: psql 
```
## EDA
Customer Info
```
select cst_id, count(*) 
from bronze.crm_cust_info 
group by cst_id 
having count(*) > 1 
order by count(*) DESC;

+--------+-------+
| cst_id | count |
|--------+-------|
| <null> | 4     |
| 29466  | 3     |
| 29473  | 2     |
| 29449  | 2     |
| 29433  | 2     |
| 29483  | 2     |
+--------+-------+
```
```
select distinct(cst_gndr), count(*) as "Row count" 
from bronze.crm_cust_info
group by cst_gndr; 
 
+----------+-----------+
| cst_gndr | Row count |
|----------+-----------|
| F        | 6848      |
| M        | 7068      |
| <null>   | 4578      |
+----------+-----------+
```
```
select cst_marital_status, count(*) as "Row count" 
from bronze.crm_cust_info 
group by cst_marital_status; 
 
+--------------------+-----------+
| cst_marital_status | Row count |
|--------------------+-----------|
| <null>             | 7         |
| S                  | 8474      |
| M                  | 10013     |
+--------------------+-----------+
```
```
SELECT cst_id, 
    ROW_NUMBER() OVER (
        PARTITION BY cst_id 
        ORDER BY cst_create_date DESC
    ) AS create_date_flag 
FROM bronze.crm_cust_info 
WHERE cst_id = 29466;

 cst_id | create_date_flag 
--------+------------------
  29466 |                1
  29466 |                2
  29466 |                3
  ```

  Product Info
```
select * from bronze.crm_prd_info where prd_key = 'AC-HE-HL-U509';

 prd_id |    prd_key    |         prd_nm          | prd_cost | prd_line | prd_start_dt | prd_end_dt 
--------+---------------+-------------------------+----------+----------+--------------+------------
    215 | AC-HE-HL-U509 | Sport-100 Helmet- Black |       12 | S        | 2011-07-01   | 2007-12-28
    216 | AC-HE-HL-U509 | Sport-100 Helmet- Black |       14 | S        | 2012-07-01   | 2008-12-27
    217 | AC-HE-HL-U509 | Sport-100 Helmet- Black |       13 | S        | 2013-07-01   | 
```  



