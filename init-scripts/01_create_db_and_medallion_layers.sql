/*
===================================================== 
           Create DB and Medallion Layers            
=====================================================

This script creates a database 'DataWarehouse'. 
Additionally, the script creates three schemas to 
implement the medallion architecture: bronze, 
silver and gold.

-----------------------------------------------------

WARNING: If the database 'DataWarehouse' exists, 
it will dropped and recreated. 
All data will be lost.

=====================================================
*/


-- Create datawarehouse database
SELECT 'CREATE DATABASE datawarehouse'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'datawarehouse')\gexec

-- Connect to datawarehouse
\c datawarehouse

-- Create medallion layers
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;