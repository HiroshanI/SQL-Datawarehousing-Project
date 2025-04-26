/*
=====================================================
        Create tables in bronze layer
=====================================================

This script creates tables in the bronze layer
to load data into.

-----------------------------------------------------

Template generated using: 
etl-scripts/utils/generate_ddl_script_template.py

=====================================================
*/


-- input-data/source_crm > sales_details.csv
CREATE TABLE IF NOT EXISTS bronze.crm_sales_details (
    sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id INTEGER,
    sls_order_dt INTEGER,
    sls_ship_dt INTEGER,
    sls_due_dt INTEGER,
    sls_sales INTEGER,
    sls_quantity INTEGER,
    sls_price INTEGER
);

-- input-data/source_crm > prd_info.csv
CREATE TABLE IF NOT EXISTS bronze.crm_prd_info (
    prd_id INTEGER,
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost FLOAT,
    prd_line VARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE
);

-- input-data/source_crm > cust_info.csv
CREATE TABLE IF NOT EXISTS bronze.crm_cust_info (
    cst_id INTEGER,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date DATE
);

-- input-data/source_erp > LOC_A101.csv
CREATE TABLE IF NOT EXISTS bronze.erp_loc_a101 (
    cid VARCHAR(50),
    cntry VARCHAR(50)
);

-- input-data/source_erp > PX_CAT_G1V2.csv
CREATE TABLE IF NOT EXISTS bronze.erp_px_cat_g1v2 (
    id VARCHAR(50),
    cat VARCHAR(50),
    subcat VARCHAR(50),
    maintenance VARCHAR(50)
);

-- input-data/source_erp > CUST_AZ12.csv
CREATE TABLE IF NOT EXISTS bronze.erp_cust_az12 (
    cid VARCHAR(50),
    bdate DATE,
    gen VARCHAR(50)
);
