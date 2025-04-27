/* 
=====================================================
              Create Silver Tables
=====================================================

This script creates tables in the silver schema.

=====================================================
*/

DO $$
BEGIN
    -- Silence Notices
    SET client_min_messages TO WARNING;
    
    RAISE INFO '======================================================';
    RAISE INFO '    Starting to create tables in the silver layer';
    RAISE INFO '======================================================';

    CREATE TABLE IF NOT EXISTS silver.crm_sales_details (
        sls_ord_num VARCHAR(50),
        sls_prd_key VARCHAR(50),
        sls_cust_id INTEGER,
        sls_order_dt DATE,
        sls_ship_dt DATE,
        sls_due_dt DATE,
        sls_sales FLOAT,
        sls_quantity INTEGER,
        sls_price FLOAT,
        dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    RAISE INFO 'Table silver.crm_sales_details created or already exists.';

    CREATE TABLE IF NOT EXISTS silver.crm_prd_info (
        prd_id INTEGER,
        cat_id VARCHAR(50),
        prd_key VARCHAR(50),
        prd_nm VARCHAR(50),
        prd_cost FLOAT,
        prd_line VARCHAR(50),
        prd_start_dt DATE,
        prd_end_dt DATE,
        dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    RAISE INFO 'Table silver.crm_prd_info created or already exists.';

    CREATE TABLE IF NOT EXISTS silver.crm_cust_info (
        cst_id INTEGER,
        cst_key VARCHAR(50),
        cst_firstname VARCHAR(50),
        cst_lastname VARCHAR(50),
        cst_marital_status VARCHAR(50),
        cst_gndr VARCHAR(50),
        cst_create_date DATE,
        dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    RAISE INFO 'Table silver.crm_cust_info created or already exists.';

    CREATE TABLE IF NOT EXISTS silver.erp_loc_a101 (
        cid VARCHAR(50),
        cntry VARCHAR(50),
        dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    RAISE INFO 'Table silver.erp_loc_a101 created or already exists.';

    CREATE TABLE IF NOT EXISTS silver.erp_px_cat_g1v2 (
        id VARCHAR(50),
        cat VARCHAR(50),
        subcat VARCHAR(50),
        maintenance VARCHAR(50),
        dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    RAISE INFO 'Table silver.erp_px_cat_g1v2 created or already exists.';

    CREATE TABLE IF NOT EXISTS silver.erp_cust_az12 (
        cid VARCHAR(50),
        bdate DATE,
        gen VARCHAR(50),
        dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    RAISE INFO 'Table silver.erp_cust_az12 created or already exists.';

    RAISE INFO '======================================================';
    RAISE INFO '        All tables created in the silver layer.';
    RAISE INFO '======================================================';
END;
$$;
