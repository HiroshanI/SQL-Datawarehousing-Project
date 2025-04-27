/* 
=====================================================
              Create Bronze Tables
=====================================================

This script creates tables in the bronze schema.

=====================================================
*/

DO $$
BEGIN
    -- Silence Notices
    SET client_min_messages TO WARNING;
    
    RAISE INFO '======================================================';
    RAISE INFO '    Starting to create tables in the bronze layer';
    RAISE INFO '======================================================';

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
    RAISE INFO 'Table bronze.crm_sales_details created or already exists.';

    CREATE TABLE IF NOT EXISTS bronze.crm_prd_info (
        prd_id INTEGER,
        prd_key VARCHAR(50),
        prd_nm VARCHAR(50),
        prd_cost FLOAT,
        prd_line VARCHAR(50),
        prd_start_dt DATE,
        prd_end_dt DATE
    );
    RAISE INFO 'Table bronze.crm_prd_info created or already exists.';

    CREATE TABLE IF NOT EXISTS bronze.crm_cust_info (
        cst_id INTEGER,
        cst_key VARCHAR(50),
        cst_firstname VARCHAR(50),
        cst_lastname VARCHAR(50),
        cst_marital_status VARCHAR(50),
        cst_gndr VARCHAR(50),
        cst_create_date DATE
    );
    RAISE INFO 'Table bronze.crm_cust_info created or already exists.';

    CREATE TABLE IF NOT EXISTS bronze.erp_loc_a101 (
        cid VARCHAR(50),
        cntry VARCHAR(50)
    );
    RAISE INFO 'Table bronze.erp_loc_a101 created or already exists.';

    CREATE TABLE IF NOT EXISTS bronze.erp_px_cat_g1v2 (
        id VARCHAR(50),
        cat VARCHAR(50),
        subcat VARCHAR(50),
        maintenance VARCHAR(50)
    );
    RAISE INFO 'Table bronze.erp_px_cat_g1v2 created or already exists.';

    CREATE TABLE IF NOT EXISTS bronze.erp_cust_az12 (
        cid VARCHAR(50),
        bdate DATE,
        gen VARCHAR(50)
    );
    RAISE INFO 'Table bronze.erp_cust_az12 created or already exists.';

    RAISE INFO '======================================================';
    RAISE INFO '        All tables created in the bronze layer.';
    RAISE INFO '======================================================';
END;
$$;
