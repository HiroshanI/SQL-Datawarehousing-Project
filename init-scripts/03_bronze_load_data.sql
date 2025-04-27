/*
=====================================================
       Bulk Load Bronze Tables [csv -> bronze]
=====================================================

This script loads multiple CSV files into their
respective tables in the bronze schema.

-----------------------------------------------------

Warning: File paths must be accessible to the 
Postgres server (inside the container). Column orders 
must match exactly. Existing data will be truncated 
before loading.

=====================================================
*/

\c datawarehouse

DO $$
BEGIN
    -- Silence Notices
    SET client_min_messages TO WARNING;

    RAISE INFO '======================================================';
    RAISE INFO '       Starting Bulk Load into Bronze Tables';
    RAISE INFO '======================================================';

    -- Load bronze.crm_cust_info
    TRUNCATE TABLE bronze.crm_cust_info;
    RAISE INFO 'Truncated table bronze.crm_cust_info';

    COPY bronze.crm_cust_info
    FROM '/data/source_crm/cust_info.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
    RAISE INFO 'Loaded data into bronze.crm_cust_info';

    -- Load bronze.crm_prd_info
    TRUNCATE TABLE bronze.crm_prd_info;
    RAISE INFO 'Truncated table bronze.crm_prd_info';

    COPY bronze.crm_prd_info
    FROM '/data/source_crm/prd_info.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
    RAISE INFO 'Loaded data into bronze.crm_prd_info';

    -- Load bronze.crm_sales_details
    TRUNCATE TABLE bronze.crm_sales_details;
    RAISE INFO 'Truncated table bronze.crm_sales_details';

    COPY bronze.crm_sales_details
    FROM '/data/source_crm/sales_details.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
    RAISE INFO 'Loaded data into bronze.crm_sales_details';

    -- Load bronze.erp_cust_az12
    TRUNCATE TABLE bronze.erp_cust_az12;
    RAISE INFO 'Truncated table bronze.erp_cust_az12';

    COPY bronze.erp_cust_az12
    FROM '/data/source_erp/CUST_AZ12.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
    RAISE INFO 'Loaded data into bronze.erp_cust_az12';

    -- Load bronze.erp_loc_a101
    TRUNCATE TABLE bronze.erp_loc_a101;
    RAISE INFO 'Truncated table bronze.erp_loc_a101';

    COPY bronze.erp_loc_a101
    FROM '/data/source_erp/LOC_A101.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
    RAISE INFO 'Loaded data into bronze.erp_loc_a101';

    -- Load bronze.erp_px_cat_g1v2
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    RAISE INFO 'Truncated table bronze.erp_px_cat_g1v2';

    COPY bronze.erp_px_cat_g1v2
    FROM '/data/source_erp/PX_CAT_G1V2.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
    RAISE INFO 'Loaded data into bronze.erp_px_cat_g1v2';

    RAISE INFO '======================================================';
    RAISE INFO '         Finished Bulk Loading Bronze Tables';
    RAISE INFO '======================================================';
    
END;
$$;
