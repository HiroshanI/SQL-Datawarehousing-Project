/*
=====================================================
         Load Silver Table [bronze -> silver]
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

    SET client_min_messages TO WARNING;

    RAISE INFO '=====================================================';
    RAISE INFO '            Starting load to silver tables';
    RAISE INFO '=====================================================';

    -----------------------------------------------------
    -- crm_cust_info
    -----------------------------------------------------

    TRUNCATE TABLE silver.crm_cust_info;
    RAISE INFO 'Truncated table silver.crm_cust_info';

    INSERT INTO silver.crm_cust_info (
        cst_id, 
        cst_key, 
        cst_firstname, 
        cst_lastname, 
        cst_marital_status, 
        cst_gndr,
        cst_create_date
    )
    SELECT 
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname)  AS cst_lastname,
        CASE 
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            ELSE 'N/A'
        END AS cst_marital_status,
        CASE 
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            ELSE 'N/A'
        END AS cst_gndr,
        cst_create_date 
    FROM (
        SELECT *, 
        ROW_NUMBER() OVER (
            PARTITION BY cst_id 
            ORDER BY cst_create_date DESC
        ) AS create_date_flag
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) AS c 
    WHERE create_date_flag = 1;  -- Get only the recent info  

    RAISE INFO 'Inserted data into silver.crm_cust_info';

    -----------------------------------------------------
    -- crm_prd_info
    -----------------------------------------------------

    TRUNCATE TABLE silver.crm_prd_info;
    RAISE INFO 'Truncated table silver.crm_prd_info';

    INSERT INTO silver.crm_prd_info (
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
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,  -- Extract category ID
        SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,      -- Extract product key
        prd_nm,
        COALESCE(prd_cost, 0) AS prd_cost, 
        CASE 
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'N/A'
        END AS prd_line, 
        CAST(prd_start_dt AS DATE) AS prd_start_dt,
        CAST(
            LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 day'
            AS DATE
        ) AS prd_end_dt  -- Calculate end date as one day before the next start date
    FROM bronze.crm_prd_info;

    RAISE INFO 'Inserted data into silver.crm_prd_info';

    -----------------------------------------------------
    -- crm_sales_details
    -----------------------------------------------------

    TRUNCATE TABLE silver.crm_sales_details;
    RAISE INFO 'Truncated table silver.crm_sales_details';

    INSERT INTO silver.crm_sales_details (
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
        
        -- Transform sls_order_dt
        CASE 
            WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
            ELSE TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD')
        END AS sls_order_dt,
        
        -- Transform sls_ship_dt
        CASE 
            WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
            ELSE TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD')
        END AS sls_ship_dt,
        
        -- Transform sls_due_dt
        CASE 
            WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
            ELSE TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD')
        END AS sls_due_dt,
        
        -- Recalculate sls_sales if original value is missing or incorrect
        CASE 
            WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
                THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END AS sls_sales,
        
        sls_quantity,
        
        -- Derive sls_price if original value is invalid
        CASE 
            WHEN sls_price IS NULL OR sls_price <= 0 
                THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE sls_price  
        END AS sls_price
        
    FROM bronze.crm_sales_details;

    RAISE INFO 'Inserted data into silver.crm_sales_details';

    -----------------------------------------------------
    -- erp_cust_az12
    -----------------------------------------------------

    TRUNCATE TABLE silver.erp_cust_az12;
    RAISE INFO 'Truncated table silver.erp_cust_az12';

    INSERT INTO silver.erp_cust_az12 (
        cid,
        bdate,
        gen
    )
    SELECT
        -- Transform cid by removing 'NAS' prefix if present
        CASE
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid FROM 4)  -- Remove 'NAS' prefix
            ELSE cid
        END AS cid, 
        
        -- Set future birthdates to NULL
        CASE
            WHEN bdate > CURRENT_DATE THEN NULL
            ELSE bdate
        END AS bdate,
        
        -- Normalize gender values to standard 'Female'/'Male'
        CASE
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'N/A'
        END AS gen
    FROM bronze.erp_cust_az12;

    RAISE INFO 'Inserted data into silver.erp_cust_az12';

    -----------------------------------------------------
    -- erp_px_cat_g1v2
    -----------------------------------------------------

    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    RAISE INFO 'Truncated table silver.erp_px_cat_g1v2';

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
    FROM bronze.erp_px_cat_g1v2;

    RAISE INFO 'Inserted data into silver.erp_px_cat_g1v2';


    -----------------------------------------------------
    -- erp_loc_a101
    -----------------------------------------------------

    TRUNCATE TABLE silver.erp_loc_a101;
    RAISE INFO 'Truncated table silver.erp_loc_a101';

    INSERT INTO silver.erp_loc_a101 (
        cid,
        cntry
    )
    SELECT
        REPLACE(cid, '-', '') AS cid, 
        CASE
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
            ELSE TRIM(cntry)
        END AS cntry
    FROM bronze.erp_loc_a101;

    RAISE INFO 'Inserted data into silver.erp_loc_a101';

    RAISE INFO '=====================================================';
    RAISE INFO '          Finished loading to silver tables';
    RAISE INFO '=====================================================';
END;
$$;

