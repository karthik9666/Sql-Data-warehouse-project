/*

====================================================================

Stored Procedure: Load Bronze Layer (Source -> Bronze)

====================================================================

Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:

    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv files to bronze tables.

Parameters:

    None. 
    This stored procedure does not accept any parameters or return any values.

Usage Example:
  EXEC bronze.load_bronze;

====================================================================

*/

CREATE OR ALTER PROCEDURE bronze.load_bronze As
BEGIN 
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY 
		SET @batch_start_time = GETDATE();
		print '=====================================';
		Print'Loading Bronze Layer' ;
		print '=====================================';
		-- Table 1
		print '-------------------------------------';
		print 'Loading CRM Tables';
		print '-------------------------------------';

		SET @start_time = GETDATE();
		print '>> Truncating Table : bronze.CRM_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;  -- it removes all stored table values not table schema.
		print '>> Inserting Data Into: bronze.crm_cust_info';
		Bulk Insert bronze.crm_cust_info
		FROM 'D:\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',', 
			TABLOCK 
		);
		SET @end_time = GETDATE();
		print '>> Load Duration: ' + CAST(datediff(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		print '>> ------------------';	
		 
		-- Table 2  
		SET @start_time = GETDATE();
		print '>> Truncating Table : bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;  
		print '>> Inserting Data Into: bronze.crm_prd_info';
		Bulk Insert bronze.crm_prd_info
		FROM 'D:\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE(); 
		print '>> Load Duration: ' + CAST(datediff(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		print '>> ------------------';
	
		-- Table 3  
		SET @start_time = GETDATE();
		print '>> Truncating Table : bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;  -- it removes all stored table values not table schema.
		print '>> Inserting Data Into: bronze.crm_sales_details';
		Bulk Insert bronze.crm_sales_details
		FROM 'D:\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',', 
			TABLOCK 
		);
		SET @end_time = GETDATE();
		print '>> Load Duration: ' + CAST(datediff(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		print '>> ------------------';

		print '-------------------------------------';
		print 'Loading ERP Tables';
		print '-------------------------------------';
		
		SET @start_time = GETDATE();
		print '>> Truncating Table : bronze.erp_cust_az12';
		-- Table 4 
		TRUNCATE TABLE bronze.erp_cust_az12;  
	
		print '>> Inserting Data Into: bronze.erp_cust_az12';
		Bulk Insert bronze.erp_cust_az12
		FROM 'D:\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',', 
			TABLOCK 
		);   
		SET @end_time = GETDATE();
		print '>> Load Duration: ' + CAST(datediff(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		print '>> ------------------';

		-- Table 5  
		SET @start_time = GETDATE();
		print '>> Truncating Table : bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101; 
		
		print '>> Inserting Data Into: bronze.erp_loc_a101';
		Bulk Insert bronze.erp_loc_a101
		FROM 'D:\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',', 
			TABLOCK  
		);
		SET @end_time = GETDATE();
		print '>> Load Duration: ' + CAST(datediff(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		print '>> ------------------';
	
		-- Table 6  
		SET @start_time = GETDATE();
		print '>> Truncating Table : bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2; 
		print '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		Bulk Insert bronze.erp_px_cat_g1v2
		FROM 'D:\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH ( 
			FIRSTROW = 2,
			FIELDTERMINATOR = ',', 
			TABLOCK 
		); 	
		SET @end_time = GETDATE();
		print '>> Load Duration: ' + CAST(datediff(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		print '>> ------------------'; 
		
		SET @batch_end_time = GETDATE();

		print '=====================================================';
		print 'Loading Bronze Layer is Completed';
		print '   - Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		print '=====================================================';
		
	END TRY 
	BEGIN CATCH 
		print '=====================================================';
		print 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		print 'Error Message' + Error_message();
		print 'Error Message' + CAST (Error_Number() As NVARCHAR); 
		print 'Error Message' + CAST (Error_State() As NVARCHAR);
		print '=====================================================';
	END CATCH	
END;
  
