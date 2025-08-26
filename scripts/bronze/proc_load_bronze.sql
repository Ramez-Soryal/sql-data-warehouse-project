USE [DataWarehouse]
GO
/****** Object:  StoredProcedure [bronze].[load_bronze]    Script Date: 8/26/2025 11:36:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER   procedure [bronze].[load_bronze] as 
begin
	DECLARE @start_time datetime , @end_time datetime;
	declare @start_time_all datetime , @end_time_all datetime ;
	set @start_time_all = getdate(); 
	begin try 
		print '============================================='
		print ' Loading Bronze Layer'
		print '============================================='
	
		set @start_time = getdate();
		truncate table bronze.crm_cust_info;
		bulk insert bronze.crm_cust_info 
		from 'D:\SQL\sql with baraa\course material\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with(
			firstrow = 2 ,
			fieldterminator =',',
			tablock
			);
		set @end_time = getdate();
		print '>> LOADING DURATION: ' + cast( datediff(second , @start_time , @end_time) as nvarchar ) + ' seconds';  
		print '>> --------------------------------------------------- '

		set @start_time = getdate();	
		truncate table bronze.crm_prd_info;
		bulk insert bronze.crm_prd_info 
		from 'D:\SQL\sql with baraa\course material\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with(
			firstrow = 2 ,
			fieldterminator =',',
			tablock
			);
		set @end_time = getdate();
		print '>> LOADING DURATION: ' + cast( datediff(second , @start_time , @end_time) as nvarchar ) + ' seconds';  
		print '>> --------------------------------------------------- '

		set @start_time = getdate();
		truncate table bronze.crm_sales_details;
		bulk insert bronze.crm_sales_details 
		from 'D:\SQL\sql with baraa\course material\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with(
			firstrow = 2 ,
			fieldterminator =',',
			tablock
			);
		set @end_time = getdate();
		print '>> LOADING DURATION: ' + cast( datediff(second , @start_time , @end_time) as nvarchar ) + ' seconds';  
		print '>> --------------------------------------------------- '

		set @start_time = getdate();
		truncate table bronze.erp_cust_az12;
		bulk insert bronze.erp_cust_az12
		from 'D:\SQL\sql with baraa\course material\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with(
			firstrow = 2 ,
			fieldterminator =',',
			tablock
			);
		set @end_time = getdate();
		print '>> LOADING DURATION: ' + cast( datediff(second , @start_time , @end_time) as nvarchar ) + ' seconds';  
		print '>> --------------------------------------------------- '

		set @start_time = getdate();
		truncate table bronze.erp_loc_a101;
		bulk insert bronze.erp_loc_a101 
		from 'D:\SQL\sql with baraa\course material\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with(
			firstrow = 2 ,
			fieldterminator =',',
			tablock
			);
		set @end_time = getdate();
		print '>> LOADING DURATION: ' + cast( datediff(second , @start_time , @end_time) as nvarchar ) + ' seconds';  
		print '>> --------------------------------------------------- '

		set @start_time = getdate();
		truncate table bronze.erp_px_cat_g1v2;
		bulk insert bronze.erp_px_cat_g1v2 
		from 'D:\SQL\sql with baraa\course material\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with(
			firstrow = 2 ,
			fieldterminator =',',
			tablock
			);
		set @end_time = getdate();
		print '>> LOADING DURATION: ' + cast( datediff(second , @start_time , @end_time) as nvarchar ) + ' seconds';  
		print '>> --------------------------------------------------- '
		print '>> ======================================================================== ';
		print '>> ======================================================================== ';

		set @end_time_all = getdate() ;
		print '>>BRONZE LAYER LOADING DURATION: ' + cast( datediff(second , @start_time_all , @end_time_all) as nvarchar ) + ' seconds';  


	end try 
	begin catch 
		print'====================================';
		print'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'ERROR MESSAGE '+  ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
		print'====================================';

	end catch 
end
