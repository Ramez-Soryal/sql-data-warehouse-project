create or alter procedure silver.load_silver as 
begin

	DECLARE @start_time datetime , @end_time datetime;
	declare @start_time_all datetime , @end_time_all datetime ;
	set @start_time_all = getdate(); 
	begin try 
			print '============================================='
			print ' Loading Silver Layer'
			print '============================================='
	
		set @start_time = getdate();
		truncate table silver.crm_cust_info;

		insert into silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
		)

		select 
		cst_id , 
		cst_key , 
		trim(cst_firstname) as cst_firstname ,
		trim(cst_lastname) as cst_lastname ,
		case when Upper(trim(cst_material_status)) = 'S' then  'Single'
		when Upper(trim(cst_material_status)) = 'M' then  'Married' 
		else 'n/a'
		end cst_material_status,
		case when Upper(trim(cst_gndr)) = 'F' then  'Female'
			 when Upper(trim(cst_gndr)) = 'M' then  'Male' 
			 else 'n/a'
			 end cst_gndr,
		cst_create_data
		from (
		select * , 
		ROW_NUMBER() over ( partition by cst_id order by cst_create_data desc ) last_flag
		from bronze.crm_cust_info )t
		where last_flag = 1 AND cst_id IS NOT NULL 
		
		set @end_time = getdate();
		print '>> LOADING DURATION: ' + cast( datediff(second , @start_time , @end_time) as nvarchar ) + ' seconds';  
		print '>> --------------------------------------------------- ';


		--=================================================

		--=================================================
		set @start_time = GETDATE();
		truncate table silver.crm_prd_info;

		insert into silver.crm_prd_info (
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt)
		select 
		prd_id,
		replace(SUBSTRING(prd_key , 1, 5)  , '-' ,'_') as cat_id ,
		substring(prd_key , 7 , len(prd_key)  ) as prd_key , 
		prd_nm,
		isnull(prd_cost,0) as prd_cost, 

		case when upper(trim(prd_line)) = 'R' then 'Road'
			 when upper(trim(prd_line)) = 'M' then 'Mountain'
			 when upper(trim(prd_line)) = 'S' then 'Other Sales'
			 when upper(trim(prd_line)) = 'T' then 'Touring'
			 Else 'n/a' 
		End prd_line,
		cast(prd_start_dt as date) as prd_start_dt,
		cast(LEAD(prd_start_dt) over ( partition by prd_key order by prd_start_dt )-1 as date) as prd_end_dt
		from bronze.crm_prd_info

			
		set @end_time = getdate();
		print '>> LOADING DURATION: ' + cast( datediff(second , @start_time , @end_time) as nvarchar ) + ' seconds';  
		print '>> --------------------------------------------------- ';


		--=================================================

		--=================================================
		set @start_time = GETDATE()
		truncate table  silver.crm_sales_details;

		insert into silver.crm_sales_details (
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales, 
		sls_quantity,
		sls_price ) 

		select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null 
			 else cast(cast(sls_order_dt as nvarchar) as date) 
		end as sls_order_dt,
		case when sls_ship_dt <= 0 or len(sls_ship_dt) != 8 then null 
			 else cast(cast(sls_ship_dt as nvarchar) as date) 
		end as sls_ship_dt,
		case when sls_due_dt <= 0 or len(sls_due_dt) != 8 then null 
			 else cast(cast(sls_due_dt as nvarchar) as date) 
		end as sls_due_dt,
		case when sls_sales <= 0 or sls_sales is null or sls_sales != sls_quantity * ABS(sls_price)
			 then sls_quantity * ABS(sls_price) 
			 else sls_sales
		end as sls_sales , 
		sls_quantity,
		case when sls_price is null or sls_price <= 0 
			 then sls_sales / nullif(sls_quantity , 0)
			 else sls_price 
		end as sls_price
		from bronze.crm_sales_details

			
		set @end_time = getdate();
		print '>> LOADING DURATION: ' + cast( datediff(second , @start_time , @end_time) as nvarchar ) + ' seconds';  
		print '>> --------------------------------------------------- ';


		--========================================================

		--========================================================
		set @start_time = GETDATE() ; 
		truncate table silver.erp_cust_az12;

		insert into silver.erp_cust_az12(
		cid , 
		bdate,
		gen ) 

		select 
		case when CID like 'NAS%' then SUBSTRING(CID , 4 , LEN(cid))
			 else CID
		end CID , 
		case when BDATE > GETDATE() then null 
			 else BDATE
		end BDATE ,
		case when UPPER(Trim(GEN)) in ( 'F' , 'FEMALE' ) then 'Female'
			 when UPPER(Trim(GEN)) in ( 'M' , 'MALE' ) then 'Male'
			 else 'N/A'
		end GEN

		from bronze.erp_cust_az12

			
		set @end_time = getdate();
		print '>> LOADING DURATION: ' + cast( datediff(second , @start_time , @end_time) as nvarchar ) + ' seconds';  
		print '>> --------------------------------------------------- ';



		--=================================================================

		--=================================================================
		set @start_time = GETDATE();
		truncate table silver.erp_loc_a101;

		insert into silver.erp_loc_a101 (
		cid,
		cntry
		)


		select 
		case when CID like 'AW%' then trim(REPLACE(CID , '-',''))
		end CID,
		case when upper(trim(CNTRY)) in ('US','USA','UNITED STATES') THEN 'United States'
			 when upper(trim(CNTRY)) in ('DE','GERMANY') THEN 'Germany'
			 when trim(CNTRY) = '' or trim(CNTRY) is  null then 'N/A' 
			 else trim(CNTRY)
		end CNTRY

		from bronze.erp_loc_a101

			
		set @end_time = getdate();
		print '>> LOADING DURATION: ' + cast( datediff(second , @start_time , @end_time) as nvarchar ) + ' seconds';  
		print '>> --------------------------------------------------- ';


		--============================================================


		--============================================================
		set @start_time = GETDATE();
		truncate table silver.erp_px_cat_g1v2;

		INSERT INTO silver.erp_px_cat_g1v2 (
		id,
		cat,
		subcat,
		maintenance 
		)

		select *
		from bronze.erp_px_cat_g1v2

			
		set @end_time = getdate();
		print '>> LOADING DURATION: ' + cast( datediff(second , @start_time , @end_time) as nvarchar ) + ' seconds';  
		print '>> --------------------------------------------------- ';


		set @end_time_all = getdate() ;
		print '>>Silver LAYER LOADING DURATION: ' + cast( datediff(second , @start_time_all , @end_time_all) as nvarchar ) + ' seconds';  
	
	end try 
	begin catch 
		print'====================================';
		print'ERROR OCCURED DURING LOADING SILVER LAYER';
		PRINT 'ERROR MESSAGE '+  ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
		print'====================================';

	end catch 
end 
