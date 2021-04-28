from create_infrastructure import build_datasets
from pull_gs_data import pull_gs_data

gsheet_warehouse_overview = '1IVj1yahZm_TbnD0RD8slEoNChhQnd7k25pSRqu9wGoA'
gsheet_product = '1qNM_au_sVAbN_U4VNzs1007urb5kv2ojFgQGcFehpPs'

df_warehouse_overview = pull_gs_data().pull_data(spreadsheet_id=gsheet_warehouse_overview)
df_product = pull_gs_data().pull_data(spreadsheet_id=gsheet_product)

dataset_id = 'warehouse'
build_datasets().create_dataset(dataset_id)

table_id_warehouse = 'warehouse_overview'
table_id_product = 'warehouse_product'

build_datasets().upload_dataframe(dataset_id,table_id_warehouse,df_warehouse_overview,truncate=False)
build_datasets().upload_dataframe(dataset_id,table_id_product,df_product,truncate=False)


table_id_product_overview = 'warehouse_product_overview'
sql = """
SELECT 
wo.warehouse_name,
wo.warehouse_id,
wo.country,
wp.inventory_quantity
 FROM `zapp-case-study.warehouse.warehouse_overview` wo
 join `zapp-case-study.warehouse.warehouse_product` wp on wo.warehouse_id = split(wp.sku,'-')[OFFSET(0)]
"""

build_datasets().create_table_from_query(dataset_id,table_id_product_overview ,sql)


prod_stream_sql = """
with product_stream as (
SELECT 
sku,
inventory_quantity,
update_time,
lag(inventory_quantity) OVER (PARTITION BY sku ORDER BY update_time) as prev_quantity,
lag(update_time) OVER (PARTITION BY sku ORDER BY update_time) as prev_update_time
 FROM `zapp-case-study.warehouse.warehouse_product_stream`)
 select 
sku,
inventory_quantity,
update_time,
prev_quantity,
prev_update_time,
case when prev_quantity is null then 0 else inventory_quantity - prev_quantity end as product_delta,
case when prev_quantity is null then 0 else update_time - prev_update_time end as time_delta,
case when inventory_quantity - prev_quantity > 0 then True else False end as restock_flag
from product_stream
"""