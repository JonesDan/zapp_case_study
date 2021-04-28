from create_infrastructure import build_datasets
from pull_gs_data import pull_gs_data
import random

def new_inventory(row):

    rand_num = random.randint(1, 10)
    new_inventory = 0 if row['inventory_quantity'] - rand_num <=0 else row['inventory_quantity'] - rand_num
    new_inventory +=10 if row['sku'].split('-')[0] != 'KEN' else 0

    return(new_inventory)

dataset_id = 'warehouse'
build_datasets().create_dataset(dataset_id)

table_id_product_stream = 'warehouse_product_stream'

gsheet_product_stream = '15FN8LC-yLs7kGq0-0NxUn6__FCbpK7c6HP__n5zaRHo'
updatetime = datetime.datetime.now()

for x in range(5):
    df_product_stream = pull_gs_data().pull_data(spreadsheet_id=gsheet_product_stream)
    df_product_stream['inventory_quantity'] = df_product_stream.apply(lambda x:new_inventory,axis=1)

    updatetime += datetime.timedelta(minutes=5)
    df_product_stream['update_time'] = datetime.datetime.strftime(updatetime,'%Y-%m-%dT%H:%M:%S')
    print(df_product_stream)
    pull_gs_data().upload_data(gsheet_product_stream,'Sheet1',df_product_stream)

    build_datasets().upload_dataframe(dataset_id,table_id_product_stream,df_product_stream,truncate=False)