'''It will only work if you run this inside the GCP platform,
otherwise an API key is required to access the crypto project
that Cesar is using.

Meant to be run in the main folder
'''

from crypto_utils import BinanceOrderBook, OrderBookComposite
from google.cloud import bigquery
import pandas as pd
import yaml
import datetime

with open('config.yaml', 'r') as f:
    CONFIG = yaml.safe_load(f)

def update_update_times(client,new_timestamp):
    table_id = 'crypto-335808.raw_data.update_times'
    query = "SELECT * FROM {}".format(table_id)
    query_job = client.query(query)
    update_times_df = query_job.to_dataframe()

    row = 1
    update_times_df.loc[row,'timestamp'] = new_timestamp

    #now we can just upload this value to the table
    job_config = bigquery.LoadJobConfig(write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE)
    job = client.load_table_from_dataframe(update_times_df,table_id,job_config=job_config)
    job.result()

def run(event,context):
    symbols = CONFIG['api_binance']['symbols']
    order_books = OrderBookComposite()
    for symbol in symbols:
        order_books.add_order_book(BinanceOrderBook(symbol))

    data = order_books.get_order_book_snapshot()
    symbol_map = {i:symbols[i-1] for i in range(1,len(symbols)+1)}
    symbol_map_reverse = {symbols[i-1]:i for i in range(1,len(symbols)+1)}

    # the next few linex put the dataframes in just one table to upload
    # to big query

    new_timestamp = pd.to_datetime(datetime.datetime.utcnow())
    dfs = []
    for coin in data:
        df = pd.DataFrame(data[coin]).rename_axis('levels').reset_index().round(3)
        df['levels'] = (df['levels']*100).round(1)
        df['levels'] = df['levels'].astype('str').str.replace('.','_')
        df['label_bids'] = 'bids_' + df['levels']
        df['label_asks'] = 'asks_' + df['levels']

        bids = pd.Series(index=df.label_bids,data=df['bids'].values)
        asks = pd.Series(index=df.label_asks,data=df['asks'].values)
        f_df = pd.concat([bids,asks]).to_frame().T
        f_df.insert(0,'timestamp',new_timestamp)
        f_df.insert(1,'coin',symbol_map_reverse[coin])
        f_df.insert(2,'price',float(data[coin]['price']))
        dfs.append(f_df)
    final_df = pd.concat(dfs)

    #the next few lines perform the loading job to the big query table.

    
    client = bigquery.Client()

    update_update_times(client,new_timestamp)

    table_id = 'crypto-335808.raw_data.order_book_copy'
    job_config = bigquery.LoadJobConfig(autodetect=True,
                                    create_disposition=bigquery.job.CreateDisposition.CREATE_IF_NEEDED,
                                    write_disposition=bigquery.job.WriteDisposition.WRITE_APPEND)

    job = client.load_table_from_dataframe(final_df,table_id,job_config)
    job.result()
