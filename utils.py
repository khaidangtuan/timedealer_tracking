import requests
import sqlalchemy as sa
import psycopg2
import pandas as pd
from config import queue_config_table, conn_str, queue_detail_table, notify_endpoint
import threading

def checkTableExist(tablename, engine):
    with engine.connect() as conn:
        sql = sa.text('''
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
            ''') 
        results = conn.execute(sql)
    tables = [i[0] for i in results.fetchall()]
    if tablename in tables:
        return True
    else:
        return False

def check_userId_refId(userId_refId, engine):
    with engine.connect() as conn:
        sql = sa.text(f'''
                      SELECT count(1) > 0 
                      FROM "{queue_config_table}" 
                      WHERE "userId_refId" = '{userId_refId}';
                      ''')
        result = conn.execute(sql)
        
    return result.fetchall()[0][0]
    
def condition_convert(config):
    
    condition_list = ['new','used']
    
    if isinstance(config["conditions"], list):
        if len(config["conditions"]) == 0:
            config["conditions"] = "none"
        elif len(config["conditions"]) == 1:
            config["conditions"] = condition_list[config["conditions"][0]]
        else:
            config["conditions"] = "both"
    else:
        return TypeError("Wrong format of Conditions")
    
def currency_convert(amount, curr_base, curr_target):
    endpoint = f'https://open.er-api.com/v6/latest/{curr_base}'
    r = requests.get(endpoint)
    rate = r.json()['rates'][curr_target]
    return {'currency_code':curr_target,
            'amount':amount*rate}

def process_config(config):
    config['max_price'] = currency_convert(config['max_price'], config['currency_code'], 'HKD')["amount"]
    config['curency_code'] = 'HKD'
    condition_convert(config)
    
def condition_filter(extractedRecord, queueConfig):
    # model
    model_check = extractedRecord["Model"].strip().lower() <= queueConfig["model"].strip().lower()
    
    # price
    price_check =  extractedRecord["Price"] <= queueConfig["max_price"]
    
    # conditions
    if queueConfig["conditions"] == "new":
        conditions_check = extractedRecord["Product Status"].strip().lower() == "new"
    elif queueConfig["conditions"] == "used":
        conditions_check = (extractedRecord["Product Status"].strip().lower() != "new") and \
            (extractedRecord["Product Status"].strip().lower() != "")
    else:
        conditions_check = True
    
    # manufacturing year
    if queueConfig["manufactured_year"] == '':
        manufacturingYear_check = True
    else:
        manufacturingYear_check = extractedRecord["ManufactureYear"] == queueConfig["manufactured_year"]

    return model_check and price_check and conditions_check and manufacturingYear_check

def push_data(data):
     r = requests.post(notify_endpoint, json=data)
     print({data["userId_refId"]:r.status_code})
 
def write_data(data, engine):
    df = pd.DataFrame(data["data"])
    df["userId_refId"] = data["userId_refId"]
    with engine.connect() as conn:
        df.to_sql(queue_detail_table, conn, schema='public', if_exists='append', index=None)
    print({data["userId_refId"]:"Successfully written to db"})

def push_write_data(all_queue_data):
    results = []
    for k, v in all_queue_data.items():
        results.append({"userId_refId":k,"data":v})
    
    push_threads = [threading.Thread(target=push_data,args=(x, )) for x in results]
    engine = sa.create_engine(conn_str)
    write_threads = [threading.Thread(target=write_data,args=(x, engine)) for x in results]
    for thread in push_threads: thread.start()
    for thread in write_threads: thread.start()
    for thread in push_threads: thread.join()
    for thread in write_threads: thread.join()
 
 
        
    
        
        
    
    
        
    
    
        
        
    