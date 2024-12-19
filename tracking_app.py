from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
import pandas as pd
import sqlalchemy as sa
import psycopg2

from utils import process_config, checkTableExist, check_userId_refId, condition_filter, push_write_data
from config import conn_str, queue_config_table, notify_endpoint, queue_detail_table

class queueConfig(BaseModel):
    userId_refId: str
    model: str
    max_price: float
    currency_code: str
    conditions: List[int]
    manufactured_year: str

app = FastAPI()

@app.post('/createQueue/')
def createQueue(config: queueConfig):
    config = dict(config)
    process_config(config)
    data = pd.DataFrame([config])
    
    engine = sa.create_engine(conn_str)
    if checkTableExist(queue_config_table, engine):
        if check_userId_refId(config["userId_refId"], engine):
            raise HTTPException(status_code=409, detail='userId_refId DOES exist!')
    
    with engine.connect() as conn:
        data.to_sql(queue_config_table, conn, schema='public', if_exists='append', index=None)
    
    return {"detail": "Successfully created"}

class queueConfigDelete(BaseModel):
    userId_refId: str
    
@app.post('/deleteQueue/')
def deleteQueue(queueId: queueConfigDelete):
    engine = sa.create_engine(conn_str)
    if checkTableExist(queue_config_table, engine):
        if check_userId_refId(queueId.userId_refId, engine):
            sql = sa.text(f'''
                          DELETE FROM {queue_config_table} 
                          WHERE "userId_refId" = '{queueId.userId_refId}';
                          ''')
            with engine.connect() as conn:
                conn.execute(sql)
                conn.commit()
        else:
            raise HTTPException(status_code=409, detail='userId_refId DOES NOT exist!')
        
    return {"detail": "Successfully deleted"}

@app.post('/track/')
def track(messages: List[dict]):
    # read all config
    engine = sa.create_engine(conn_str)
    with engine.connect() as conn:
        config = pd.read_sql(f'SELECT * FROM {queue_config_table};', conn)
    
    results = {}
    keys = ['parent_message_id','Brand','Model','Product Status','Price','Time','want_to_buy','Color','Material','Strap','Dial','ManufactureYear']
    for conf in config.to_dict(orient='records'):
        r = []
        for mes in messages:
            if condition_filter(mes, conf):
                r.append({key: mes[key] for key in keys})
        
        if len(r) > 0:
            results[conf['userId_refId']] = r
    
    push_write_data(results) 
    

                
        
        
        
    
    