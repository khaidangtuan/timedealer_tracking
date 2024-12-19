from fastapi import FastAPI, HTTPException
from datetime import datetime
import json

app = FastAPI()

@app.post('/dataReceiver/')
def receive_data(data: dict):
    filename = data['userId_refId'] + '_' + datetime.now().strftime('%Y%m%d%H%M%S%f') + '.json'
    with open(filename,'w') as file:
        json.dump(data, file)
        
    return {'details':'Successfully received'}