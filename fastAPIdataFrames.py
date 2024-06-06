# -*- coding: utf-8 -*-
"""
Created on Thu Jun  6 16:14:27 2024

@author: bqxes
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd

class Item(BaseModel):
    id: str
    name: str
    price: str
    


itemDB = pd.DataFrame(columns = ["id", "name", "price"])

def newItem(item: Item):
    global itemDB
    if itemDB.loc[((itemDB["id"] == item.id) | (itemDB["name"] == item.name))].empty:
        itemDB = pd.concat([itemDB, pd.DataFrame.from_records([{'id': item.id, 'name': item.name, 'price':  item.price}])])
        return True
    else:
        return False
def query(item_id):
    global itemDB
    return itemDB.loc[itemDB["id"] == item_id]

def update(item):
    global itemDB
    target = itemDB.loc[((itemDB["id"] == item.id) | (itemDB["name"] == item.name))]
    if target.empty:
        index = itemDB.loc[((itemDB["id"] == item.id) | (itemDB["name"] == item.name))].index
        itemDB.loc[index, "id"] = item.id
        itemDB.loc[index, "name"] = item.name
        itemDB.loc[index, "price"] = item.price
        return False
    else:
        
        
        return True

app = FastAPI()




@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/items/{item_id}")
async def read_item(item_id: str, q: str | None = None):
    item = query(item_id)
    if item.empty:
        raise HTTPException(status_code=404, detail="Item not found")
    return {"id": item["id"], "name": item["name"], "price": item["price"]}
    
@app.post("/items/")
async def create_item(item: Item):
    global itemDB
    success = newItem(item)
    if not success:
        raise HTTPException(status_code=422, detail="Item already exists. Use patch to update the item")
    print(itemDB)
    return item
    
@app.patch("/items/")
async def update_item(item: Item):
    success = update(item)
    if not success:
        raise HTTPException(status_code=404, detail="Item not found")
    return item
