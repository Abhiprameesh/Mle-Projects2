from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()

class Item (BaseModel) : 
    text: str = None
    is_done: bool = False 

items = []

@app.get("/")
def read_root():
    return {"message": "Hello, My name is Abhinand"}

# Create an item (POST)
@app.post("/items/")
def create_item(item: str):
    items.append(item)
    return {"message": "Item added successfully", "items": items}

@app.get("/items/")
def list_items(limit: int = 10):
    return items[0 :limit]

# Read a specific item (GET)
@app.get("/items/{item_id}")
def get_item(item_id: int):
    if item_id < len(items):
        return {"item": items[item_id]}
    else:
        raise HTTPException(status_code=404, detail="Item not found")