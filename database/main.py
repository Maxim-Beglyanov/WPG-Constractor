import asyncio

from init import init
from table import Table
import logger


class Country(Table):
    name: str

class Item(Table):
    name: str
    price: int

class Inventory(Table):
    country: Country
    item: Item

if __name__ == '__main__':
    asyncio.run(init(create_tables=True))
