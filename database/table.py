import asyncio, re

from meta import init, MetaTable, Field


Table = type

class MetaTable(MetaTable):
    def __init__(cls, clsname, superclasses, attributedict):
        if clsname == 'Table': return
        _switch_field_types(cls, clsname)
        super().__init__(clsname, superclasses, attributedict)

def _switch_field_types(cls, tabname):
    tables = (table for table in Table.__subclasses__() if table.__name__ != tabname)
    for table in tables:
        for field in table.fields.values():
            try: type = field.type.__args__[0]
            except: type = field.type

            if type == tabname: 
                field.__dict__['type'] = cls

class Table(metaclass=MetaTable):
    fields: dict[str, Field]
    table: str

class Country(Table):
    name: str
    inventory: list['Inventory']

class Inventory(Table):
    count: int
    country: Country

async def main():
    await init()

if __name__ == '__main__':
    asyncio.run(main())
