import asyncio, re
from collections import defaultdict, namedtuple
from typing import Iterable

from functions import Row, fetch
from meta import init, Table, Field, TablesAttitude as TA


class Table(Table):
    @classmethod
    async def select(cls) -> list[Row] | dict[Row, Row]:
        data = args, keys, params, joins = tuple(map(set, ((),)*4))
        for name, field in cls.fields.items():
            cls._field_handle(name, field, *data)

        params = ', '.join(params)
        joins = '\n'.join(joins)
        query = f'SELECT {params} FROM {cls.table} '+joins

        output = await fetch(query) 
        return cls._keys_handle(args, keys, output)
    
    @classmethod
    def _field_handle(cls, name, field, args, keys, params, joins):
        if field.attitude is TA.Simple:
            args.add(name)
            params.add(name)
        else:
            table = field.type
            fields = tuple(n for n, f in table.fields.items()
                           if f.attitude is TA.Simple)

            args.update(table.table+'_'+f for f in fields)
            params.update('{0}.{1} AS {0}_{1}'.format(table.table, n) 
                          for n in fields)
            if key := cls._key(table, field, fields):
                keys.update(key)
            join_form = '{} '+f' {table.table} '+'USING({})'
            joins.add(join_form.format(*cls._join(table, field)))

    @classmethod
    def _key(cls, table: Table, field: Field, fields) -> Iterable[str] | None:
        if field.attitude is TA.OneToMany:
            return (n for n, f in cls.fields.items() if f.attitude is TA.Simple)
        elif field.attitude is TA.ManyToOne:
            return (table.table+'_'+f for f in fields)

    @classmethod
    def _join(cls, table: Table, field: Field) -> tuple[str, str]:
        if field.attitude is TA.OneToOne:
            return 'LEFT JOIN', f'{table.table}_id'
        elif field.attitude is TA.OneToMany:
            return 'LEFT JOIN', f'{cls.table}_id'
        else:
            return 'RIGHT JOIN', f'{table.table}_id'


    @classmethod
    def _keys_handle(cls, args, keys, output):
        if keys := args & keys:
            output_ = defaultdict(list)
            KeyRecord = namedtuple('Record', keys)
            RowRecord = namedtuple('Record', args-keys)
            for row in output:
                row = dict(row)
                key = KeyRecord(*(row.pop(key) for key in keys))
                row = RowRecord(*row.values())
                output_[key].append(row)
            output = dict(output_)
        return output


class Country(Table):
    name: str

class Item(Table):
    name: str

class Inventory(Table):
    count: int
    item: Item = Field(unique=True)
    country: Country = Field(unique=True)

async def main():
    await init()
    print(await Inventory.select())

if __name__ == '__main__':
    asyncio.run(main())
