from collections import defaultdict, namedtuple
from typing import Any, Iterable

from functions import Row, fetch
from meta import Table
from fields import Field, TablesAttitude as TA


class Table(Table):
    @classmethod
    async def select(cls, arg: str = None, *args: str, **kwargs: Any) -> list[Row] | dict[Row, Row]:
        if not arg and not args:
            fields = cls.fields
        else:
            names = args+(arg,) if arg and args else arg.split()
            fields = {name: cls.fields[name.split('.')[0]] for name in names}

        data = args, keys, params, joins = tuple(map(set, ((),)*4))
        for name, field in fields.items():
            cls._field_handle(name, field, *data)
        where = cls._where(kwargs, args, joins)

        params = ', '.join(params)
        joins = '\n'+'\n'.join(joins) if joins else ''
        where = '\nWHERE '+', '.join(where) if where else ''
        query = f'SELECT {params} FROM {cls.table} '+joins+where

        values = []
        for value in kwargs.values():
            if issubclass(type(value), Iterable):
                values += list(value)
            else:
                values.append(value)
        output = await fetch(query, *values) 
        return cls._keys_handle(args, keys, output)
    
    @classmethod
    def _field_handle(cls, name, field, args, keys, params, joins):
        if field.attitude is TA.Simple:
            args.add(name)
            params.add(name)
        else:
            table = field.type
            name = name.split('.')
            if len(name) == 1:
                fields = tuple(n for n, f in table.fields.items() 
                               if f.attitude is TA.Simple)
            else:
                assert table.fields[name[1]].attitude is TA.Simple
                fields = (name[1],)

            args.update(table.table+'_'+f for f in fields)
            params.update('{0}.{1} AS {0}_{1}'.format(table.table, n) 
                          for n in fields)
            if key := cls._key(table, field, fields):
                keys.update(key)
            joins.add(cls._join(table, field))

    @classmethod
    def _key(cls, table: Table, field: Field, fields) -> Iterable[str] | None:
        if field.attitude is TA.OneToMany:
            return (n for n, f in cls.fields.items() if f.attitude is TA.Simple)
        elif field.attitude is TA.ManyToOne:
            return (table.table+'_'+f for f in fields)

    @classmethod
    def _join(cls, table: Table, field: Field) -> str:
        if field.attitude is TA.OneToOne:
            join = 'LEFT JOIN', f'{table.table}_id'
        elif field.attitude is TA.OneToMany:
            join = 'LEFT JOIN', f'{cls.table}_id'
        else:
            join = 'RIGHT JOIN', f'{table.table}_id'
        join_form = '{}'+f' {table.table} '+'USING({})'
        return join_form.format(*join)


    @classmethod
    def _where(cls, kwargs, args, joins) -> set[str]:
        where = set()
        for name, value in kwargs.items():
            field = ''
            for field in cls.fields.keys():
                if field == name: break
                elif name.startswith(field+'_'):
                    field = '.'.join((field, name.replace(field+'_', '')))
                    break
            else: raise ValueError(f"I can't find field {name}")
            number = len(where)+1
            if not issubclass(type(value), Iterable):
                where.add(f'{field} = ${number}')
            else:
                where_ = ', '.join(f'${n}' for n in range(number, len(value)+1))
                where_ = f'({where_})'
                where.add(f'{field} IN {where_}')
            if name in args: continue
            else:
                field = field.split('.')[0]
                field = cls.fields[field]
                joins.add(cls._join(field.type, field))
        return where

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
