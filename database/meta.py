import asyncio, re
from enum import Enum
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Iterable, Protocol

from functions import init_pool, pool


class TablesAttitude(Enum):
    Simple = 'Type'
    OneToOne = 'unique Table and unique Table'
    OneToMany = 'list[Table] and not unique Table'
    ManyToOne = 'not unique Table and list[Table]'
    ManyToMany = 'list[Table] and list[Table]'
TA = TablesAttitude

Table = type
@dataclass(frozen=True)
class Field:
    name: str = ...
    sql_type: str = field(default=..., repr=False)
    
    default: Any = ...
    unique: bool = False
    not_null: bool = False
    
    type: Table = ...
    attitude: TA = TA.Simple

    select: bool = True

class Constraint(Protocol):
    @property
    def constraint(self) -> str | list[str]:
        """Constraint returns row or rows with constraints, 
        which will add to the creation of table"""

class Unique:
    def __init__(self, *fields: Field):
        for field in fields:
            if field.attitude is TA.Simple: ...
            else:
                field.__dict__['attitude'] = TA.OneToOne
        self.fields = fields

    @property
    def constraint(self) -> str | list[str]:
        fields = tuple(field.name for field in self.fields)
        unique_form = '{} UNIQUE({})'
        return unique_form.format(
                '_'.join(fields), ', '.join(f+'_id' for f in fields)
        )


class MetaTable(type):
    def __init__(cls, clsname, superclasses, attributedict): 
        if clsname == 'Table': return
        
        _switch_field_types(cls, clsname)
        cls.table = _clstable(clsname)

        fields = {}
        for key, type_ in cls.__annotations__.items():
            field: Field = attributedict.get(key, Field())
            _field_handle(clsname, key, field, type_)
            setattr(cls, key, field)
            fields[_clstable(key)] = field
        cls.fields = MappingProxyType(fields)

def _switch_field_types(cls, tabname):
    tables = (table for table in Table.subtables() if table.__name__ != tabname)
    for table in tables:
        for field in table.fields.values():
            try: type = field.type.__args__[0]
            except: type = field.type

            if type == tabname: 
                field.__dict__['type'] = cls

def _clstable(clstable: str) -> str:
    if table := re.findall(r'[A-Z][a-z]+', clstable):
        table = map(str.lower, table)
        clstable = '_'.join(table)
    return clstable

_types = {str: 'TEXT', float: 'FLOAT', int: 'INT', bool: 'BOOLEAN'}
def _field_handle(clsname, name, field: Field, type_: type | str):
    is_table = _is_table(type_)
    type_in_iter = _type_iter_table(type_)
    field.__dict__['name'] = name
    field.__dict__['type'] = type_in_iter if type_in_iter else type_

    if is_table or type_in_iter and field.attitude is TA.Simple:
        linked_field = _linked_field(clsname, field)
        attitudes = _attitudes_tab_field(field, linked_field) if is_table \
               else _attitudes_iter_field(linked_field)

        if len(attitudes) > 1:
            field.__dict__['attitude'], linked_field.__dict__['attitude'] = attitudes
        else:
            field.__dict__['attitude'] = attitudes[0]
    else:
        field.__dict__['sql_type'] = _types.get(field.type, None)

def _linked_field(clsname, field: Field) -> Field | None:
    if type(field.type) is str: return 
    
    for linked_field in field.type.fields.values():
        try: type_ = linked_field.type.__args__[0]
        except: type_ = linked_field.type
        if _is_table(type_) and type_.__name__ == clsname:
            return linked_field

def _is_table(type_) -> bool:
    return type(type_) == str or issubclass(type_, Table)

def _type_iter_table(type_):
    is_sequence = type_ is not str and issubclass(type(type_()), Iterable)
    if is_sequence and _is_table(type_.__args__[0]):
        return type_.__args__[0]


def _attitudes_tab_field(field, linked_field) -> tuple[TA] | tuple[TA, TA]:
    if linked_field and linked_field.attitude is TA.OneToOne:
        assert field.unique
        return (TA.OneToOne,)
    elif linked_field and linked_field.attitude is TA.OneToMany:
        assert not field.unique
        return (TA.ManyToOne,)
    else:
        return (TA.OneToOne,) if field.unique else (TA.ManyToOne,)

def _attitudes_iter_field(linked_field) -> tuple[TA] | tuple[TA, TA]:
    if linked_field and linked_field.attitude is TA.ManyToOne:
        return (TA.OneToMany,)
    elif linked_field and linked_field.attitude is TA.OneToMany:
        return (TA.ManyToMany,)*2
    else:
        return (TA.OneToMany,)


class Table(metaclass=MetaTable):
    fields: MappingProxyType[str, Field]
    table: str

    __constraints__: tuple[Constraint, ...] = ()

    @classmethod
    def subtables(cls) -> list['Table']:
        subclasses = []
        for sub in cls.__subclasses__():
            if sub.__name__ == 'Table':
                subclasses += sub.subtables()
            else:
                subclasses.append(sub)
        return subclasses


async def init(create_tables=False):
    await init_pool()
    if create_tables:
        created_tables = set()
        for table in Table.subtables():
            await _create_table(table, created_tables)
            created_tables.add(table)

async def _create_table(table: Table, created_tables: set[Table]):
    fields, postcreates = [], []
    for name, field in table.fields.items():
        constraints = []
        constraints.append('UNIQUE') if field.unique else ...
        constraints.append('NOT NULL') if field.not_null else ...
        if field.attitude is TA.Simple:
            sql_type = field.sql_type if field.sql_type else _types.get(field.type, None)
        elif field.type in created_tables:
            name = field.type.table+'_id'
            sql_type = 'INT'
            if field.attitude in (TA.OneToOne, TA.ManyToOne):
                constraints.append(f'REFERENCES {field.type.table}({name}) ON DELETE CASCADE')
            elif field.attitude is TA.ManyToMany:
                postcreates.append(
                        f'CREATE TABLE {field.type.table}_{table.table}('
                        f'{field.type.table}_id INT REFERNCES {field.type.table}({name}) ON DELETE CASCADE, '
                        f'{table.table}_id INT REFERENCES {table.table}({table.table}_id) ON DELETE CASCADE)'
                )
        else: continue
        constraints = ' '.join(constraints)
        fields.append(f'{name} {sql_type} {constraints}')

    constraints = []
    for constraint in table.__constraints__:
        constraint = constraint.constraint
        if type(constraint) is str:
            constraints.append('CONSTRAINT '+constraint)
        else:
            constraints += ['CONSTRAINT '+c for c in constraint]
    fields = [f'{table.table}_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY']+fields
    query = ', '.join(fields+constraints)

    async with pool().acquire() as conn:
        try: await conn.execute(f'DROP TABLE {table.table} CASCADE')
        except: ...
        await conn.execute(f'CREATE TABLE {table.table}({query})')
        await asyncio.gather(conn.execute(pc) for pc in postcreates)

if __name__ == '__main__':
    asyncio.run(init())
