import asyncio, re
from enum import Enum
from dataclasses import asdict, dataclass
from types import MappingProxyType
from typing import Any, Callable, Coroutine, Iterable

from functions import init_pool


class TablesAttitude(Enum):
    Simple = 'Type'
    OneToOne = 'unique Table and unique Table'
    OneToMany = 'list[Table] and not unique Table'
    ManyToOne = 'not unique Table and list[Table]'
    ManyToMany = 'list[Table] and list[Table]'
TA = TablesAttitude

@dataclass(frozen=True)
class Field:
    sql_type: str = ...
    type: 'Table' = ...
    default: Any = ...
    unique: bool = False
    not_null: bool = False

    attitude: TA = TA.Simple
    init: Callable[[], Coroutine] = ...
    insert: Callable[[], Coroutine] = ...

class MetaTable(type):
    def __init__(cls, clsname, superclasses, attributedict):
        cls.table = _clstable(clsname)

        fields = {}
        for key, type_ in cls.__annotations__.items():
            field: Field = attributedict.get(key, Field())
            field = _field_handle(clsname, field, type_)
            setattr(cls, key, field)
            fields[_clstable(key)] = field
        cls.fields = MappingProxyType(fields)

def _clstable(clstable: str) -> str:
    if table := re.findall(r'[A-Z][a-z]+', clstable):
        table = map(str.lower, table)
        clstable = '_'.join(table)
    return clstable


def _field_handle(clsname, field: Field, type_: type | str) -> Field:
    field.__dict__['type'] = type_
    data_field = asdict(field)

    is_table = _is_table(field.type)
    type_in_iter = _type_iter_table(field.type)
    if is_table or type_in_iter:
        field.__dict__['type'] = type_in_iter if type_in_iter else type_
        linked_field = _linked_field(clsname, field)
        attitudes = _attitudes_tab_field(field, linked_field) if is_table \
               else _attitudes_seq_field(linked_field)

        if len(attitudes) > 1:
            data_field['attitude'], linked_field.__dict__['attitude'] = attitudes
        else:
            data_field['attitude'] = attitudes
    
    return Field(**data_field)

def _linked_field(clsname, field: Field) -> Field | None:
    if type(field.type) is str: return 
    
    for linked_field in field.type.fields.values():
        try: type_ = linked_field.type.__args__[0]
        except: type_ = linked_field.type
        if type_.__name__ == clsname:
            return linked_field

def _is_table(type_) -> bool:
    return type(type_) == str or 'Table' in (sup.__name__ for sup in type_.__bases__)

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

def _attitudes_seq_field(linked_field) -> tuple[TA] | tuple[TA, TA]:
    if linked_field and linked_field.attitude is TA.ManyToOne:
        return (TA.OneToMany,)
    elif linked_field and linked_field.attitude is TA.OneToMany:
        return (TA.ManyToMany,)*2
    else:
        return (TA.OneToMany,)


async def init():
    await init_pool()

if __name__ == '__main__':
    asyncio.run(init())
