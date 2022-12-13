import re
from types import MappingProxyType
from typing import Iterable

from fields import Field, TablesAttitude as TA
from constraints import Constraint, Unique


class MetaTable(type):
    def __init__(cls, clsname, superclasses, attributedict): 
        if clsname == 'Table': return
        
        _switch_field_types(cls, clsname)
        cls.table = _clstable(clsname)

        fields = {}
        for key, type_ in cls.__annotations__.items():
            field: Field = attributedict.get(key, Field())
            _field_handle(cls, clsname, key, field, type_)
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
def _field_handle(table, clsname, name, field: Field, type_: type | str):
    is_table = _is_table(type_)
    type_in_iter = _type_iter_table(type_)
    sql_type = field.sql_type
    field.__dict__['type'] = type_in_iter if type_in_iter else type_

    if is_table or type_in_iter and field.attitude is TA.Simple:
        name += '_id'
        sql_type = 'INT'
        linked_field = _linked_field(clsname, field)
        attitudes = _attitudes_tab_field(table, field, linked_field) if is_table \
               else _attitudes_iter_field(linked_field)

        if len(attitudes) > 1:
            field.__dict__['attitude'], linked_field.__dict__['attitude'] = attitudes
        else:
            field.__dict__['attitude'] = attitudes[0]
    elif not field.sql_type:
        sql_type = _types.get(field.type)
    field.__dict__['name'] = name
    field.__dict__['sql_type'] = sql_type

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


def _attitudes_tab_field(table, field, linked_field) -> tuple[TA] | tuple[TA, TA]:
    if linked_field and linked_field.attitude is TA.OneToOne:
        return (TA.OneToOne,)
    elif linked_field and linked_field.attitude is TA.OneToMany:
        return (TA.ManyToOne,)
    else:
        for constraint in table.__constraints__:
            if type(constraint) is Unique and (field,) == tuple(constraint):
                return (TA.OneToOne,)
        return (TA.ManyToOne,)

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

    __constraints__: Constraint | Iterable[Constraint] = ()

    @classmethod
    def subtables(cls) -> list['Table']:
        subclasses = []
        for sub in cls.__subclasses__():
            if sub.__name__ == 'Table':
                subclasses += sub.subtables()
            else:
                subclasses.append(sub)
        return subclasses
