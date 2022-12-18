from enum import Enum
from dataclasses import dataclass, fields
from typing import Literal


class TablesAttitude(Enum):
    Simple = 'Type'
    OneToOne = 'Table and Table'
    OneToMany = 'list[Table] and Table'
    ManyToOne = 'Table and list[Table]'
    ManyToMany = 'list[Table] and list[Table]'

@dataclass
class Sequence:
    start: int = 1
    increment: int = 1
    max: int | None = None
    min: int | None = None

    def __repr__(self) -> str:
        seq = f'start {self.start} increment {self.increment}'
        seq += f' max {self.max}' if self.max else ''
        seq += f' min {self.min}' if self.min else ''
        return seq

@dataclass
class Generated:
    generation: Literal['ALWAYS'] | Literal['BY DEFAULT'] = 'ALWAYS'
    type: Literal['IDENTITY'] | Literal['STORED'] = 'IDENTITY'
    options: str | Sequence = ''

    def __post_init__(self):
        if self.type == 'IDENTITY':
            self.options = Sequence()
        self.options = '('+str(self.options)+')' if self.options else ''

    def __repr__(self) -> str:
        if self.type == 'IDENTITY':
            return f'GENERATED {self.generation} AS IDENTITY {self.options}'
        else:
            return f'GENERATED {self.generation} AS {self.options} STORED'

Table = type
@dataclass(frozen=True)
class Field:
    name: str = None
    sql_type: str = None
    
    default = None
    not_null: bool = False
    generated: Generated = None
    select: bool = True
    
    type: str | Table = None
    attitude: TablesAttitude = TablesAttitude.Simple

    def __str__(self) -> str:
        field = f'{self.name} {self.sql_type}'
        field += ' '+str(self.generated) if self.generated else ''
        field += f' DEFAULT {self.default}' if self.default else ''
        field += ' NOT NULL' if self.not_null else ''

        return field

    def __sub__(self, other: 'Field'):
        differences = {}
        for key in fields(self):
            key = key.name
            if getattr(self, key) != getattr(other, key):
                differences[key] = getattr(self, key)
        return differences
