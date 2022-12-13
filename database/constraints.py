from typing import Protocol, TypeAlias, runtime_checkable

from fields import Field


name: TypeAlias = str
constraint: TypeAlias = tuple[name, str]
@runtime_checkable
class Constraint(Protocol):
    @property
    def constraint(self) -> constraint | list[constraint]:
        """Constraint returns row or rows with constraints, 
        which will add to the creation of table"""

class Unique(list):
    def __init__(self, *fields: Field):
        super().__init__(fields)

    @property
    def constraint(self) -> constraint | list[constraint]:
        fields = [field.name for field in self]
        return '_'.join(fields), 'UNIQUE('+', '.join(fields)+')'

    def __repr__(self) -> str:
        fields = ', '.join(field.name for field in self)
        return f'Unique({fields})'
