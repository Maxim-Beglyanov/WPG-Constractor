import asyncio
from typing import Coroutine

from meta import Table
from fields import Field, Generated, TablesAttitude as TA
from constraints import constraint
from functions import init_pool, pool


async def init(create_tables=False):
    await init_pool()
    if create_tables:
        await _create_tables()

async def _create_tables():
    async with pool().acquire() as conn:
        async with conn.transaction():
            processed_tables = []
            for table in Table.subtables():
                created_fields = await conn.fetch(
                    'SELECT column_name AS name, data_type AS type, '
                    'column_default AS default, is_nullable AS is_null, '
                    'is_identity, identity_generation AS generation, '
                    'identity_start AS start, identity_increment AS increment, '
                    'identity_maximum AS max, identity_minimum AS min, '
                    'is_generated, generation_expression AS expression '
                    'FROM information_schema.columns '
                    'WHERE table_name = $1', table.table
                )
                if created_fields:
                    created_constraints = await conn.fetch(
                            'SELECT constraint_name AS name FROM information_schema.constraint_table_usage '
                            'WHERE table_name = $1', table.table
                    )
                    await _alter_table(conn, created_fields, created_constraints, 
                                       table, processed_tables)
                else:
                    await _create_table(conn, table, processed_tables)
                processed_tables.append(table)

async def _alter_table(conn, created_fields, created_constraints, table, processed_tables): ...

Precreates = Postcreates = list[Coroutine]
async def _create_table(conn, table: Table, processed_tables: list[Table]):
    constraints, fields = set(), set()
    precreates: Precreates = []
    postcreates: Postcreates = []
    for field in table.fields.values():
        field, field_constr, field_precr, field_postcr = _field_handle(conn, table, field, processed_tables)
        fields.add(str(field))
        constraints = constraints.union(field_constr)
        precreates += field_precr
        postcreates += field_postcr
    constraints = constraints.union(c.constraint for c in table.__constraints__)
    constraints = {f'CONSTRAINT {table.table}_{n} {c}' for n, c in constraints}

    query = ',\n'.join(fields | constraints)
    [await p for p in precreates]
    await conn.execute(f'CREATE TABLE {table.table}('+query+')')
    await asyncio.gather(*postcreates)

def _field_handle(
        conn, table: Table, field: Field, processed_tables: list[Table]
) -> tuple[Field, list[constraint], Precreates, Postcreates]:
    name = field.name
    constraints, precreates, postcreates = [], [], []
    if field.attitude is not TA.Simple:
        ref_is_created = field.type in processed_tables
        if field.attitude in (TA.OneToOne, TA.ManyToOne) and ref_is_created:
            constraints.append(
                    (field.name+'_fkey', f'FOREIGN KEY({field.name}) REFERENCES {field.type.table}({field.name}) ON DELETE CASCADE')
            )
            precreates.append(conn.execute(
                f'ALTER TABLE {field.type.table} '
                f'ADD COLUMN IF NOT EXISTS {field.type.table}_id '
                'INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY'
            ))
        else:
            field = Field(table.table+'_id', 'INT', generated=Generated())
            constraints.append((name+'_pkey', f'PRIMARY KEY({name})'))

        if field.attitude is TA.ManyToMany:
            table_name = f'{field.type.table}_{table.table}'
            ref_id = f'{field.type.table}_id'
            self_id = f'{table.table}_id'
            postcreates.append(conn.execute(
                f'CREATE TABLE {table_name}( '
                f'{ref_id} INT REFERENCES {field.type.table}({ref_id}) ON DELETE CASCADE, '
                f'{self_id} INT REFERENCES {table.table}({self_id}) ON DELETE CASCADE, '
                f'CONSTRAINT {table_name}_{ref_id}_{self_id}_un UNIQUE({ref_id}, {self_id}))'
            ))
    return field, constraints, precreates, postcreates
