import asyncio
from typing import Coroutine

from logger import logger
from meta import Table
from fields import Field, Generated, Sequence, TablesAttitude as TA
from constraints import constraint
from functions import init_pool, pool


@logger
async def init(create_tables=False):
    await init_pool()
    if create_tables:
        await _create_tables()

@logger
async def _create_tables():
    async with pool().acquire() as conn:
        async with conn.transaction():
            processed_tables = []
            for table in Table.subtables():
                created_fields = await conn.fetch(
                    'SELECT column_name AS name, data_type AS sql_type, '
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
                    func = _alter_table(conn, created_fields, created_constraints, 
                                        table, processed_tables)
                else:
                    func = _create_table(conn, table, processed_tables)
                query, precreates, postcreates = await func
                [await p for p in precreates]
                await conn.execute(query) if query else ...
                await asyncio.gather(*postcreates)
                processed_tables.append(table)

Precreates = Postcreates = list[Coroutine]
@logger
async def _alter_table(
        conn, created_fields, created_constraints, table, processed_tables
        ) -> tuple[str, Precreates, Postcreates]:
    fields, constraints, precreates, postcreates = _table_fields(conn, table, processed_tables)
    droping_fields, altering_fields = map(set, ((),)*3)
    for created_field in created_fields:
        for field in fields:
            if field.name == created_field['name']: break
        else:
            droping_fields.add(created_field['name'])
            continue
        created_field = dict(created_field)
        generated = None
        if created_field.pop('is_identity'):
            generated = Generated(
                    type='IDENTITY', generation=created_field['generation'],
                    options=Sequence(
                        start=created_field['start'], increment=created_field['increment'], 
                        max=created_field['max'], min=created_field['min']
                    )
            )
        if created_field.pop('is_generated'):
            generated = Generated(
                    type='STORED', generation=created_field['generation'],
                    options=created_field['expression']
            )
        [created_field.__delitem__(arg) for arg in 'generation start increment max min expression'.split()]
        created_field = Field(**created_field, generated=generated)
        for difference, value in field-created_field:
            match difference:
                case 'sql_type': altering_fields.add(f'SET DATA TYPE {value}')
                case 'not_null': altering_fields.add(('SET' if value else 'DROP')+' NOT NULL')
                case 'default':  altering_fields.add(f'SET DEFAULT {value}' if value else 'DROP DEFAULT')
                case 'generated':
                    if not value: altering_fields.add(f'DROP IDENTITY IF EXISTS')
                    else: ...

    adding_fields = fields
    return '', [], []

@logger
async def _create_table(
        conn, table: Table, processed_tables: list[Table]
        ) -> tuple[str, Precreates, Postcreates]:
    fields, constraints, precreates, postcreates = _table_fields(conn, table, processed_tables)
    fields = {str(field) for field in fields}
    constraints = {f'CONSTRAINT {table.table}_{n} {c}' for n, c in constraints}

    query = ',\n'.join(fields | constraints)
    return f'CREATE TABLE {table.table}(\n'+query+')', precreates, postcreates

@logger
def _table_fields(
        conn, table: Table, processed_tables: list[Table]
        ) -> tuple[set[Field], set[constraint], Precreates, Postcreates]:
    constraints, fields = set(), set()
    precreates: Precreates = []
    postcreates: Postcreates = []
    for field in table.fields.values():
        field, field_constr, field_precr, field_postcr = _field_handle(conn, table, field, processed_tables)
        fields.add(field)
        constraints = constraints.union(field_constr)
        precreates += field_precr
        postcreates += field_postcr
    constraints = constraints.union(c.constraint for c in table.__constraints__)

    return fields, constraints, precreates, postcreates

@logger
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
                f'CREATE TABLE IF NOT EXISTS {table_name}( '
                f'{ref_id} INT REFERENCES {field.type.table}({ref_id}) ON DELETE CASCADE, '
                f'{self_id} INT REFERENCES {table.table}({self_id}) ON DELETE CASCADE, '
                f'CONSTRAINT {table_name}_{ref_id}_{self_id}_un UNIQUE({ref_id}, {self_id}))'
            ))
    return field, constraints, precreates, postcreates
