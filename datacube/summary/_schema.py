from sqlalchemy import Table, Column, Integer, DateTime
from sqlalchemy.dialects import postgresql as postgres
from sqlalchemy.sql import func

from datacube.drivers.postgres import _core, sql

PRODUCT_SUMMARIES = Table(
    'product_summaries', _core.METADATA,
    Column('id', Integer, primary_key=True, autoincrement=True),

    Column('query', postgres.JSONB, unique=True, nullable=False),

    Column('spatial_bounds', postgres.JSONB, nullable=False),

    Column('spatial_footprint', postgres.JSONB, nullable=False),

    Column('dates', postgres.JSONB, nullable=False),

    # When it was added and by whom.
    Column('added', DateTime(timezone=True), server_default=func.now(), nullable=False),
    Column('added_by', sql.PGNAME, server_default=func.current_user(), nullable=False)
)
