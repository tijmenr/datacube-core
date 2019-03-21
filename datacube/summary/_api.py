from sqlalchemy import select
from datacube.index import Index

from ._schema import PRODUCT_SUMMARIES


class SummaryAPI:
    def __init__(self, index: Index):
        self._engine = index._db._engine

    def get_summaries(self, **kwargs):
        return self._engine.execute(
            select(
                [PRODUCT_SUMMARIES]
            ).where(
                PRODUCT_SUMMARIES.c.query == kwargs
            )
        ).first()
