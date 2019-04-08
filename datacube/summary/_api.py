from sqlalchemy import select, func
from datacube.index import Index
from datacube.drivers.postgres._fields import DateDocField
from datacube.drivers.postgres._schema import DATASET

from ._schema import PRODUCT_SUMMARIES


class SummaryAPI:  # pylint: disable=protected-access
    def __init__(self, index: Index):
        self._index = index
        # self._engine = index._db._engine  # pylint: disable=protected-access

    def get_summaries(self, **kwargs):
        return self._index._db._engine.execute(
            select(
                [PRODUCT_SUMMARIES]
            ).where(
                PRODUCT_SUMMARIES.c.query == kwargs
            )
        ).first()

    def get_product_time_min(self, product: str):

        # Get the offsets of min time in dataset doc
        metadata_type = self._index.products.get_by_name(product).metadata_type
        dataset_section = metadata_type.definition['dataset']
        min_offset = dataset_section['search_fields']['time']['min_offset']

        time_field = DateDocField('aquisition_time_min',
                                  'Min of time when dataset was acquired',
                                  DATASET.c.metadata,
                                  False,
                                  offset=min_offset,
                                  selection='least')

        result = self._index._db._engine.execute(
            select([func.min(time_field.alchemy_expression)])
        ).first()

        return result[0]

    def get_product_time_max(self, product: str):

        # Get the offsets of min time in dataset doc
        metadata_type = self._index.products.get_by_name(product).metadata_type
        dataset_section = metadata_type.definition['dataset']
        max_offset = dataset_section['search_fields']['time']['max_offset']

        time_field = DateDocField('aquisition_time_max',
                                  'Max of time when dataset was acquired',
                                  DATASET.c.metadata,
                                  False,
                                  offset=max_offset,
                                  selection='greatest')

        result = self._index._db._engine.execute(
            select([func.max(time_field.alchemy_expression)])
        ).first()

        return result[0]
