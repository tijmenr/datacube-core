import json
import logging
from collections import namedtuple

from sqlalchemy import select, func
from datacube.index import Index, fields
from datacube.model import Dataset
from datacube.drivers.postgres._fields import DateDocField, SimpleDocField
from datacube.drivers.postgres._schema import DATASET

from ._schema import PRODUCT_SUMMARIES

_LOG = logging.getLogger(__name__)


class SummaryAPI:  # pylint: disable=protected-access
    def __init__(self, index: Index):
        self._index = index

    def get_summaries(self, **kwargs):
        return self._index._db._engine.execute(
            select(
                [PRODUCT_SUMMARIES]
            ).where(
                PRODUCT_SUMMARIES.c.query == kwargs
            )
        ).first()


class SearchAPI:  # pylint: disable=protected-access

    def __init__(self, index: Index):
        self._index = index

    def get_product_time_min(self, product: str):
        """
        Returns the minimum acquisition time of the product.
        """

        # Get the offsets of min time in dataset doc
        product = self._index.products.get_by_name(product)
        dataset_section = product.metadata_type.definition['dataset']
        min_offset = dataset_section['search_fields']['time']['min_offset']

        time_field = DateDocField('aquisition_time_min',
                                  'Min of time when dataset was acquired',
                                  DATASET.c.metadata,
                                  False,  # is it indexed ToDo
                                  offset=min_offset,
                                  selection='least')

        result = self._index._db._engine.execute(
            select([func.min(time_field.alchemy_expression)]).where(
                DATASET.c.dataset_type_ref == product.id
            )
        ).first()

        return result[0]

    def get_product_time_max(self, product: str):
        """
        Returns the maximum acquisition time of the product.
        """

        # Get the offsets of min time in dataset doc
        product = self._index.products.get_by_name(product)
        dataset_section = product.metadata_type.definition['dataset']
        max_offset = dataset_section['search_fields']['time']['max_offset']

        time_field = DateDocField('aquisition_time_max',
                                  'Max of time when dataset was acquired',
                                  DATASET.c.metadata,
                                  False,  # is it indexed ToDo
                                  offset=max_offset,
                                  selection='greatest')

        result = self._index._db._engine.execute(
            select([func.max(time_field.alchemy_expression)]).where(
                DATASET.c.dataset_type_ref == product.id
            )
        ).first()

        return result[0]

    # pylint: disable=redefined-outer-name
    def search_returing_datasets_light(self, field_names: tuple, custom_offsets=None, limit=None, **query):
        """
        This is dataset search function that returns the results as objects of a dynamically
        generated Dataset class that is a subclass of tuple.

        Only the requested fields will be returned together with derived attributes as property functions
        similer to the datacube.model.Dataset class.

        The select fields can be custom fields (those not specified in metadata_type, fixed fields, or
        native fields). This require custom offsets of the metadata doc be provided.

        The datasets can be selected based on values of custom fields as well as long as relevant custom
        offsets are provided.

        :param field_names: A tuple of field names that would be returned including derived fields
                            such as extent, crs
        :param custom_offsets: A dictionary of offsets in the metadata doc for custom fields
        :param limit: Number of datasets returned per product.
        :param query: key, value mappings of query that will be processed against metadata_types,
                      product definitions on the client side as well as dataset table.
        :return: A Dynamically generated DatasetLight (a subclass of tuple) objects.
        """

        assert field_names

        for product, query_exprs in self.make_query_expr(query, custom_offsets):

            select_fields, fields_to_process = self.make_select_fields(product, field_names, custom_offsets)

            result_type = namedtuple('DatasetLight', tuple(field.name for field in select_fields))

            class DatasetLight(result_type):

                if fields_to_process.get('grid_spatial'):
                    @property
                    def _gs(self):
                        return self.grid_spatial

                    @property
                    def crs(self):
                        return Dataset.crs.__get__(self)

                    @property
                    def extent(self):
                        return Dataset.extent.__get__(self, Dataset)

            with self._index._db.connect() as connection:
                results = connection.search_datasets(
                    query_exprs,
                    select_fields=select_fields,
                    limit=limit
                )

            for result in results:
                field_values = {field.name: result[i_] for i_, field in enumerate(select_fields)}
                if 'grid_spatial' in fields_to_process:
                    field_values['grid_spatial'] = json.loads(field_values['grid_spatial'])
                yield DatasetLight(**field_values)

    def make_select_fields(self, product, field_names, custom_offsets):
        """
        Parse and generate the list of select fields to be passed to the database API and
        those fields that are to be further processed once the results are returned.
        """

        assert product and field_names

        dataset_fields = product.metadata_type.dataset_fields
        dataset_section = product.metadata_type.definition['dataset']

        select_fields = []
        fields_to_process = dict()
        for field_name in field_names:
            if dataset_fields.get(field_name):
                select_fields.append(dataset_fields[field_name])
            else:
                # try to construct the field
                if field_name in {'grid_spatial', 'extent', 'crs'}:
                    grid_spatial = dataset_section.get('grid_spatial')
                    if grid_spatial:
                        select_fields.append(SimpleDocField(
                            'grid_spatial', 'grid_spatial', DATASET.c.metadata,
                            False,
                            offset=grid_spatial
                        ))
                    if field_name in {'extent', 'crs'}:
                        if not fields_to_process.get('grid_spatial'):
                            fields_to_process['grid_spatial'] = set()
                        fields_to_process['grid_spatial'].add(field_name)
                elif field_name in custom_offsets:
                    select_fields.append(SimpleDocField(
                        field_name, field_name, DATASET.c.metadata,
                        False,
                        offset=custom_offsets[field_name]
                    ))

        return select_fields, fields_to_process

    def make_source_expr(self, source_filter):

        assert source_filter

        product_queries = list(self._index.datasets._get_product_queries(source_filter))
        if not product_queries:
            # No products match our source filter, so there will be no search results regardless.
            raise ValueError('No products match source filter: ' % source_filter)
        if len(product_queries) > 1:
            raise RuntimeError("Multi-product source filters are not supported. Try adding 'product' field")

        source_queries, source_product = product_queries[0]
        dataset_fields = source_product.metadata_type.dataset_fields

        return tuple(fields.to_expressions(dataset_fields.get, **source_queries))

    def make_query_expr(self, query, custom_offsets):
        """
        Generate query expressions including queries based on custom fields
        """

        product_queries = list(self._index.datasets._get_product_queries(query))
        custom_query = dict()
        if not product_queries:
            # The key, values in query that are un-machable with info
            # in metadata types and product definitions, perhaps there are custom
            # fields, will need to handle custom fields separately

            canonical_query = query.copy()
            custom_query = {key: canonical_query.pop(key) for key in custom_offsets
                            if key in canonical_query}
            product_queries = list(self._index.datasets._get_product_queries(canonical_query))

            if not product_queries:
                raise ValueError('No products match search terms: %r' % query)

        for q, product in product_queries:
            print(q, product)
            dataset_fields = product.metadata_type.dataset_fields
            query_exprs = tuple(fields.to_expressions(dataset_fields.get, **q))
            custom_query_exprs = tuple(self.get_custom_query_expressions(custom_query, custom_offsets))

            yield product, query_exprs + custom_query_exprs

    def get_custom_query_expressions(self, custom_query, custom_offsets):
        """
        Generate query expressions for custom fields. it is assumed that custom fields are to be found
        in metadata doc and their offsets are provided
        """

        custom_exprs = []
        for key in custom_query:
            # for now we assume all custom query fields are SimpleDocFields
            custom_field = SimpleDocField(
                custom_query[key], custom_query[key], DATASET.c.metadata,
                False, offset=custom_offsets[key]
            )
            custom_exprs.append(fields.as_expression(custom_field, custom_query[key]))

        return custom_exprs
