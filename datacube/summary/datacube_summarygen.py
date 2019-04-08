"""
Generate extent (spatial and time) summaries for products in order to support queries
such as
    - What is the footprint of product 'ls8_level1_scene' for the month of 'November-2018'?
    - What is the footprint of product 'ls8_level1_scene'?
    - What is the footprint of product 'product_A' for region_code 'codeA' for month 'monthA'?

"""

import logging
import click

import ogr
from datacube import Datacube


LOG = logging.getLogger(__name__)


@click.group(help=__doc__)
@click.option('--config', '-c', help="Pass the configuration file to access the database",
              type=click.Path(exists=True))
@click.pass_context
def cli(ctx, config):
    """ Used to pass the datacube index to functions via click."""
    ctx.obj = Datacube(config=config).index


def compute_extent(index, crs, **kwargs):

    time_footprint = []
    geom = ogr.Geometry(ogr.wkbMultiPolygon)

    datasets = index.datasets.search(**kwargs)
    for dataset in datasets:
        time_footprint.append(dataset.center_time)
        if dataset.extent:
            geom.AddGeometry(dataset.extent.to_crs(crs).__geo_interface__)
        else:
            LOG.info('Extent undefined for the dataset: %s', dataset.id)

    if not datasets:
        LOG.info('No datasets returned for query: %s', kwargs)

    return geom.UnionCascaded(), time_footprint


def compute_extent_periodic(index, product, period_type):
    pass
