"""
Generate extent (spatial and time) summaries for products in order to support queries
such as
    - What is the footprint of product 'ls8_level1_scene' for the month of 'November-2018'?
    - What is the footprint of product 'ls8_level1_scene'?
    - What is the footprint of product 'product_A' for region_code 'codeA' for month 'monthA'?

"""

import logging
import click

from datacube import Datacube
from datacube.utils.geometry import unary_union


LOG = logging.getLogger(__name__)


@click.group(help=__doc__)
@click.option('--config', '-c', help="Pass the configuration file to access the database",
              type=click.Path(exists=True))
@click.pass_context
def cli(ctx, config):
    """ Used to pass the datacube index to functions via click."""
    ctx.obj = Datacube(config=config).index


def compute_extent(index, **kwargs):

    time_footprint = []

    def consume_datasets(datasets):
        for dataset in datasets:
            time_footprint.append(dataset.center_time)
            if dataset.extent:
                yield dataset.extent

    datasets = index.datasets.search(**kwargs)

    if not datasets:
        return None

    spatial_footprint = unary_union(consume_datasets(datasets))

    return spatial_footprint, time_footprint or None



