import logging

import click

from mtbls2mhd.commands.create_mhd_legacy_file import create_mhd_legacy_file_task
from mtbls2mhd.commands.create_mhd_ms_file import create_mhd_ms_file_task

logger = logging.getLogger(__name__)


@click.group(name="convert", no_args_is_help=True)
def conversion_group():
    """Convert MetaboLights study to MHD File."""


conversion_group.add_command(create_mhd_ms_file_task)
conversion_group.add_command(create_mhd_legacy_file_task)
