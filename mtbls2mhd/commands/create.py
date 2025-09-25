import logging

import click
from mhd_model.commands.create.announcement import create_announcement_file_task

from mtbls2mhd.commands.create_mhd_file import create_mhd_file_task

logger = logging.getLogger(__name__)


@click.group(name="create", no_args_is_help=True)
def creation_cli():
    """Create MHD model or annoucenment file."""
    pass


creation_cli.add_command(create_mhd_file_task)
creation_cli.add_command(create_announcement_file_task)
