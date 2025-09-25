import logging

import click
from mhd_model.commands.validate.announcement import validate_announcement_file_task
from mhd_model.commands.validate.mhd_file import validate_mhd_file_task

logger = logging.getLogger(__name__)


@click.group(name="validate", no_args_is_help=True)
def validation_cli():
    """Validate MHD model or annoucenment file."""
    pass


validation_cli.add_command(validate_mhd_file_task)
validation_cli.add_command(validate_announcement_file_task)
