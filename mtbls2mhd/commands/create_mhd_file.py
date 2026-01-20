import logging
from pathlib import Path

import click
from mhd_model.model.definitions import (
    MHD_MODEL_V0_1_DEFAULT_SCHEMA_NAME,
    MHD_MODEL_V0_1_LEGACY_PROFILE_NAME,
)

from mtbls2mhd.convertor_factory import Mtbls2MhdConvertorFactory

logger = logging.getLogger(__name__)


@click.command(name="mhd", no_args_is_help=True)
@click.option(
    "--output-dir",
    default="outputs",
    show_default=True,
    help="Output directory for MHD file",
)
@click.option(
    "--output-filename",
    default=None,
    show_default=True,
    help="MHD filename (e.g., MHD000001_mhd.json, ST000001_mhd.json)",
)
@click.option(
    "--schema_uri",
    default=MHD_MODEL_V0_1_DEFAULT_SCHEMA_NAME,
    show_default=True,
    help="Target MHD model schema. It defines format of MHD model structure.",
)
@click.option(
    "--profile_uri",
    default=MHD_MODEL_V0_1_LEGACY_PROFILE_NAME,
    show_default=True,
    help="Target MHD model profile. It is used to validate MHD model",
)
@click.argument("mtbls_study_id")
@click.argument("mhd_identifier")
def create_mhd_file_task(
    mtbls_study_id: str,
    mhd_identifier: str,
    output_dir: str,
    output_filename: str,
    schema_uri: str,
    profile_uri: str,
):
    """Convert a MetaboLights study to MHD file format.

    Args:

        mtbls_study_id (str): mtbls study accession id. e.g, MTBLS2.

        mhd_identifier (str): MHD accession number.
        Use same value of mtbls_study_id if study profile is legacy. e.g., MTBLS2.

    """

    if mhd_identifier == mtbls_study_id:
        mhd_identifier = None
    factory = Mtbls2MhdConvertorFactory()
    convertor = factory.get_convertor(
        target_mhd_model_schema_uri=schema_uri,
        target_mhd_model_profile_uri=profile_uri,
    )
    mhd_output_root_path = Path(output_dir)
    mhd_output_root_path.mkdir(exist_ok=True, parents=True)
    try:
        success, result = convertor.convert(
            repository_name="MetaboLights",
            repository_identifier=mtbls_study_id,
            mhd_identifier=mhd_identifier,
            mhd_output_folder_path=mhd_output_root_path,
            mhd_output_filename=output_filename,
        )
        if success:
            click.echo(f"{mtbls_study_id} is converted successfully. {result}")
        else:
            click.echo(f"{mtbls_study_id} conversion failed. {result}")
    except Exception as ex:
        click.echo(f"{mtbls_study_id} conversion failed. {str(ex)}")


if __name__ == "__main__":
    create_mhd_file_task(["MTBLS3107", "MTBLS3107"])
