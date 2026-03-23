import json
import logging
from pathlib import Path

import click
import yaml
from metabolights_utils.models.metabolights.model import (
    MetabolightsStudyModel,
)
from mhd_model.model.definitions import (
    MHD_MODEL_V0_1_DEFAULT_SCHEMA_NAME,
    MHD_MODEL_V0_1_LEGACY_PROFILE_NAME,
)

from mtbls2mhd.commands.fetch_mtbls_study import fetch_mtbls_data
from mtbls2mhd.config import ConfigurationFile, Mtbls2MhdConfiguration
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
    "--schema-uri",
    default=MHD_MODEL_V0_1_DEFAULT_SCHEMA_NAME,
    show_default=True,
    help="Target MHD model schema. It defines format of MHD model structure.",
)
@click.option(
    "--profile-uri",
    default=MHD_MODEL_V0_1_LEGACY_PROFILE_NAME,
    show_default=True,
    help="Target MHD model profile. It is used to validate MHD model",
)
@click.option(
    "--config-file",
    default="config.yaml",
    show_default=True,
    help="MetaboLights MHD convertor config file.",
)
@click.option(
    "--mtbls-ws-url",
    default="https://www.ebi.ac.uk/metabolights/ws3",
    show_default=True,
    help="MetaboLights Webservice Endpoint URL",
)
@click.option(
    "--mtbls-model-file-path",
    show_default=True,
    help="MetaboLights Metadata (ISA-TAB + DB) json file path",
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
    config_file: str,
    mtbls_ws_url: str,
    mtbls_model_file_path: None | str = None,
):
    """Convert a MetaboLights study to MHD file format.

    Args:

        mtbls_study_id (str): mtbls study accession id. e.g, MTBLS2.

        mhd_identifier (str): MHD accession number.
        Use same value of mtbls_study_id if study profile is legacy. e.g., MTBLS2.


    """
    config: ConfigurationFile = None
    if not config_file:
        click.echo("config file is not defined. Default one will be used.")
        config = ConfigurationFile()
    else:
        try:
            with Path("config.yaml").open() as f:
                data: dict = yaml.safe_load(f)
                config = ConfigurationFile.model_validate(data)
        except Exception as ex:
            click.echo(f"error while parsing config file {ex}")
            click.echo("config file is not defined")
            exit(1)
    if mhd_identifier == mtbls_study_id:
        mhd_identifier = None
    factory = Mtbls2MhdConvertorFactory()
    mtbls2mhd_config = Mtbls2MhdConfiguration(
        database_host=config.db.host,
        database_user=config.db.user,
        database_user_password=config.db.password,
        database_name=config.db.name,
        mtbls_studies_root_path=config.folders.mtbls_studies_root_path,
        selected_schema_uri=schema_uri,
        selected_profile_uri=profile_uri,
        public_http_base_url=str(config.urls.public_http_base_url),
        public_ftp_base_url=str(config.urls.public_ftp_base_url),
        study_http_base_url=str(config.urls.study_http_base_url),
        default_dataset_licence_url=str(config.license.url),
    )
    convertor = factory.get_convertor(
        target_mhd_model_schema_uri=schema_uri,
        target_mhd_model_profile_uri=profile_uri,
    )

    if not mtbls_model_file_path or not Path(mtbls_model_file_path).exists():
        mtbls_model_file_path = fetch_mtbls_data(
            mtbls_study_id,
            output_folder_path=output_dir,
            output_filename=output_filename,
            mtbls_ws_url=mtbls_ws_url,
        )

    model_file_path = Path(mtbls_model_file_path)
    json_data = json.loads(model_file_path.read_text())
    metabolights_study_model = MetabolightsStudyModel.model_validate(json_data)
    mhd_output_root_path = Path(output_dir)
    mhd_output_root_path.mkdir(exist_ok=True, parents=True)
    try:
        success, result = convertor.convert(
            repository_name="MetaboLights",
            repository_identifier=mtbls_study_id,
            mhd_identifier=mhd_identifier,
            mhd_output_folder_path=mhd_output_root_path,
            mhd_output_filename=output_filename,
            config=mtbls2mhd_config,
            metabolights_study_model=metabolights_study_model,
        )
        if success:
            click.echo(f"{mtbls_study_id} is converted successfully. {result}")
        else:
            click.echo(f"{mtbls_study_id} conversion failed. {result}")
    except Exception as ex:
        click.echo(f"{mtbls_study_id} conversion failed. {str(ex)}")


if __name__ == "__main__":
    create_mhd_file_task(
        [
            "MTBLS3107",
            "MTBLS3107",
            "--mtbls-ws-url",
            "https://wwwdev.ebi.ac.uk/metabolights/ws3",
        ]
    )
