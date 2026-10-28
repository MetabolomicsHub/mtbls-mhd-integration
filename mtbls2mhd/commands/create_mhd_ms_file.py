import json
import logging
import sys
from pathlib import Path

import click
import yaml
from metabolights_utils.models.metabolights.model import (
    MetabolightsStudyModel,
)
from mhd_model.model.definitions import (
    MHD_MODEL_V1_0_DEFAULT_SCHEMA_NAME,
    MHD_MODEL_V1_0_MS_PROFILE_NAME,
)
from mhd_model.validation import select_cv_term_helper

from mtbls2mhd.commands.fetch_mtbls_study import fetch_mtbls_data
from mtbls2mhd.config import ConfigurationFile, Mtbls2MhdConfiguration
from mtbls2mhd.convertor_factory import Mtbls2MhdConvertorFactory

logger = logging.getLogger(__name__)


@click.command(name="to-mhd-ms", no_args_is_help=True)
@click.option(
    "--mtbls-study-id",
    required=True,
    help="MetaboLights accession to convert, for example MTBLS2.",
)
@click.option(
    "--mhd-id",
    required=True,
    help=(
        "Identifier assigned to the resulting MHD file. For a legacy study, "
        "use the MetaboLights accession."
    ),
)
@click.option(
    "--output-dir",
    default="outputs",
    required=True,
    help="Required directory where the converted MHD file is written.",
)
@click.option(
    "--output-filename",
    default=None,
    show_default=True,
    help=(
        "Name for the converted MHD JSON file (for example, "
        "MHD000001_mhd.json). When omitted, the converter chooses the name."
    ),
)
@click.option(
    "--config-file",
    default="config.yaml",
    show_default=True,
    help="Path to the MetaboLights to MHD configuration YAML file.",
)
@click.option(
    "--mtbls-ws-url",
    default="https://www.ebi.ac.uk/metabolights/ws3",
    show_default=True,
    help="Base URL of the MetaboLights web service used to fetch study metadata.",
)
@click.option(
    "--mtbls-model-file-path",
    show_default=True,
    help=(
        "Path to an existing MetaboLights metadata JSON file. If omitted or "
        "the path does not exist, the study data is fetched from the web service."
    ),
)
@click.option(
    "--ontology-lookup-db-path",
    default=None,
    show_default=True,
    help="Path to the ontology lookup SQLite database (defaults to the configured database).",
)
def create_mhd_ms_file_task(
    mtbls_study_id: str,
    mhd_id: str,
    output_dir: str,
    output_filename: str,
    config_file: str,
    mtbls_ws_url: str,
    mtbls_model_file_path: None | str = None,
    ontology_lookup_db_path: None | str = None,
):
    """Convert a MetaboLights study to an MHD file using the MS profile.

    Study metadata is fetched from the MetaboLights web service unless an
    existing metadata JSON file is supplied with --mtbls-model-file-path. The
    resulting file uses --mhd-id as its MHD identifier and is written to
    --output-dir. For a legacy study, use its MetaboLights accession as
    --mhd-id.
    """
    profile_uri = MHD_MODEL_V1_0_MS_PROFILE_NAME
    schema_uri = MHD_MODEL_V1_0_DEFAULT_SCHEMA_NAME

    ontology_cache_service = None

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
            sys.exit(1)

    ontology_lookup_db_path = ontology_lookup_db_path or config.ontology.local_db_path
    cv_term_helper = select_cv_term_helper(db_path=ontology_lookup_db_path)

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
        mtbls_auth_server_url=config.auth.server_url,
        mtbls_auth_realm_name=config.auth.realm_name,
        mtbls_auth_client_id=config.auth.client_id,
        mtbls_auth_client_secret=config.auth.client_secret,
        ontology_lookup_db_path=ontology_lookup_db_path,
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
            mhd_identifier=mhd_id,
            mhd_output_folder_path=mhd_output_root_path,
            mhd_output_filename=output_filename,
            config=mtbls2mhd_config,
            metabolights_study_model=metabolights_study_model,
            ontology_cache_service=ontology_cache_service,
            cv_term_helper=cv_term_helper,
        )
        if success:
            click.echo(f"{mtbls_study_id} is converted successfully. {result}")
        else:
            click.echo(f"{mtbls_study_id} conversion failed. {result}")
    except Exception as ex:
        click.echo(f"{mtbls_study_id} conversion failed. {str(ex)}")
