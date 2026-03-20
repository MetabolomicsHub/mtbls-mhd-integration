import logging
from pathlib import Path

import jsonschema
from metabolights_utils.models.metabolights.model import (
    MetabolightsStudyModel,
)
from metabolights_utils.provider.study_provider import (
    MetabolightsStudyProvider,
)
from mhd_model.model.v0_1.dataset.validation.validator import validate_mhd_model

from mtbls2mhd.config import Mtbls2MhdConfiguration, get_default_config
from mtbls2mhd.convertor_factory import Mtbls2MhdConvertorFactory
from mtbls2mhd.v0_1.legacy.db_metadata_collector import (
    DbMetadataCollector,
    create_postgresql_connection,
)
from mtbls2mhd.v0_1.legacy.folder_metadata_collector import (
    LocalFolderMetadataCollector,
)
from scripts.utils import setup_basic_logging_config

logger = logging.getLogger(__name__)


def convert_mtbls_study_to_mhd(
    mtbls_study_id: str, mtbls_config: None | Mtbls2MhdConfiguration = None
) -> tuple[bool, dict[str, list[jsonschema.ValidationError]]]:
    root_path = mtbls_config.mtbls_studies_root_path
    factory = Mtbls2MhdConvertorFactory()
    convertor = factory.get_convertor(
        target_mhd_model_schema_uri=mtbls_config.selected_schema_uri,
        target_mhd_model_profile_uri=mtbls_config.selected_profile_uri,
    )
    mtbls_study_path = Path(root_path) / Path(mtbls_study_id)

    if not mtbls_study_path.exists():
        logger.warning("%s folder does not exist", mtbls_study_id)
        (False,)
    mhd_output_root_path = Path("tests/mhd_dataset")
    mhd_output_root_path.mkdir(exist_ok=True, parents=True)
    mtbls_study_id.removeprefix("MTBLS").removeprefix("REQ")
    mhd_output_filename = f"{mtbls_study_id}.mhd.json"
    convertor.convert(
        repository_name="MetaboLights",
        repository_identifier=mtbls_study_id,
        mhd_identifier=None,
        mhd_output_folder_path=mhd_output_root_path,
        mhd_output_filename=mhd_output_filename,
        config=mtbls_config,
    )
    mhd_file_path = mhd_output_root_path / Path(mhd_output_filename)
    mhd_announcement_output_root_path = Path("tests/mhd_announcement/legacy")
    announcement_file_name = f"{mtbls_study_id}.announcement.json"
    announcement_file_path = mhd_announcement_output_root_path / Path(
        announcement_file_name
    )
    mhd_file_url = mtbls_config.study_http_base_url + mtbls_study_id
    return validate_mhd_model(
        mtbls_study_id=mtbls_study_id,
        mhd_file_path=mhd_file_path,
        validate_announcement_file=True,
        announcement_file_path=announcement_file_path,
        mhd_file_url=mhd_file_url,
    )


def convert_mtbls_study_model_to_mhd(
    mtbls_study_id: str, mtbls_config: None | Mtbls2MhdConfiguration = None
) -> tuple[bool, dict[str, list[jsonschema.ValidationError]]]:
    root_path = mtbls_config.mtbls_studies_root_path
    factory = Mtbls2MhdConvertorFactory()
    convertor = factory.get_convertor(
        target_mhd_model_schema_uri=mtbls_config.selected_schema_uri,
        target_mhd_model_profile_uri=mtbls_config.selected_profile_uri,
    )
    mtbls_study_path = Path(root_path) / Path(mtbls_study_id)

    if not mtbls_study_path.exists():
        logger.warning("%s folder does not exist", mtbls_study_id)
        (False,)
    mhd_output_root_path = Path("tests/mhd_dataset")
    mhd_output_root_path.mkdir(exist_ok=True, parents=True)
    mtbls_study_id.removeprefix("MTBLS").removeprefix("REQ")
    mhd_output_filename = f"{mtbls_study_id}.mhd.json"
    connection = create_postgresql_connection(mtbls_config)
    db_collector = DbMetadataCollector(mtbls_config)
    provider = MetabolightsStudyProvider(
        db_metadata_collector=db_collector,
        folder_metadata_collector=LocalFolderMetadataCollector(),
    )
    metabolights_study_model: MetabolightsStudyModel = provider.load_study(
        mtbls_study_id,
        study_path=str(mtbls_study_path),
        load_assay_files=True,
        load_sample_file=True,
        load_maf_files=True,
        load_folder_metadata=True,
        connection=connection,
    )

    convertor.convert(
        repository_name="MetaboLights",
        repository_identifier=mtbls_study_id,
        mhd_identifier=None,
        mhd_output_folder_path=mhd_output_root_path,
        mhd_output_filename=mhd_output_filename,
        config=mtbls_config,
        metabolights_study_model=metabolights_study_model,
    )
    mhd_file_path = mhd_output_root_path / Path(mhd_output_filename)
    mhd_announcement_output_root_path = Path("tests/mhd_announcement/legacy")
    announcement_file_name = f"{mtbls_study_id}.announcement.json"
    announcement_file_path = mhd_announcement_output_root_path / Path(
        announcement_file_name
    )
    mhd_file_url = mtbls_config.study_http_base_url + mtbls_study_id
    return validate_mhd_model(
        repository_study_id=mtbls_study_id,
        mhd_file_path=mhd_file_path,
        validate_announcement_file=True,
        announcement_file_path=announcement_file_path,
        mhd_file_url=mhd_file_url,
    )


if __name__ == "__main__":
    setup_basic_logging_config()
    study_ids = ["MTBLS30008993", "MTBLS30008997"]
    mtbls_config = get_default_config()

    for mtbls_study_id in study_ids:
        convert_mtbls_study_model_to_mhd(mtbls_study_id, mtbls_config)
