import json
import logging
from pathlib import Path

import jsonschema
from mhd_model.convertors.announcement.v0_1.legacy.mhd2announce import (
    create_announcement_file,
)
from mhd_model.model.v0_1.announcement.validation.validator import (
    MhdAnnouncementFileValidator,
)
from mhd_model.model.v0_1.dataset.validation.validator import validate_mhd_file

from mtbls2mhd.config import Mtbls2MhdConfiguration, get_default_config
from mtbls2mhd.convertor_factory import Mtbls2MhdConvertorFactory
from scripts.utils import setup_basic_logging_config

logger = logging.getLogger(__name__)


def convert_mtbls_study_to_mhd(
    mtbls_study_id: str, mtbls_config: None | Mtbls2MhdConfiguration = None
) -> tuple[bool, dict[str, list[jsonschema.ValidationError]]]:
    all_validation_errors = {}

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
    validation_errors = validate_mhd_file(str(mhd_file_path))
    if validation_errors:
        logger.error("MHD model validation errors found for %s", mtbls_study_id)
        for error in validation_errors:
            logger.error(error)
        all_validation_errors[mhd_output_filename] = validation_errors
    elif not mhd_file_path.exists():
        logger.error("MHD model file not found for %s", mtbls_study_id)
        all_validation_errors[mhd_output_filename] = [
            f"MHD model file '{mhd_output_filename}' not found"
        ]
    else:
        mhd_announcement_output_root_path = Path("tests/mhd_announcement/legacy")
        mhd_announcement_output_root_path.mkdir(exist_ok=True, parents=True)
        logger.info("MHD model validation successful for %s", mtbls_study_id)
        announcement_file_name = f"{mtbls_study_id}.announcement.json"
        announcement_file_path = mhd_announcement_output_root_path / Path(
            announcement_file_name
        )
        mhd_data_json = json.loads(mhd_file_path.read_text())
        mhd_file_url = mtbls_config.study_http_base_url + mtbls_study_id
        create_announcement_file(mhd_data_json, mhd_file_url, announcement_file_path)
        if not announcement_file_path.exists():
            logger.error("MHD announcement file not found for %s", mtbls_study_id)
            all_validation_errors[mhd_output_filename] = [
                f"MHD announcement file '{mhd_output_filename}' not found"
            ]
        else:
            announcement_file_json = json.loads(announcement_file_path.read_text())
            validator = MhdAnnouncementFileValidator()
            all_errors = validator.validate(announcement_file_json)
            if all_errors:
                logger.error(
                    "MHD announcement file validation errors found for %s",
                    mtbls_study_id,
                )
                for error in all_errors:
                    logger.error(error)
                all_validation_errors[mhd_output_filename] = all_errors
            else:
                success = True
                logger.info(
                    "MHD announcement file validation successful for %s",
                    mtbls_study_id,
                )
    return success, all_validation_errors


if __name__ == "__main__":
    setup_basic_logging_config()
    study_ids = ["MTBLS30008993", "MTBLS30008997"]
    mtbls_config = get_default_config()

    for mtbls_study_id in study_ids:
        convert_mtbls_study_to_mhd(mtbls_study_id, mtbls_config)
