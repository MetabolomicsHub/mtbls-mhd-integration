import logging
from pathlib import Path

from mhd_model.model.v0_1.dataset.validation.validator import validate_mhd_file

from mtbls2mhd.config import get_default_config
from mtbls2mhd.convertor_factory import Mtbls2MhdConvertorFactory
from scripts.utils import setup_basic_logging_config

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    setup_basic_logging_config()
    study_ids = ["MTBLS30008993"]

    # METADATA FILES path
    config = get_default_config()
    root_path = config.mtbls_studies_root_path
    count = 0
    factory = Mtbls2MhdConvertorFactory()
    convertor = factory.get_convertor(
        target_mhd_model_schema_uri=config.selected_schema_uri,
        target_mhd_model_profile_uri=config.selected_profile_uri,
    )
    for mtbls_study_id in study_ids:
        mtbls_study_path = Path(root_path) / Path(mtbls_study_id)

        if not mtbls_study_path.exists():
            logger.warning("%s folder does not exist", mtbls_study_id)
            continue
        count += 1
        mhd_output_root_path = Path("tests/mhd_dataset")
        mhd_output_root_path.mkdir(exist_ok=True, parents=True)
        mtbls_study_id.removeprefix("MTBLS").removeprefix("REQ")
        mhd_numeric_id = mtbls_study_id.removeprefix("MTBLS").removeprefix("REQ")
        mhd_output_filename = f"{mtbls_study_id}.mhd.json"
        convertor.convert(
            repository_name="MetaboLights",
            repository_identifier=mtbls_study_id,
            mhd_identifier=None,
            mhd_output_folder_path=mhd_output_root_path,
            mhd_output_filename=mhd_output_filename,
            config=config,
        )
        file_path = mhd_output_root_path / Path(mhd_output_filename)

        validation_errors = validate_mhd_file(str(file_path))

        if validation_errors:
            logger.info("MHD model validation errors found for %s", mtbls_study_id)
        for error in validation_errors:
            logger.error(error)
