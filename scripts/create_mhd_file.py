import logging
from pathlib import Path

from mtbls2mhd.config import get_default_config
from mtbls2mhd.convertor_factory import Mtbls2MhdConvertorFactory
from scripts.utils import setup_basic_logging_config

# from mtbls2mhd.v0_1.db_metadata_collector import create_postgresql_connection

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    setup_basic_logging_config()
    study_ids = ["MTBLS30008987"]

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
        file_path = Path(f"tests/mtbls_dataset/{mtbls_study_id}.json")
        mtbls_study_id.removeprefix("MTBLS").removeprefix("REQ")
        mhd_numeric_id = mtbls_study_id.removeprefix("MTBLS").removeprefix("REQ")
        convertor.convert(
            repository_name="MetaboLights",
            repository_identifier=mtbls_study_id,
            mhd_identifier=None,
            mhd_output_folder_path=mhd_output_root_path,
            mhd_output_filename=None,
            config=config,
        )
