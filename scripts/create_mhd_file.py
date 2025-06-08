import logging
from pathlib import Path
from typing import OrderedDict

from mtbls2mhd.config import mtbls2mhd_config
from mtbls2mhd.v0_1.convertor import Mtbs2MhdConvertor
from mtbls2mhd.v0_1.db_metadata_collector import create_postgresql_connection

logger = logging.getLogger(__name__)


if __name__ == "__main__":
    study_ids = []
    if not study_ids:
        connection = None
        try:
            connection = create_postgresql_connection()
            cursor = connection.cursor()
            query = "SELECT acc FROM studies WHERE status = 3;"
            cursor.execute(query)
            results = cursor.fetchall()
            for item in results:
                study_ids.append(item[0])
            study_ids.sort(key=lambda x: int(x.replace("MTBLS", "")), reverse=True)
        except Exception as ex:
            logger.exception(ex, stack_info=True)
            exit(1)
        finally:
            if connection:
                connection.close()
    study_ids = [x for x in study_ids if int(x.replace("MTBLS", "")) <= 35]
    # METADATA FILES path
    root_path = "<APPLICATION FOLDER>/data/studies/metadata-files"
    count = 0
    for mtbls_study_id in study_ids:
        protocol_summaries = OrderedDict()
        mtbls_study_path = Path(root_path) / Path(mtbls_study_id)
        if not mtbls_study_path.exists():
            print(f"{mtbls_study_id} folder does not exist")
            continue
        count += 1
        Path("output/mhd_data/mtbls").mkdir(exist_ok=True, parents=True)
        file_path = Path(f"output/mtbls_data/{mtbls_study_id}.json")
        mhd_id = f"MHDM{int(mtbls_study_id.replace('MTBLS', '').replace('REQ', '')):06}"
        mtbls_study_repository_url = (
            f"{mtbls2mhd_config.study_http_base_url}/{mtbls_study_id}"
        )
        # cached_mtbls_model_files_root_path = Path("/tmp/mtbls2mhd") / Path(
        #     ".mtbls_model_cache"
        # )
        # cached_mtbls_model_files_root_path.mkdir(parents=True, exist_ok=True)
        # cached_mtbls_model_file_path = cached_mtbls_model_files_root_path / Path(mhd_id)
        cached_mtbls_model_file_path = None
        mhd_output_root_path = Path("output/mhd_data/mtbls")
        mhd_output_root_path.mkdir(parents=True, exist_ok=True)
        mhd_output_path = mhd_output_root_path / Path(f"{mhd_id}.mhd.json")
        mtbls_study_convertor = Mtbs2MhdConvertor()

        mtbls_study_convertor.convert(
            mhd_id=mhd_id,
            mhd_output_path=mhd_output_path,
            mtbls_study_id=mtbls_study_id,
            mtbls_study_path=mtbls_study_path,
            mtbls_study_repository_url=mtbls_study_repository_url,
            target_mhd_model_schema_uri=mtbls2mhd_config.target_mhd_model_schema_uri,
            target_mhd_model_profile_uri=mtbls2mhd_config.target_mhd_model_profile_uri,
            config=mtbls2mhd_config,
            cached_mtbls_model_file_path=cached_mtbls_model_file_path,
        )
