from pathlib import Path

from mhd_model.convertors.mhd.convertor import BaseMhdConvertor
from mhd_model.shared.model import Revision

from mtbls2mhd.config import mtbls2mhd_config
from mtbls2mhd.v0_1.legacy.builder import BuildType, MhdLegacyDatasetBuilder


class LegacyProfileV01Convertor(BaseMhdConvertor):
    def __init__(
        self,
        target_mhd_model_schema_uri: str,
        target_mhd_model_profile_uri: str,
    ):
        self.target_mhd_model_schema_uri = target_mhd_model_schema_uri
        self.target_mhd_model_profile_uri = target_mhd_model_profile_uri

    def convert(
        self,
        repository_name: str,
        repository_identifier: str,
        mhd_identifier: None | str,
        mhd_output_folder_path: Path,
        repository_revision: None | Revision = None,
        **kwargs,
    ):
        mhd_dataset_builder = MhdLegacyDatasetBuilder()
        mtbls_study_repository_url = (
            f"{mtbls2mhd_config.study_http_base_url}/{repository_identifier}"
        )
        cached_mtbls_model_files_root_path = Path("/tmp/mtbls2mhd") / Path(
            ".mtbls_model_cache"
        )
        mtbls_study_path = Path(mtbls2mhd_config.mtbls_studies_root_path) / Path(
            repository_identifier
        )
        cached_mtbls_model_files_root_path.mkdir(parents=True, exist_ok=True)
        cache_file_name = mhd_identifier or repository_identifier
        cached_mtbls_model_file_path = cached_mtbls_model_files_root_path / Path(
            cache_file_name
        )
        mhd_dataset_builder.build(
            mhd_id=mhd_identifier,
            mhd_output_path=mhd_output_folder_path,
            mtbls_study_id=repository_identifier,
            mtbls_study_path=mtbls_study_path,
            mtbls_study_repository_url=mtbls_study_repository_url,
            target_mhd_model_schema_uri=self.target_mhd_model_schema_uri,
            target_mhd_model_profile_uri=self.target_mhd_model_profile_uri,
            config=mtbls2mhd_config,
            cached_mtbls_model_file_path=cached_mtbls_model_file_path,
            revision=repository_revision,
            repository_name=repository_name,
            build_type=BuildType.FULL_AND_CUSTOM_NODES,
        )
