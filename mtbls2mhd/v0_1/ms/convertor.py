from pathlib import Path

from mhd_model.convertors.mhd.convertor import BaseMhdConvertor
from mhd_model.shared.model import Revision

from mtbls2mhd.config import Mtbls2MhdConfiguration, get_default_config
from mtbls2mhd.v0_1.legacy.builder import BuildType, MhdLegacyDatasetBuilder


class MsProfileConvertor(BaseMhdConvertor):
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
        repository_revision: None | Revision = None,
        config: None | Mtbls2MhdConfiguration = None,  # noqa: F821
        cached_mtbls_model_file_path: None | str = None,
        **kwargs,
    ):
        if not config:
            config = get_default_config()
        mhd_dataset_builder = MhdLegacyDatasetBuilder()
        mtbls_study_repository_url = (
            f"{config.study_http_base_url}/{repository_identifier}"
        )

        mtbls_study_path = Path(config.mtbls_studies_root_path) / Path(
            repository_identifier
        )
        try:
            success, message = mhd_dataset_builder.build(
                mhd_id=mhd_identifier,
                mtbls_study_id=repository_identifier,
                mtbls_study_path=mtbls_study_path,
                mtbls_study_repository_url=mtbls_study_repository_url,
                target_mhd_model_schema_uri=self.target_mhd_model_schema_uri,
                target_mhd_model_profile_uri=self.target_mhd_model_profile_uri,
                config=config,
                cached_mtbls_model_file_path=cached_mtbls_model_file_path,
                revision=repository_revision,
                repository_name=repository_name,
                build_type=BuildType.FULL_AND_CUSTOM_NODES,
                **kwargs,
            )
            return success, message
        except Exception as ex:
            import traceback

            traceback.print_exc()
            return False, str(ex)
