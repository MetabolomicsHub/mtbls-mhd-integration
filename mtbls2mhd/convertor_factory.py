from mhd_model.convertors.mhd.convertor import BaseMhdConvertor, BaseMhdConvertorFactory
from mhd_model.model.definitions import (
    MHD_MODEL_V0_1_DEFAULT_SCHEMA_NAME,
    MHD_MODEL_V0_1_LEGACY_PROFILE_NAME,
    MHD_MODEL_V0_1_MS_PROFILE_NAME,
)

from mtbls2mhd.v0_1.legacy.convertor import LegacyProfileV01Convertor


class Mtbls2MhdConvertorFactory(BaseMhdConvertorFactory):
    def get_convertor(
        self,
        target_mhd_model_schema_uri: str,
        target_mhd_model_profile_uri: str,
    ) -> BaseMhdConvertor:
        if target_mhd_model_schema_uri == MHD_MODEL_V0_1_DEFAULT_SCHEMA_NAME:
            if target_mhd_model_profile_uri == MHD_MODEL_V0_1_LEGACY_PROFILE_NAME:
                return LegacyProfileV01Convertor(
                    target_mhd_model_schema_uri=target_mhd_model_schema_uri,
                    target_mhd_model_profile_uri=target_mhd_model_profile_uri,
                )
            elif target_mhd_model_profile_uri == MHD_MODEL_V0_1_MS_PROFILE_NAME:
                raise NotImplementedError()
            raise NotImplementedError()
        else:
            raise NotImplementedError()
