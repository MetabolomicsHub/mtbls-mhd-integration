import enum

from dotenv import dotenv_values
from pydantic import AnyUrl, BaseModel

MHD_MODEL_V0_1_SCHEMA_URI: str = "https://metabolomicshub.github.io/mhd-model/schemas/v0_1/common-data-model-v0.1.schema.json"
MHD_MODEL_V0_1_MS_PROFILE_URI: str = "https://metabolomicshub.github.io/mhd-model/schemas/v0_1/common-data-model-v0.1.ms-profile.json"
MHD_MODEL_V0_1_LEGACY_PROFILE_URI: str = "https://metabolomicshub.github.io/mhd-model/schemas/v0_1/common-data-model-v0.1.legacy-profile.json"


class BuildType(enum.Enum):
    MINIMUM = "minimal_mhd_model"
    FULL = "full"
    FULL_AND_CUSTOM_NODES = "full_and_custom_nodes"


class Mtbls2MhdConfiguration(BaseModel):
    database_name: None | str = None
    database_user: None | str = None
    database_user_password: None | str = None
    database_host: None | str = None
    database_host_port: int = 5432
    selected_schema_uri: None | str = None
    selected_profile_uri: None | str = None
    public_ftp_base_url: str = (
        "ftp://ftp.ebi.ac.uk/pub/databases/metabolights/studies/public"
    )
    public_http_base_url: str = (
        "http://ftp.ebi.ac.uk/pub/databases/metabolights/studies/public"
    )
    study_http_base_url: str = "https://www.ebi.ac.uk/metabolights"
    default_dataset_licence_url: str = (
        "https://creativecommons.org/publicdomain/zero/1.0"
    )
    default_mhd_model_version: str = "0.1"
    mtbls_studies_root_path: None | str = None

    use_label_for_invalid_cv_term: bool = False
    build_type: BuildType = BuildType.FULL_AND_CUSTOM_NODES


class DatabaseConfiguration(BaseModel):
    host: str
    port: int
    name: str
    user: str
    password: str


class UrlConfiguration(BaseModel):
    public_ftp_base_url: AnyUrl = AnyUrl(
        "ftp://ftp.ebi.ac.uk/pub/databases/metabolights/studies/public"
    )
    public_http_base_url: AnyUrl = AnyUrl(
        "http://ftp.ebi.ac.uk/pub/databases/metabolights/studies/public"
    )
    study_http_base_url: AnyUrl = AnyUrl("https://www.ebi.ac.uk/metabolights")


class LicenseConfiguration(BaseModel):
    name: None | str = "CC0 1.0 Universal"
    url: AnyUrl = AnyUrl("https://creativecommons.org/publicdomain/zero/1.0/")


class FoldersConfiguration(BaseModel):
    mtbls_studies_root_path: str


class ConfigurationFile(BaseModel):
    db: None | DatabaseConfiguration = None
    urls: UrlConfiguration = UrlConfiguration()
    license: LicenseConfiguration = LicenseConfiguration()
    folders: None | FoldersConfiguration = None


def get_default_config() -> Mtbls2MhdConfiguration:
    env_vars = dotenv_values(".env")
    config_kwargs = {k.lower(): v for k, v in env_vars.items()} if env_vars else {}
    mtbls2mhd_config = Mtbls2MhdConfiguration(**config_kwargs)
    return mtbls2mhd_config
