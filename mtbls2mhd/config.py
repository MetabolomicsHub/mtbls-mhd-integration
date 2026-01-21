from pydantic import AnyUrl, BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

mhd_model_v0_1_schema_uri: str = "https://metabolomicshub.github.io/mhd-model/schemas/v0_1/common-data-model-v0.1.schema.json"
mhd_model_v0_1_ms_profile_uri: str = "https://metabolomicshub.github.io/mhd-model/schemas/v0_1/common-data-model-v0.1.ms-profile.json"
mhd_model_v0_1_legacy_profile_uri: str = "https://metabolomicshub.github.io/mhd-model/schemas/v0_1/common-data-model-v0.1.legacy-profile.json"


class Mtbls2MhdConfiguration(BaseSettings):
    database_name: str
    database_user: str
    database_user_password: str
    database_host: str
    database_host_port: int = 5432
    selected_schema_uri: str
    selected_profile_uri: str
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
    mtbls_studies_root_path: str
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


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
    db: DatabaseConfiguration
    urls: UrlConfiguration
    license: LicenseConfiguration
    folders: FoldersConfiguration


def get_default_config() -> Mtbls2MhdConfiguration:
    mtbls2mhd_config = Mtbls2MhdConfiguration()
    return mtbls2mhd_config
