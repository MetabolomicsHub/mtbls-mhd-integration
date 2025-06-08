from pydantic_settings import BaseSettings, SettingsConfigDict


class Mtbls2MhdConfiguration(BaseSettings):
    database_name: str
    database_user: str
    database_user_password: str
    database_host: str
    database_host_port: int = 5432

    target_mhd_model_schema_uri: str = "https://www.metabolomicshub.org/schemas/v0.1/common-data-model-v0.1.schema.json"
    target_mhd_model_profile_uri: str = "https://www.metabolomicshub.org/schemas/v0.1/common-data-model-v0.1.ms-profile.json"

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
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


mtbls2mhd_config = Mtbls2MhdConfiguration()
