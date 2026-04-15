import json
import logging
import traceback
from pathlib import Path

import jsonschema
from metabolights_utils.models.metabolights.model import (
    MetabolightsStudyModel,
)
from metabolights_utils.provider.study_provider import (
    MetabolightsStudyProvider,
)
from mhd_model.convertors.mhd.convertor import BaseMhdConvertor
from mhd_model.convertors.sdrf.mhd2sdrf import create_sdrf_files
from mhd_model.model.v0_1.dataset.validation.validator import validate_mhd_model
from psycopg import Connection
from psycopg.rows import TupleRow

from mtbls2mhd.commands.fetch_mtbls_study import fetch_mtbls_data
from mtbls2mhd.config import (
    MHD_MODEL_V0_1_LEGACY_PROFILE_URI,
    # MHD_MODEL_V0_1_MS_PROFILE_URI,
    MHD_MODEL_V0_1_SCHEMA_URI,
    Mtbls2MhdConfiguration,
    get_default_config,
)
from mtbls2mhd.convertor_factory import Mtbls2MhdConvertorFactory
from mtbls2mhd.v0_1.legacy.db_metadata_collector import (
    DbMetadataCollector,
    create_postgresql_connection,
)
from mtbls2mhd.v0_1.legacy.folder_metadata_collector import LocalFolderMetadataCollector
from scripts.utils import setup_basic_logging_config

logger = logging.getLogger(__name__)


def convert_mtbls_study_to_mhd(
    mtbls_study_id: str,
    mtbls_config: None | Mtbls2MhdConfiguration = None,
    mhd_output_root_path: None | Path = None,
    mhd_announcement_output_root_path: None | Path = None,
    mtbls_model_source_path: None | Path = None,
    convertor: None | BaseMhdConvertor = None,
    errors_file_path: None | Path = None,
    force_recreate: bool = False,
) -> tuple[bool | None, dict[str, list[jsonschema.ValidationError]]]:
    root_path = mtbls_config.mtbls_studies_root_path

    if not mhd_output_root_path:
        mhd_output_root_path = Path("tests/mhd_dataset")
    mtbls_study_id.removeprefix("MTBLS").removeprefix("REQ")
    mhd_output_filename = f"{mtbls_study_id}.mhd.json"
    mhd_file_path = mhd_output_root_path / Path(mhd_output_filename)
    if not mhd_announcement_output_root_path:
        mhd_announcement_output_root_path = Path("tests/mhd_announcement/legacy")
    announcement_file_name = f"{mtbls_study_id}.announcement.json"
    announcement_file_path = mhd_announcement_output_root_path / Path(
        announcement_file_name
    )

    if (
        not force_recreate
        and announcement_file_path.exists()
        and mhd_file_path.exists()
        and errors_file_path
        and not errors_file_path.exists()
    ):
        return None, []
    if mhd_file_path.exists():
        mhd_file_path.unlink()
    if announcement_file_path.exists():
        announcement_file_path.unlink()
    if errors_file_path and errors_file_path.exists():
        errors_file_path.unlink()
    if not convertor:
        factory = Mtbls2MhdConvertorFactory()
        convertor = factory.get_convertor(
            target_mhd_model_schema_uri=mtbls_config.selected_schema_uri,
            target_mhd_model_profile_uri=mtbls_config.selected_profile_uri,
        )
    mtbls_study_path = Path(root_path) / Path(mtbls_study_id)

    if not mtbls_study_path.exists():
        logger.warning("%s folder does not exist", mtbls_study_id)
        (False,)
    mhd_output_root_path.mkdir(exist_ok=True, parents=True)
    metabolights_study_model = None
    if mtbls_model_source_path and mtbls_model_source_path.exists():
        metabolights_study_model = MetabolightsStudyModel.model_validate_json(
            mtbls_model_source_path.read_text(), by_alias=True
        )
    try:
        convertor.convert(
            repository_name="MetaboLights",
            repository_identifier=mtbls_study_id,
            mhd_identifier=None,
            mhd_output_folder_path=mhd_output_root_path,
            mhd_output_filename=mhd_output_filename,
            config=mtbls_config,
            metabolights_study_model=metabolights_study_model,
        )

    except Exception:
        if mhd_file_path.exists():
            mhd_file_path.unlink()
        if announcement_file_path.exists():
            announcement_file_path.unlink()
        return False, {
            mhd_file_path.name: [
                ("error", jsonschema.ValidationError(message=traceback.format_exc()))
            ]
        }

    try:
        mhd_file_url = (
            mtbls_config.public_http_base_url
            + "/"
            + mtbls_study_id
            + "/"
            + mhd_output_filename
        )
        return validate_mhd_model(
            repository_study_id=mtbls_study_id,
            mhd_file_path=mhd_file_path,
            validate_announcement_file=True,
            announcement_file_path=announcement_file_path,
            mhd_file_url=mhd_file_url,
        )
    except Exception:
        if mhd_file_path.exists():
            mhd_file_path.unlink()
        if announcement_file_path.exists():
            announcement_file_path.unlink()
        return False, {
            "input": [
                ("error", jsonschema.ValidationError(message=traceback.format_exc()))
            ]
        }


def convert_mtbls_study_model_to_mhd(
    mtbls_study_id: str,
    mtbls_config: Mtbls2MhdConfiguration,
    mtbls_ws_url: str = "https://www.ebi.ac.uk/metabolights/ws3",
) -> tuple[bool, dict[str, list[jsonschema.ValidationError]]]:
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

    output_dir_path = Path("tests/mtbls_model")
    mtbls_model_file_path = output_dir_path / f"{mtbls_study_id}_model.json"
    file_path = fetch_mtbls_data(
        mtbls_study_id,
        output_folder_path=output_dir_path,
        output_filename=mtbls_model_file_path.name,
        mtbls_ws_url=mtbls_ws_url,
    )

    metabolights_study_model = MetabolightsStudyModel.model_validate_json(
        file_path.read_text()
    )
    convertor.convert(
        repository_name="MetaboLights",
        repository_identifier=mtbls_study_id,
        mhd_identifier=None,
        mhd_output_folder_path=mhd_output_root_path,
        mhd_output_filename=mhd_output_filename,
        config=mtbls_config,
        metabolights_study_model=metabolights_study_model,
    )
    mhd_file_path = mhd_output_root_path / Path(mhd_output_filename)
    mhd_announcement_output_root_path = Path("tests/mhd_announcement/legacy")
    announcement_file_name = f"{mtbls_study_id}.announcement.json"
    announcement_file_path = mhd_announcement_output_root_path / Path(
        announcement_file_name
    )
    mhd_file_url = mtbls_config.study_http_base_url + mtbls_study_id
    return validate_mhd_model(
        repository_study_id=mtbls_study_id,
        mhd_file_path=mhd_file_path,
        validate_announcement_file=True,
        announcement_file_path=announcement_file_path,
        mhd_file_url=mhd_file_url,
    )


def write_to_file(errors_file_path, success, errors):
    if success and errors_file_path.exists():
        errors_file_path.unlink()
    if not success or errors:
        errors_dict = {}
        for file, val in errors.items():
            for key, error in val:
                if file not in errors_dict:
                    errors_dict[file] = {}
                if key not in errors_dict[file]:
                    errors_dict[file][key] = []
                errors_dict[file][key].append(error.message)

        errors_file_path.write_text(json.dumps({"errors": errors_dict}, indent=2))


def create_mtbls_model(
    mtbls_study_id: str,
    provider: MetabolightsStudyProvider,
    connection: Connection[TupleRow],
    ms_assay_types: set[str],
    study_root_path: Path,
    mtbls_model_root_path: Path,
):
    mtbls_study_path = Path(study_root_path) / Path(mtbls_study_id)
    mtbls_model_target_path = mtbls_model_root_path / Path(
        f"{mtbls_study_id}_model.json"
    )
    if not mtbls_model_target_path.exists():
        data: MetabolightsStudyModel = provider.load_study(
            mtbls_study_id,
            study_path=str(mtbls_study_path),
            load_assay_files=True,
            load_sample_file=True,
            load_maf_files=True,
            load_folder_metadata=True,
            connection=connection,
        )
    else:
        data_json = json.loads(mtbls_model_target_path.read_text())
        version = data_json.get("studyDbMetadata", {}).get("mhdModelVersion", "")
        dataset_license_url = data_json.get("studyDbMetadata", {}).get(
            "datasetLicenseUrl", ""
        )
        if version is None:
            data_json.get("studyDbMetadata", {})["mhdModelVersion"] = ""
        if dataset_license_url is None:
            data_json.get("studyDbMetadata", {})["datasetLicenseUrl"] = (
                "https://www.ebi.ac.uk/about/terms-of-use/"
            )
        if version is None or dataset_license_url is None:
            mtbls_model_target_path.write_text(json.dumps(data_json, indent=2))

        data = MetabolightsStudyModel.model_validate_json(
            mtbls_model_target_path.read_text(), by_alias=True
        )

    excluded_assays = None
    assay_type_labels: set[str] = set()
    if data.investigation.studies:
        for assay in data.investigation.studies[0].study_assays.assays:
            assay_file = data.assays.get(assay.file_name)
            if not assay_file:
                assay_type_labels.add(None)
            assay_type_labels.add(assay_file.assay_technique.name)
        excluded_assays = assay_type_labels - ms_assay_types
    if not assay_type_labels or excluded_assays:
        logger.warning(
            "%s - Excluded. It has '%s' assays types",
            mtbls_study_id,
            ", ".join(excluded_assays),
        )
        mtbls_model_target_path.unlink(missing_ok=True)
    else:
        if not mtbls_model_target_path.exists():
            logger.info(
                "%s - assays types: '%s'",
                mtbls_study_id,
                ", ".join(assay_type_labels),
            )
            mtbls_model_target_path.write_text(
                data.model_dump_json(indent=2, by_alias=True)
            )


def create_mtbls_models(skip_current: bool = True, working_dir: str = ".outputs"):
    mtbls_config = get_default_config()
    root_path = mtbls_config.mtbls_studies_root_path

    connection = create_postgresql_connection(mtbls_config)

    sql = "select acc from studies where status = 3;"
    try:
        cursor = connection.cursor()
        cursor.execute(sql)
        data = cursor.fetchall()
    except Exception as ex:
        raise ex
    study_ids = [x.get("acc") for x in data]
    study_ids.sort(
        key=lambda x: int(x.replace("MTBLS", "").replace("REQ", "")), reverse=True
    )

    db_collector = DbMetadataCollector(mtbls_config)
    provider = MetabolightsStudyProvider(
        db_metadata_collector=db_collector,
        folder_metadata_collector=LocalFolderMetadataCollector(),
    )
    ms_assay_types = {
        "LC-MS",
        "GC-MS",
        "GCxGC-MS",
        "DI-MS",
        "FIA-MS",
        "CE-MS",
        "MALDI-MS",
        "MS",
    }
    mtbls_model_root_path = Path(working_dir) / "mtbls_model"

    mtbls_model_root_path.mkdir(parents=True, exist_ok=True)
    for mtbls_study_id in study_ids:
        mtbls_model_target_path = mtbls_model_root_path / Path(
            f"{mtbls_study_id}_model.json"
        )
        if mtbls_model_target_path.exists():
            continue
        create_mtbls_model(
            mtbls_study_id=mtbls_study_id,
            provider=provider,
            connection=connection,
            ms_assay_types=ms_assay_types,
            study_root_path=root_path,
            mtbls_model_root_path=mtbls_model_root_path,
        )


def create_mhd_legacy_profile(
    force_recreate: bool = False, working_dir: str = ".outputs"
):
    study_ids = [
        x.name.replace("_model.json", "")
        for x in Path(f"{working_dir}/mtbls_model").glob("*_model.json")
    ]
    study_ids.sort(
        key=lambda x: int(x.replace("MTBLS", "").replace("REQ", "")), reverse=True
    )
    # study_ids = ["REQ20250108125518"]
    factory = Mtbls2MhdConvertorFactory()
    mhd_output_root_path = Path(f"{working_dir}/mhd_legacy")
    mtbls_config = get_default_config()

    mtbls_config.selected_schema_uri = MHD_MODEL_V0_1_SCHEMA_URI
    mtbls_config.selected_profile_uri = MHD_MODEL_V0_1_LEGACY_PROFILE_URI
    mtbls_config.use_label_for_invalid_cv_term = True
    legacy_convertor = factory.get_convertor(
        target_mhd_model_schema_uri=mtbls_config.selected_schema_uri,
        target_mhd_model_profile_uri=mtbls_config.selected_profile_uri,
    )
    mtbls_model_root_path = Path(f"{working_dir}/mtbls_model")

    for mtbls_study_id in study_ids:
        errors_file_path = mhd_output_root_path / f"{mtbls_study_id}.mhd.errors.json"
        # if errors_file_path.exists():
        #     # if "characteristic-value node at index" not in errors_file_path.read_text():
        #     #     continue
        #     pass
        # else:
        #     continue
        mtbls_model_source_path = mtbls_model_root_path / Path(
            f"{mtbls_study_id}_model.json"
        )
        if not mtbls_model_source_path.exists():
            continue

        success, errors = convert_mtbls_study_to_mhd(
            mtbls_study_id,
            mtbls_config,
            mhd_output_root_path=mhd_output_root_path,
            mhd_announcement_output_root_path=mhd_output_root_path,
            mtbls_model_source_path=mtbls_model_source_path,
            convertor=legacy_convertor,
            errors_file_path=errors_file_path,
            force_recreate=force_recreate,
        )
        if success is None:
            logger.info("%s is skipped", mtbls_study_id)
            continue
        write_to_file(errors_file_path, success, errors)

        # ms_mtbls_config = get_default_config()
        # ms_mtbls_config.selected_schema_uri = MHD_MODEL_V0_1_SCHEMA_URI
        # ms_mtbls_config.selected_profile_uri = MHD_MODEL_V0_1_MS_PROFILE_URI
        # ms_mhd_output_root_path = Path(".outputs/mhd")

        # ms_convertor = factory.get_convertor(
        #     target_mhd_model_schema_uri=ms_mtbls_config.selected_schema_uri,
        #     target_mhd_model_profile_uri=ms_mtbls_config.selected_profile_uri,
        # )

        # errors_file_path = ms_mhd_output_root_path / f"{mtbls_study_id}.mhd.errors.json"
        # success, errors = convert_mtbls_study_to_mhd(
        #     mtbls_study_id,
        #     ms_mtbls_config,
        #     mhd_output_root_path=ms_mhd_output_root_path,
        #     mhd_announcement_output_root_path=ms_mhd_output_root_path,
        #     mtbls_model_source_path=mtbls_model_source_path,
        #     convertor=ms_convertor,
        #     errors_file_path=errors_file_path,
        # )
        # write_to_file(errors_file_path, success, errors)


def create_sdrf_file_from_mhd_file(working_dir: str = ".outputs"):
    files = list(Path(f"{working_dir}/mhd_legacy").glob("*.mhd.json"))
    sdrf_file_root_path = Path(f"{working_dir}/sdrf_files/legacy")
    Path(sdrf_file_root_path).mkdir(parents=True, exist_ok=True)

    for file in files:
        create_sdrf_files(str(file), sdrf_file_root_path)


if __name__ == "__main__":
    setup_basic_logging_config()
    create_mhd_legacy_profile(force_recreate=True, working_dir="tests")
    # create_sdrf_file_from_mhd_file()
