import json
import shutil
import uuid
from pathlib import Path

import pytest
from click.testing import CliRunner
from mhd_model.model.definitions import (
    MHD_MODEL_V0_1_DEFAULT_SCHEMA_NAME,
    MHD_MODEL_V0_1_LEGACY_PROFILE_NAME,
)

from mtbls2mhd.commands.cli import cli


@pytest.fixture
def output_dir():
    path = Path(f".test-output/{uuid.uuid4().hex}")
    path.mkdir(parents=True, exist_ok=True)
    yield path
    shutil.rmtree(path)


def test_cli_help_01():
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    result = runner.invoke(cli)
    assert result.exit_code == 2
    assert result.output.startswith("Usage")


@pytest.mark.parametrize(
    "study_id",
    ["MTBLS2", "MTBLS3", "MTBLS1796", "MTBLS816", "MTBLS11733", "MTBLS13043"],
)
def test_download_studies(study_id: str, output_dir: Path):
    mtbls_ws_url = "https://wwwdev.ebi.ac.uk/metabolights/ws3"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "download",
            study_id,
            f"--output-dir={output_dir}",
            "--output-filename=" + study_id + ".json",
            "--mtbls-ws-url=" + mtbls_ws_url,
        ],
    )

    assert result.exit_code == 0
    assert (output_dir / f"{study_id}.json").exists()


@pytest.mark.parametrize(
    "study_id",
    ["MTBLS2", "MTBLS3", "MTBLS1796", "MTBLS816", "MTBLS11733", "MTBLS13043"],
)
def test_create_mhd_files_01(study_id: str, output_dir: Path):
    mtbls_ws_url = "https://wwwdev.ebi.ac.uk/metabolights/ws3"
    runner = CliRunner()
    result = runner.invoke(cli, ["create", "mhd", "--help"])
    assert result.exit_code == 0

    mhd_file_path = output_dir / f"{study_id}.mhd.json"
    result = runner.invoke(
        cli,
        [
            "create",
            "mhd",
            study_id,
            study_id,
            f"--output-dir={output_dir}",
            "--output-filename=" + mhd_file_path.name,
            "--profile-uri=" + MHD_MODEL_V0_1_LEGACY_PROFILE_NAME,
            "--schema-uri=" + MHD_MODEL_V0_1_DEFAULT_SCHEMA_NAME,
            "--mtbls-ws-url=" + mtbls_ws_url,
        ],
    )
    assert result.exit_code == 0
    assert mhd_file_path.exists()

    result = runner.invoke(cli, ["create", "announcement", "--help"])
    assert result.exit_code == 0

    # TODO: target_mhd_model_file_url should be updated
    target_mhd_model_file_url = (
        "https://ftp.ebi.ac.uk/pub/databases/metabolights/studies/public/"
        + study_id.upper()
    )
    announcement_file_path = output_dir / f"{study_id}.announcement.json"
    result = runner.invoke(
        cli,
        [
            "create",
            "announcement",
            study_id,
            str(mhd_file_path),
            target_mhd_model_file_url,
            f"--output-dir={output_dir}",
            "--output-filename=" + announcement_file_path.name,
        ],
    )

    assert result.exit_code == 0
    assert announcement_file_path.exists()

    result = runner.invoke(cli, ["validate", "mhd", "--help"])
    assert result.exit_code == 0
    output_path = output_dir / Path(f"{study_id}.mhd.validation.json")
    result = runner.invoke(
        cli,
        [
            "validate",
            "mhd",
            study_id,
            str(mhd_file_path),
            "--output-path=" + str(output_path),
        ],
    )
    assert result.exit_code == 0
    try:
        validation = json.loads(output_path.read_text())
        assert validation.get("success")
    except Exception as ex:
        pytest.fail(f"MHD validation file is not valid JSON: {ex}")

    result = runner.invoke(cli, ["validate", "announcement", "--help"])
    assert result.exit_code == 0

    output_path = output_dir / Path(f"{study_id}.announcement.validation.json")

    result = runner.invoke(
        cli,
        [
            "validate",
            "announcement",
            study_id,
            str(announcement_file_path),
            "--output-path=" + str(output_path),
        ],
    )
    try:
        validation = json.loads(output_path.read_text())
        assert validation.get("success")
    except Exception as ex:
        pytest.fail(f"MHD validation file is not valid JSON: {ex}")
    assert result.exit_code == 0
