import json
import shutil
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest
from click.testing import CliRunner
from mhd_model.model.definitions import (
    MHD_MODEL_V0_1_DEFAULT_SCHEMA_NAME,
    MHD_MODEL_V0_1_LEGACY_PROFILE_NAME,
)

from mtbls2mhd.commands.cli import cli
from mtbls2mhd.commands.fetch_mtbls_study import fetch_mtbls_data
from mtbls2mhd.commands.output_paths import resolve_output_file_path


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


def test_resolve_output_file_path_accepts_filename(output_dir: Path):
    output_path = resolve_output_file_path(output_dir, "MTBLS2_model.json")

    assert output_path == output_dir / "MTBLS2_model.json"


@pytest.mark.parametrize(
    "output_filename",
    [
        "../outside.json",
        "nested/outside.json",
        "/tmp/outside.json",
        r"..\outside.json",
        r"nested\outside.json",
        r"C:\tmp\outside.json",
    ],
)
def test_resolve_output_file_path_rejects_paths(
    output_filename: str, output_dir: Path
):
    with pytest.raises(ValueError, match="must be a file name"):
        resolve_output_file_path(output_dir, output_filename)


def test_fetch_mtbls_data_rejects_traversal_output_filename(
    monkeypatch: pytest.MonkeyPatch, output_dir: Path
):
    requested_urls = []

    def fake_get(*args, **kwargs):
        requested_urls.append(args[0])
        return SimpleNamespace(
            raise_for_status=lambda: None,
            json=lambda: {"content": {"study": "MTBLS2"}},
        )

    monkeypatch.setattr(
        "mtbls2mhd.commands.fetch_mtbls_study.httpx2.get", fake_get
    )

    result = fetch_mtbls_data(
        "MTBLS2",
        output_folder_path=str(output_dir),
        output_filename="../outside.json",
    )

    assert result is None
    assert requested_urls == []
    assert not (output_dir.parent / "outside.json").exists()


def test_fetch_mtbls_data_rejects_traversal_default_filename(
    monkeypatch: pytest.MonkeyPatch, output_dir: Path
):
    requested_urls = []

    def fake_get(*args, **kwargs):
        requested_urls.append(args[0])
        return SimpleNamespace(
            raise_for_status=lambda: None,
            json=lambda: {"content": {"study": "MTBLS2"}},
        )

    monkeypatch.setattr(
        "mtbls2mhd.commands.fetch_mtbls_study.httpx2.get", fake_get
    )

    result = fetch_mtbls_data(
        "../outside",
        output_folder_path=str(output_dir),
    )

    assert result is None
    assert requested_urls == []
    assert not (output_dir.parent / "outside_model.json").exists()


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
