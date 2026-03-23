import json
import logging
from pathlib import Path

import click
import httpx

logger = logging.getLogger(__name__)


@click.command(name="download", no_args_is_help=True)
@click.option(
    "--output-dir",
    default="outputs",
    show_default=True,
    help="Output directory for MetaboLights model file",
)
@click.option(
    "--output-filename",
    default=None,
    show_default=True,
    help="MHD filename (e.g., MTBLS2_model.json)",
)
@click.option(
    "--mtbls-ws-url",
    default="https://www.ebi.ac.uk/metabolights/ws3",
    show_default=True,
    help="MetaboLights Webservice Endpoint URL",
)
@click.argument("mtbls_study_id")
def fetch_mtbls_study(
    mtbls_study_id: str, output_dir: str, output_filename: str, mtbls_ws_url: str
):
    """Download a MetaboLights metadata (ISA-TAB + DB) as json file."""
    file_path = fetch_mtbls_data(
        mtbls_study_id,
        output_folder_path=output_dir,
        output_filename=output_filename,
        mtbls_ws_url=mtbls_ws_url,
    )
    if not file_path:
        click.echo(f"{mtbls_study_id} failed.")
        exit(1)
    click.echo(f"{mtbls_study_id} MetaboLights model is downloaded: {file_path}")


def fetch_mtbls_data(
    study_id: str,
    output_folder_path: str,
    output_filename: None | str = None,
    mtbls_ws_url: str = "https://www.ebi.ac.uk/metabolights/ws3",
) -> Path:
    try:
        data_path: Path = Path(str(output_folder_path))
        data_path.mkdir(parents=True, exist_ok=True)
        output_filename = output_filename or f"{study_id}_model.json"
        study_path = data_path / Path(output_filename)
        url = f"{mtbls_ws_url}/submissions/v2/validations/{study_id}/metabolights-model"
        response = httpx.get(url, timeout=60)
        response.raise_for_status()
        json_data = response.json()
        with study_path.open("w") as f:
            json.dump(json_data.get("content", {}), f, indent=4)
        return str(study_path)

    except Exception as ex:
        logger.error("%s: %s", study_id, ex)

        return None


if __name__ == "__main__":
    fetch_mtbls_study(["MTBLS2"])
