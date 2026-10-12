import glob
import json
import logging
import os
import re
from pathlib import Path
from typing import Dict, List, Tuple, Union

from metabolights_utils.models.common import GenericMessage, InfoMessage, WarningMessage
from metabolights_utils.models.metabolights.model import (
    StudyFileDescriptor,
    StudyFolderMetadata,
)
from metabolights_utils.provider import definitions
from metabolights_utils.provider.study_provider import (
    AbstractFolderMetadataCollector,
)

logger = logging.getLogger(__name__)


class LocalFolderMetadataCollector(AbstractFolderMetadataCollector):
    def __init__(self):
        pass

    def visit_folder(
        self,
        directory: str,
        study_path: str,
        metadata: Dict[str, StudyFileDescriptor],
        messages: List[GenericMessage],
        data_files_path: str = "FILES",
        data_files_mapping_folder_name: None | str = None,
    ):
        try:
            dir_relative_path = str(directory).replace(
                f"{str(study_path).rstrip(os.sep)}", ""
            )
            dir_relative_path = dir_relative_path.lstrip("/")
            skip_content = False
            for pattern in definitions.skip_folder_content_patterns:
                if pattern.match(dir_relative_path):
                    skip_content = True
                    break
            if skip_content:
                messages.append(
                    InfoMessage(
                        short=f"{dir_relative_path} directory is in content ignore list. SKIPPED"
                    )
                )
                return
            dir_path = Path(directory)
            # entries = os.listdir(directory)
            for entry in dir_path.iterdir():
                full_path: Path = entry
                if not full_path.exists():
                    logger.warning("File does not exist: %s", full_path)
                    continue
                relative_path = Path(dir_relative_path) / Path(entry.name)
                base_name = relative_path.name
                parent_directory = ""
                if str(relative_path.parent) != ".":
                    parent_directory = str(relative_path.parent)

                in_ignore_list = False
                for pattern in definitions.ignore_file_patterns:
                    if pattern.match(str(relative_path)):
                        in_ignore_list = True
                        break
                if in_ignore_list:
                    messages.append(
                        InfoMessage(
                            short=f"{str(relative_path)} is in ignore list. SKIPPED."
                        )
                    )
                    continue

                descriptor = StudyFileDescriptor()

                for tag in definitions.TAG_PATTERNS:
                    for pattern in definitions.TAG_PATTERNS[tag]:
                        if re.match(pattern, base_name, re.IGNORECASE):
                            descriptor.tags.append(tag)

                ext = relative_path.suffix
                descriptor.extension = ext
                descriptor.base_name = base_name
                descriptor.parent_directory = parent_directory
                descriptor.file_path = str(relative_path)

                descriptor.is_directory = full_path.is_dir()
                descriptor.is_link = full_path.is_symlink()
                if full_path.exists():
                    stats = full_path.stat()
                    if descriptor.is_directory:
                        descriptor.size_in_bytes = 0
                    else:
                        descriptor.size_in_bytes = stats.st_size
                    descriptor.created_at = int(stats.st_ctime)
                    descriptor.modified_at = int(stats.st_mtime)
                    descriptor.mode = oct(stats.st_mode & 0o777).replace("0o", "")
                metadata[str(relative_path)] = descriptor

                if full_path.is_dir():
                    self.visit_folder(
                        full_path, study_path, metadata=metadata, messages=messages
                    )

        except PermissionError as ex:
            messages.append(
                WarningMessage(
                    short=f"{directory} directory permission error {str(ex)}"
                )
            )
        except Exception as exc:
            messages.append(
                WarningMessage(short=f"{directory} directory error {str(exc)}")
            )

    def get_folder_metadata(
        self,
        study_path,
        calculate_data_folder_size: bool = False,
        calculate_metadata_size: bool = False,
        data_files_path: str = "FILES",
        data_files_mapping_folder_name: None | str = None,
    ) -> Tuple[Union[None, StudyFolderMetadata], List[GenericMessage]]:
        messages: List[GenericMessage] = []
        study_folder_metadata = StudyFolderMetadata()
        metadata: Dict[str, StudyFileDescriptor] = {}
        data_files_json = (
            Path(study_path) / "INTERNAL_FILES/DATA_FILES/data_file_index.json"
        )
        data_files = json.loads(data_files_json.read_bytes())
        public_data_files = data_files.get("public_data_files", {})
        private_data_files = data_files.get("private_data_files", {})

        for k in public_data_files:
            file = None
            if k in private_data_files:
                file = private_data_files[k]
            else:
                file = public_data_files[k]

            desc = StudyFileDescriptor(
                file_path=file.get("relative_path", ""),
                base_name=file.get("name", ""),
                parent_directory=file.get("parent_relative_path", ""),
                modified_at=file.get("modified_time", 0),
                extension=file.get("extension"),
                is_directory=file.get("is_dir"),
                size_in_bytes=file.get("file_size"),
            )
            metadata[k] = desc
        for k in private_data_files:
            if k in metadata:
                file = private_data_files[k]
                desc = StudyFileDescriptor(
                    file_path=file.get("relative_path", ""),
                    base_name=file.get("name", ""),
                    parent_directory=file.get("parent_relative_path", ""),
                    modified_at=file.get("modified_time", 0),
                    extension=file.get("extension"),
                    is_directory=file.get("is_dir"),
                    size_in_bytes=file.get("file_size"),
                )
                metadata[k] = desc
        metadata_files = glob.glob(f"{study_path}/[asi]_*.txt")  # noqa: PTH207
        metadata_files = [x for x in metadata_files]
        maf_files = glob.glob(f"{study_path}/m_*.tsv")  # noqa: PTH207
        metadata_files.extend([x for x in maf_files])

        for item in metadata_files:
            file = Path(item)
            basename = file.name
            metadata[basename] = StudyFileDescriptor(
                file_path=basename,
                base_name=basename,
                parent_directory="",
                modified_at=int(file.stat().st_mtime),
                extension=file.suffix,
                is_directory=file.is_dir(),
                size_in_bytes=file.stat().st_size,
            )

        study_folder_metadata.folders = {
            x: metadata[x] for x in metadata if metadata[x].is_directory
        }
        study_folder_metadata.files = {
            x: metadata[x] for x in metadata if not metadata[x].is_directory
        }
        data_folder_size = 0
        if calculate_data_folder_size:
            data_folder_size = sum(
                [
                    metadata[x].size_in_bytes
                    for x in metadata
                    if metadata[x].file_path.startswith("FILES/")
                ]
            )

            study_folder_metadata.folder_size_in_bytes = data_folder_size

        if calculate_metadata_size:
            metadata_size = sum(
                [
                    metadata[x].size_in_bytes
                    for x in metadata
                    if metadata[x].file_path.startswith("FILES/")
                ]
            )
            study_folder_metadata.metadata_folder_size_calculated = metadata_size
            if study_folder_metadata.folder_size_in_bytes >= 0:
                study_folder_metadata.folder_size_in_bytes += metadata_size
            else:
                study_folder_metadata.folder_size_in_bytes = metadata_size

        total_size = study_folder_metadata.folder_size_in_bytes
        if total_size > -1:
            if total_size / (1024**3) >= 1:
                study_folder_metadata.folder_size_in_str = (
                    str(round(total_size / (1024**3), 2)) + "GB"
                )
            else:
                study_folder_metadata.folder_size_in_str = (
                    str(round(total_size / (1024**2), 2)) + "MB"
                )

        return study_folder_metadata, messages
