import glob
import logging
import os
import re
import subprocess
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
    ) -> Tuple[Union[None, StudyFolderMetadata], List[GenericMessage]]:
        messages: List[GenericMessage] = []
        study_folder_metadata = StudyFolderMetadata()
        metadata: Dict[str, StudyFileDescriptor] = {}
        self.visit_folder(study_path, study_path, metadata=metadata, messages=messages)
        study_folder_metadata.folders = {
            x: metadata[x] for x in metadata if metadata[x].is_directory
        }
        study_folder_metadata.files = {
            x: metadata[x] for x in metadata if not metadata[x].is_directory
        }
        data_folder_size = 0
        if calculate_data_folder_size:
            files_folder_path = os.path.join(study_path, "FILES")  # noqa: PTH118
            size = self.folder_size(files_folder_path)
            data_folder_size = size if size else 0

            study_folder_metadata.folder_size_in_bytes = data_folder_size

        if calculate_metadata_size:
            metadata_size = 0
            metadata_files = glob.glob(f"{study_path}/[asi]_*.txt")  # noqa: PTH207
            metadata_files = [x for x in metadata_files]
            maf_files = glob.glob(f"{study_path}/m_*.tsv")  # noqa: PTH207
            metadata_files.extend([x for x in maf_files])

            for item in metadata_files:
                stats = os.stat(item)  # noqa: PTH116
                metadata_size += stats.st_size

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

    def folder_size(self, directory: str) -> Union[int, None]:
        try:
            # Run the 'du' command to get the size of the directory in bytes
            directory = os.path.realpath(directory)
            result = subprocess.run(
                ["du", "-s", directory],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            if result.returncode == 0:
                size_str = result.stdout.split()[0]
                return int(size_str) * 1000
            else:
                logger.error("Error: %s", result.stderr.strip())
                return None
        except FileNotFoundError as e:
            logger.error("File not found:  %s", e)
            return None
        except Exception as e:
            logger.exception(e)
            return None
