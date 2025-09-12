import asyncio
from datetime import datetime, timezone
from functools import lru_cache
from logging import getLogger
from typing import Any, Dict, List

import psycopg2
from metabolights_utils.models.common import ErrorMessage
from metabolights_utils.models.metabolights.model import (
    CurationRequest,
    StudyDBMetadata,
    StudyStatus,
    Submitter,
    UserRole,
    UserStatus,
)
from metabolights_utils.provider.study_provider import AbstractDbMetadataCollector
from psycopg2.extras import DictCursor
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from mtbls2mhd.config import mtbls2mhd_config
from mtbls2mhd.v0_1.legacy.mtbls_study_schema import Study

logger = getLogger(__file__)


@lru_cache(1)
def get_session_factory():
    db = mtbls2mhd_config
    url = "".join(
        [
            "postgresql+asyncpg://",
            db.database_user,
            ":",
            db.database_user_password,
            "@",
            db.database_host,
            ":",
            str(db.database_host_port),
            "/",
            db.database_name,
        ]
    )
    engine = create_async_engine(
        url,
        future=True,
        # json_serializer=jsonable_encoder,
        pool_size=3,
        max_overflow=7,
    )

    # expire_on_commit=False will prevent attributes from being expired after commit.
    AsyncSessionFactory = sessionmaker(
        engine, autoflush=False, expire_on_commit=False, class_=AsyncSession
    )

    return AsyncSessionFactory


STUDY_FIELDS = [
    "id",
    "acc",
    "obfuscationcode",
    "submissiondate",
    "releasedate",
    "updatedate",
    "studysize",
    "status_date",
    "studytype",
    "status",
    "override",
    "comment",
    "curation_request",
]

SUBMITTER_FIELDS = [
    "id",
    "orcid",
    "address",
    "joindate",
    "username",
    "firstname",
    "lastname",
    "status",
    "affiliation",
    "affiliationurl",
    "role",
]


logger = getLogger(__file__)


def create_postgresql_connection():
    """
    Creates and returns a PostgreSQL connection.
    """
    try:
        connection = psycopg2.connect(
            dbname=mtbls2mhd_config.database_name,
            user=mtbls2mhd_config.database_user,
            password=mtbls2mhd_config.database_user_password,
            host=mtbls2mhd_config.database_host,
            port=mtbls2mhd_config.database_host_port,
        )
        return connection
    except psycopg2.Error as e:
        logger.exception(e)
        raise e


class DbMetadataCollector(AbstractDbMetadataCollector):
    def __init__(self):
        pass

    def get_study_metadata_from_db(self, study_id: str, connection):
        try:
            study = self._get_study_from_db(study_id, connection)
            submitters = self._get_study_submitters_from_db(study_id, connection)
            study_db_metadata = self._create_study_db_metadata(study, submitters)
            return study_db_metadata, []
        except Exception as ex:
            return StudyDBMetadata(), [
                ErrorMessage(short="Error while loading db metadata", detail=str(ex))
            ]

    async def get_all_public_and_review_study_ids_from_db(self):
        AsyncSessionFactory = get_session_factory()
        async with AsyncSessionFactory() as db_session:
            stmt = select(Study.acc, Study.status).where(
                or_(
                    Study.status == StudyStatus.PUBLIC.value,
                    Study.status == StudyStatus.INREVIEW.value,
                )
            )
            result = await db_session.execute(stmt)

            study_ids: List[Dict[str, Any]] = []
            if result:
                for item in result:
                    if item[0] not in study_ids:
                        study_ids.append({"acc": item[0], "status": item[1]})

            return study_ids

    async def get_all_public_study_ids_from_db(self):
        AsyncSessionFactory = get_session_factory()
        async with AsyncSessionFactory() as db_session:
            stmt = select(Study.acc, Study.updatedate, Study.status).where(
                Study.status == StudyStatus.PUBLIC.value
            )
            result = await db_session.execute(stmt)

            study_ids: List[Dict[str, Any]] = []
            if result:
                for item in result:
                    if item[0] not in study_ids:
                        study_ids.append({"acc": item[0], "updatedate": item[1]})

            return study_ids

    def get_updated_public_study_ids_from_db(
        self,
        connection,
        min_last_update_date: datetime = None,
        max_last_update_date: datetime = None,
    ):
        _filter = ["status = 3"]

        if min_last_update_date:
            min_last_update_date_str = min_last_update_date.strftime("%Y-%m-%d")
            _filter.append(f"status_date >= '{min_last_update_date_str}'")
        if max_last_update_date:
            max_last_update_date_str = max_last_update_date.strftime("%Y-%m-%d")
            _filter.append(f"status_date <= '{max_last_update_date_str}'")
        where_clause = " and ".join(_filter)
        _input = f"select acc from studies where {where_clause};"
        try:
            cursor = connection.cursor(cursor_factory=DictCursor)
            cursor.execute(_input)
            data = cursor.fetchall()
            return data

        except Exception as ex:
            raise ex

    async def get_all_study_ids_from_db(self, connection):
        AsyncSessionFactory = get_session_factory()
        async with AsyncSessionFactory() as db_session:
            stmt = select(Study.acc, Study.status)
            result = await db_session.execute(stmt)

            study_ids: List[Dict[str, Any]] = []
            if result:
                for item in result:
                    if item[0] not in study_ids:
                        study_ids.append({"acc": item[0]})

            return study_ids

    def _get_study_from_db(self, study_id: str, connection):
        _input = (
            f"select {', '.join(STUDY_FIELDS)} from studies where acc = %(study_id)s;"
        )
        try:
            cursor = connection.cursor(cursor_factory=DictCursor)
            cursor.execute(_input, {"study_id": study_id})
            data = cursor.fetchone()
            return data

        except Exception as ex:
            raise ex

    def _get_study_submitters_from_db(self, study_id: str, connection):
        submitter_fields = [f"u.{field}" for field in SUBMITTER_FIELDS]

        _input = f"select {', '.join(submitter_fields)} from studies as s, study_user as su, \
            users as u where su.userid = u.id and su.studyid = s.id and s.acc = %(study_id)s;"
        try:
            cursor = connection.cursor(cursor_factory=DictCursor)
            cursor.execute(_input, {"study_id": study_id})
            data = cursor.fetchall()
            if data:
                return [dict(item) for item in data]

            return data
        except Exception as ex:
            raise ex

    def _create_study_db_metadata(
        self, study, submitters: List[Dict[str, Any]]
    ) -> StudyDBMetadata:
        study_db_metadata: StudyDBMetadata = StudyDBMetadata()
        study_db_metadata.db_id = study["id"] or -1
        study_db_metadata.study_id = study["acc"] or ""
        study_db_metadata.obfuscation_code = study["obfuscationcode"] or ""
        if study_db_metadata.study_id:
            study_no: str = study_db_metadata.study_id.replace("MTBLS", "").replace(
                "REQ", ""
            )
            if study_no.isnumeric():
                study_db_metadata.numeric_study_id = int(study_no)
        if study["status"] > -1:
            study_db_metadata.status = StudyStatus.get_from_int(study["status"])

        if study["studytype"] and len(study["studytype"].strip()) > 0:
            study_db_metadata.study_types = study["studytype"].strip().split(";")

        if study["override"] and len(study["override"].strip()) > 0:
            override_list = study["override"].strip().split("|")
            overrides = {}
            for item in override_list:
                if item:
                    key_value = item.split(":")
                    if len(key_value) > 1:
                        overrides[key_value[0]] = key_value[1] or ""
            study_db_metadata.overrides = overrides
        study_db_metadata.study_size = int(study["studysize"])
        study_db_metadata.submission_date = self._get_date_string(
            study["submissiondate"]
        )
        study_db_metadata.curation_request = CurationRequest.get_from_int(
            study["curation_request"]
        )
        study_db_metadata.release_date = self._get_date_string(study["releasedate"])
        study_db_metadata.update_date = self._get_date_time_string(study["updatedate"])
        study_db_metadata.status_date = self._get_date_time_string(study["status_date"])
        study_db_metadata.submitters = self._create_submitters(submitters)
        return study_db_metadata

    def _create_submitters(self, submitters: List[Dict[str, Any]]) -> List[Submitter]:
        if not submitters:
            return []
        submitter_metadata_list = []
        for submitter in submitters:
            submitter_metadata = Submitter()
            submitter_metadata.address = submitter["address"] or ""
            submitter_metadata.affiliation = submitter["affiliation"] or ""
            submitter_metadata.affiliation_url = submitter["affiliationurl"] or ""
            submitter_metadata.db_id = submitter["id"] or -1
            submitter_metadata.first_name = submitter["firstname"] or ""
            submitter_metadata.last_name = submitter["lastname"] or ""
            submitter_metadata.user_name = submitter["username"] or ""
            submitter_metadata.orcid = submitter["orcid"] or ""
            submitter_metadata.join_date = self._get_date_string(submitter["joindate"])
            submitter_metadata.role = (
                UserRole.get_from_int(submitter["role"])
                if submitter["role"] is not None
                else UserRole.ANONYMOUS
            )
            submitter_metadata.status = (
                UserStatus.get_from_int(submitter["status"])
                if submitter["status"] is not None
                else UserStatus.FROZEN
            )
            submitter_metadata_list.append(submitter_metadata)
        return submitter_metadata_list

    @classmethod
    def _get_date_string(cls, date_value: datetime, pattern: str = "%Y-%m-%d"):
        if not date_value:
            return ""
        return date_value.strftime(pattern)

    @classmethod
    def _get_date_time_string(cls, date_value: datetime):
        if not date_value:
            return ""

        return datetime.fromtimestamp(
            date_value.timestamp(), tz=timezone.utc
        ).isoformat()


if __name__ == "__main__":
    db = DbMetadataCollector()
    result = asyncio.run(db.get_all_public_study_ids_from_db())
    logger.info(result)
