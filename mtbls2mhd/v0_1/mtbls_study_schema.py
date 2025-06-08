from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    ForeignKey,
    Numeric,
    String,
    Table,
    Text,
    text,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()
metadata = Base.metadata


class StudyRevision(Base):
    __tablename__ = "study_revisions"

    id = Column(BigInteger, primary_key=True)
    accession_number = Column(String(255), nullable=False)
    revision_number = Column(BigInteger, nullable=False)
    revision_datetime = Column(DateTime, nullable=False)
    revision_comment = Column(String(1024), nullable=False)
    created_by = Column(String(255), nullable=False)
    status = Column(BigInteger, nullable=False, default=0)
    task_started_at = Column(DateTime, nullable=True)
    task_completed_at = Column(DateTime, nullable=True)
    task_message = Column(Text, nullable=True)


t_study_user = Table(
    "study_user",
    metadata,
    Column("userid", ForeignKey("users.id"), primary_key=True, nullable=False),
    Column("studyid", ForeignKey("studies.id"), primary_key=True, nullable=False),
)


class User(Base):
    __tablename__ = "users"

    id = Column(BigInteger, primary_key=True)
    address = Column(String(255))
    affiliation = Column(String(255))
    affiliationurl = Column(String(255))
    apitoken = Column(String(255), unique=True)
    email = Column(String(255))
    firstname = Column(String(255))
    joindate = Column(DateTime)
    lastname = Column(String(255))
    password = Column(String(255))
    role = Column(BigInteger, nullable=False)
    partner = Column(BigInteger, nullable=False, default=0)
    status = Column(BigInteger, nullable=False)
    username = Column(String(255), unique=True)
    orcid = Column(String(255))
    metaspace_api_key = Column(String(255))

    studies = relationship("Study", secondary="study_user", back_populates="users")


class Study(Base):
    __tablename__ = "studies"

    id = Column(BigInteger, primary_key=True)
    acc = Column(String(255), unique=True)
    releasedate = Column(DateTime, nullable=False)
    status = Column(BigInteger, nullable=False)
    studysize = Column(Numeric(38, 0))
    updatedate = Column(
        DateTime,
        nullable=False,
        server_default=text("('now'::text)::timestamp without time zone"),
    )
    submissiondate = Column(
        DateTime,
        nullable=False,
        server_default=text("('now'::text)::timestamp without time zone"),
    )
    studytype = Column(String(1000))
    biostudies_acc = Column(String)
    status_date = Column(DateTime)
    curation_request = Column(BigInteger, nullable=False)
    reserved_accession = Column(Text, nullable=True)
    reserved_submission_id = Column(Text, nullable=True)
    first_public_date = Column(DateTime, nullable=True)
    first_private_date = Column(DateTime, nullable=True)
    dataset_license = Column(String)
    revision_number = Column(BigInteger, nullable=False, default=0)
    revision_datetime = Column(DateTime, nullable=True)

    users = relationship("User", secondary="study_user", back_populates="studies")
