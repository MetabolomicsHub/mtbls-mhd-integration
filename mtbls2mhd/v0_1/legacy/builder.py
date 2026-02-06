import datetime
import enum
import json
import logging
import re
from pathlib import Path
from typing import OrderedDict

from metabolights_utils.models.isa.assay_file import AssayFile
from metabolights_utils.models.isa.common import IsaTableColumn
from metabolights_utils.models.isa.enums import ColumnsStructure
from metabolights_utils.models.isa.investigation_file import Assay, Study
from metabolights_utils.models.isa.samples_file import SamplesFile
from metabolights_utils.models.metabolights.model import (
    MetabolightsStudyModel,
    UserStatus,
)
from metabolights_utils.provider.study_provider import (
    MetabolightsStudyProvider,
)
from mhd_model.model.v0_1.dataset.profiles.base import graph_nodes as mhd_domain
from mhd_model.model.v0_1.dataset.profiles.base.dataset_builder import MhDatasetBuilder
from mhd_model.model.v0_1.dataset.profiles.base.profile import MhDatasetBaseProfile
from mhd_model.model.v0_1.dataset.profiles.base.relationships import Relationship
from mhd_model.model.v0_1.dataset.profiles.legacy.profile import MhDatasetLegacyProfile
from mhd_model.model.v0_1.rules.managed_cv_terms import (
    COMMON_ASSAY_TYPES,
    COMMON_CHARACTERISTIC_DEFINITIONS,
    COMMON_MEASUREMENT_TYPES_MAP,
    COMMON_OMICS_TYPES,
    COMMON_PARAMETER_DEFINITIONS,
    COMMON_PROTOCOLS,
    COMMON_STUDY_FACTOR_DEFINITIONS,
    COMMON_TECHNOLOGY_TYPES,
    REQUIRED_COMMON_PARAMETER_DEFINITIONS,
)
from mhd_model.shared.fields import DOI
from mhd_model.shared.model import CvTerm, Revision, UnitCvTerm
from pydantic import BaseModel, HttpUrl, ValidationError

from mtbls2mhd.config import Mtbls2MhdConfiguration
from mtbls2mhd.v0_1.legacy.db_metadata_collector import (
    DbMetadataCollector,
    create_postgresql_connection,
)
from mtbls2mhd.v0_1.legacy.folder_metadata_collector import LocalFolderMetadataCollector

logger = logging.getLogger(__name__)


## MTBLS RELATED CONFIGURATION ###
##############################################################################################################
MTBLS_ASSAY_TYPES = {
    "LC-MS": COMMON_ASSAY_TYPES["OBI:0003097S"],
    "GC-MS": COMMON_ASSAY_TYPES["OBI:0003110"],
    "CE-MS": COMMON_ASSAY_TYPES["OBI:0003741"],
    "GCxGC-MS": COMMON_ASSAY_TYPES["OBI:0003110"],
    "FIA-MS": COMMON_ASSAY_TYPES["OBI:0000470"],
    "MALDI-MS": COMMON_ASSAY_TYPES["OBI:0000470"],
    "DI-MS": COMMON_ASSAY_TYPES["OBI:0000470"],
    "MS": COMMON_ASSAY_TYPES["OBI:0000470"],
}
MTBLS_MEASUREMENT_TYPES = {
    "targeted": COMMON_MEASUREMENT_TYPES_MAP["MSIO:0000100"],
    "untargeted": COMMON_MEASUREMENT_TYPES_MAP["MSIO:0000101"],
}

DEFAULT_MEASUREMENT_TYPE = COMMON_MEASUREMENT_TYPES_MAP["MSIO:0000101"]

DEFAULT_OMICS_TYPE = COMMON_OMICS_TYPES["EDAM:3172"]

COMMON_PROTOCOLS_MAP = {
    "Sample collection": COMMON_PROTOCOLS["EFO:0005518"],
    "Extraction": COMMON_PROTOCOLS["MS:1000831"],
    "Mass spectrometry": COMMON_PROTOCOLS["CHMO:0000470"],
    "Data transformation": COMMON_PROTOCOLS["OBI:0200000"],
    "Metabolite identification": COMMON_PROTOCOLS["MI:2131"],
    "Chromatography": COMMON_PROTOCOLS["CHMO:0001000"],
    "Treatment": COMMON_PROTOCOLS["EFO:0003969"],
    "Flow Injection Analysis": COMMON_PROTOCOLS["MS:1000058"],
    "Capillary Electrophoresis": COMMON_PROTOCOLS["CHMO:0001024"],
    # TODO: Update after adding to managed CV terms
}

MTBLS_PROTOCOLS_MAP = COMMON_PROTOCOLS_MAP.copy()

MTBLS_PROTOCOLS_MAP.update(
    {
        "Direct infusion": CvTerm(
            source="MS",
            accession="MS:1000060",
            name="infusion",
        ),
    }
)
MANAGED_CHARACTERISTICS_MAP = {
    "organism": COMMON_CHARACTERISTIC_DEFINITIONS["NCIT:C14250"],
    "organism part": COMMON_CHARACTERISTIC_DEFINITIONS["NCIT:C103199"],
    "disease": COMMON_CHARACTERISTIC_DEFINITIONS["EFO:0000408"],
    "cell type": COMMON_CHARACTERISTIC_DEFINITIONS["EFO:0000324"],
    # "geographic location": COMMON_CHARACTERISTIC_DEFINITIONS["GAZ:00000448"],
}

MTBLS_CHARACTERISTICS_MAP = MANAGED_CHARACTERISTICS_MAP.copy()
MTBLS_CHARACTERISTICS_MAP.update(
    {
        "phenotype": CvTerm(source="EFO", accession="EFO:0000651", name="phenotype"),
    }
)

MANAGED_STUDY_FACTOR_MAP = {
    "disease": COMMON_STUDY_FACTOR_DEFINITIONS["EFO:0000408"],
    "treatment": CvTerm(source="EFO", accession="EFO:0000727", name="treatment"),
}
MTBLS_STUDY_FACTOR_MAP = MANAGED_STUDY_FACTOR_MAP.copy()
MTBLS_STUDY_FACTOR_MAP.update({})


REQUIRED_PROTOCOL_PARAMETER_VALUE_MAP = {
    "Mass spectrometry": {
        "Instrument": REQUIRED_COMMON_PARAMETER_DEFINITIONS["MSIO:0000171"],
    },
}

MTBLS_PROTOCOL_PARAMETER_DEFINITION_MAP = {}

MTBLS_PROTOCOL_PARAMETER_DEFINITION_MAP.update(
    {
        "Mass spectrometry": {
            **REQUIRED_PROTOCOL_PARAMETER_VALUE_MAP["Mass spectrometry"],
            "Scan polarity": COMMON_PARAMETER_DEFINITIONS["MS:1000465"],
            "Ion source": COMMON_PARAMETER_DEFINITIONS["CHMO:0000960"],
            "Mass analyzer": COMMON_PARAMETER_DEFINITIONS["OBI:0000345"],
            "CE instrument": COMMON_PARAMETER_DEFINITIONS["OBI:0001132"],
            "Scan m/z range": COMMON_PARAMETER_DEFINITIONS["MTBLS:50020"],
            "FIA instrument": COMMON_PARAMETER_DEFINITIONS["MTBLS:50021"],
        },
        "Chromatography": {
            "Column model": COMMON_PARAMETER_DEFINITIONS["MTBLS:50001"],
            "Column type": COMMON_PARAMETER_DEFINITIONS["MTBLS:50002"],
            "Guard column": COMMON_PARAMETER_DEFINITIONS["MTBLS:50003"],
            "Autosampler model": COMMON_PARAMETER_DEFINITIONS["MTBLS:50004"],
        },
        "Extraction": {
            "Post Extraction": COMMON_PARAMETER_DEFINITIONS["MTBLS:50010"],
            "Derivatization": COMMON_PARAMETER_DEFINITIONS["MTBLS:50011"],
        },
    }
)


FILE_EXTENSIONS: dict[tuple[str, bool], CvTerm] = {
    (".d", True): CvTerm(
        source="MS", accession="MS:1002302", name="Bruker Container format"
    ),
    (".raw", False): CvTerm(source="EDAM", accession="EDAM:3712", name="Thermo RAW"),
    (".raw", True): CvTerm(source="EDAM", accession="EDAM:3858", name="Waters RAW"),
    (".wiff", False): CvTerm(source="EFO", accession="EDAM:3710", name="WIFF format"),
    (".mzml", False): CvTerm(source="EDAM", accession="EDAM:3244", name="mzML"),
    (".mzdata", False): CvTerm(source="EFO", accession="EDAM:3834", name="mzData"),
    (".mzxml", False): CvTerm(source="EDAM", accession="EDAM:3654", name="mzXML"),
    (".ibd", False): CvTerm(source="EDAM", accession="EDAM:3839", name="ibd"),
}

# DEFAULT_RAW_DATA_FILE_FORMAT = CvTerm(
#     source="EDAM", accession="EDAM:3245", name="Mass spectrometry data format"
# )

# DEFAULT_DERIVED_DATA_FILE_FORMAT = CvTerm(
#     source="EDAM", accession="EDAM:3245", name="Mass spectrometry data format"
# )


class ProtocolRunSummary(BaseModel):
    protocol_name: str = ""
    protocol: None | mhd_domain.Protocol = None
    column: None | IsaTableColumn = None
    sample_run_configurations: OrderedDict[str, mhd_domain.SampleRunConfiguration] = (
        OrderedDict()
    )


def create_cv_term_object(
    type_: str, accession: str, source: str, name: str
) -> mhd_domain.CvTermObject:
    if accession and accession.lower().startswith("mtbls"):
        accession = ""
    if source and source.lower().startswith("mtbls"):
        source = ""
    if not source or not accession:
        return mhd_domain.CvTermObject(type_=type_, name=name)

    return mhd_domain.CvTermObject(
        type_=type_, accession=accession, source=source, name=name
    )


def create_cv_term_value_object(
    type_: str,
    accession: str = "",
    source: str = "",
    name: str = "",
    value: None | str = None,
    unit: None | UnitCvTerm = None,
) -> mhd_domain.CvTermValueObject:
    if accession and accession.lower().startswith("mtbls"):
        accession = ""
    if source and source.lower().startswith("mtbls"):
        source = ""
    unit_cv = unit
    if unit:
        if unit.accession and unit.accession.lower().startswith("mtbls"):
            unit.accession = ""
        if unit.source and unit.source.lower().startswith("mtbls"):
            unit.source = ""
        if not source or not accession:
            unit_cv = UnitCvTerm(name=unit.name) if unit else None

    if not source or not accession:
        return mhd_domain.CvTermValueObject(
            type_=type_, name=name, value=value, unit=unit_cv
        )

    return mhd_domain.CvTermValueObject(
        type_=type_,
        accession=accession,
        source=source,
        name=name,
        value=value,
        unit=unit_cv,
    )


class BuildType(enum.Enum):
    MINIMUM = "minimal_mhd_model"
    FULL = "full"
    FULL_AND_CUSTOM_NODES = "full_and_custom_nodes"


class MhdLegacyDatasetBuilder:
    def convert_to_curie(self, source_ref: str, uri: str) -> str:
        if not uri:
            return ""

        parts = uri.split("/")
        if len(parts) > 1:
            part = parts[-1]
            subpart = part.split("_")
            if len(subpart) > 1:
                part = subpart[-1].split("#")
                accession = part[0]
                if len(part) > 1:
                    accession = part[-1]
                result = f"{subpart[0]}:{accession}"
                return result
            result = f"{source_ref}:{subpart[0]}"
            return result
        if ":" in uri:
            return uri
        result = f"{source_ref}:{uri}"
        return result

    def add_contacts(
        self,
        data: MetabolightsStudyModel,
        mhd_builder: MhDatasetBuilder,
        mhd_study: mhd_domain.Study,
        build_type: BuildType = BuildType.FULL,
    ):
        organizations = {}
        if build_type == BuildType.MINIMUM:
            # Add submitters as contacts only
            if data.study_db_metadata and data.study_db_metadata.submitters:
                for submitter in data.study_db_metadata.submitters:
                    mhd_contact = mhd_domain.Person(
                        full_name=" ".join(
                            [
                                x
                                for x in (
                                    submitter.first_name,
                                    submitter.last_name,
                                )
                                if x
                            ]
                        ),
                        email_list=[submitter.user_name],
                    )
                    mhd_builder.add(mhd_contact)
                    mhd_builder.link(
                        mhd_contact,
                        "submits",
                        mhd_study,
                        reverse_relationship_name="submitted-by",
                    )
        else:
            study = data.investigation.studies[0]
            contacts: dict[str, mhd_domain.Person] = {}
            contributers = {}
            for idx, contact in enumerate(study.study_contacts.people, start=1):
                mhd_contact = None
                try:
                    mhd_contact = mhd_domain.Person(
                        full_name=" ".join(
                            [
                                x
                                for x in (
                                    contact.first_name,
                                    contact.mid_initials,
                                    contact.last_name,
                                )
                                if x
                            ]
                        ),
                        email_list=[contact.email] if contact.email else None,
                        address_list=[contact.address] if contact.address else None,
                        phone_list=[contact.phone] if contact.phone else None,
                    )
                except ValidationError as e:
                    logger.error(
                        "%s study %s. contact email format is not valid. Contact will be ignored. '%s' %s",
                        mhd_study.repository_identifier,
                        idx,
                        contact.email,
                        e,
                    )
                    continue

                if not mhd_contact.full_name:
                    logger.warning(
                        "%s study %s. contact's name is empty",
                        mhd_study.repository_identifier,
                        idx,
                    )
                    continue
                if contact.additional_emails:
                    if mhd_contact.email_list:
                        mhd_contact.email_list.extend(contact.additional_emails)
                    else:
                        mhd_contact.email_list = contact.additional_emails
                if not mhd_contact.email_list:
                    logger.warning(
                        "%s study %s. contact's ('%s') email is empty. Contact will be ignored.",
                        mhd_study.repository_identifier,
                        idx,
                        mhd_contact.full_name,
                    )
                    # continue
                else:
                    contacts[mhd_contact.email_list[0]] = mhd_contact
                mhd_builder.add(mhd_contact)
                affiliation = contact.affiliation or None
                organization = None
                if not affiliation:
                    continue
                if affiliation not in organizations:
                    organization = mhd_domain.Organization(
                        name=affiliation,
                        address=contact.address if contact.address else None,
                    )
                    mhd_builder.add(organization)
                    organizations[affiliation] = organization
                organization = organizations[affiliation]
                mhd_builder.link(
                    mhd_contact,
                    "affiliated-with",
                    organization,
                    reverse_relationship_name="affiliates",
                )
                roles = set()
                for role in contact.roles:
                    if role.term.lower() in roles:
                        continue
                    roles.add(role.term.lower())
                    if role.term.lower() == "principal investigator":
                        mhd_builder.link(
                            mhd_contact,
                            "principal-investigator-of",
                            mhd_study,
                            reverse_relationship_name="has-principal-investigator",
                        )
                    elif role.term.lower() == "submitter":
                        mhd_builder.link(
                            mhd_contact,
                            "submits",
                            mhd_study,
                            reverse_relationship_name="submitted-by",
                        )

                    contributers[contact.email] = mhd_contact
                    mhd_builder.link(
                        mhd_contact,
                        "contributes",
                        mhd_study,
                        reverse_relationship_name="has-contributor",
                    )
            if data.study_db_metadata and data.study_db_metadata.submitters:
                for submitter in data.study_db_metadata.submitters:
                    if submitter.status != UserStatus.ACTIVE:
                        continue

                    if submitter.user_name not in contacts:
                        affiliation = submitter.affiliation or None
                        organization = None
                        if not affiliation:
                            continue
                        if affiliation not in organizations:
                            organization = mhd_domain.Organization(name=affiliation)
                            mhd_builder.add(organization)
                            organizations[affiliation] = organization

                        organization = organizations[affiliation]
                        mhd_contact = mhd_domain.Person(
                            full_name=" ".join(
                                [
                                    x
                                    for x in (
                                        submitter.first_name,
                                        submitter.last_name,
                                    )
                                    if x
                                ]
                            ),
                            email_list=[submitter.user_name]
                            if submitter.user_name
                            else None,
                            address_list=[submitter.address]
                            if submitter.address
                            else None,
                            orcid=submitter.orcid or None,
                        )
                        mhd_builder.add(mhd_contact)
                        if organization:
                            mhd_builder.link(
                                mhd_contact,
                                "affiliated-with",
                                organization,
                                reverse_relationship_name="affiliates",
                            )
                    else:
                        mhd_contact = contacts[submitter.user_name]
                    submitter_roles = [
                        x for x in contact.roles if x.term.lower() == "submitter"
                    ]
                    if not submitter_roles:
                        mhd_builder.link(
                            mhd_contact,
                            "submits",
                            mhd_study,
                            reverse_relationship_name="submitted-by",
                        )

                    if mhd_contact.email_list[0] not in contributers:
                        mhd_builder.link(
                            mhd_contact,
                            "contributes",
                            mhd_study,
                            reverse_relationship_name="has-contributor",
                        )

        return organizations

    def add_funders(
        self,
        data: MetabolightsStudyModel,
        mhd_builder: MhDatasetBuilder,
        mhd_study: mhd_domain.Study,
        organizations: dict[str, mhd_domain.Organization],
        build_type: BuildType = BuildType.FULL,
    ):
        if build_type == build_type.MINIMUM:
            return
        study: Study = data.investigation.studies[0]
        comments = {x.name: x for x in study.comments if x and x.name}
        grant_ids = []
        if comments.get("Funder") and comments["Funder"].value:
            if isinstance(comments["Funder"].value, str):
                funders = comments["Funder"].value.split(";") or []
            else:
                funders = comments["Funder"].value[0].split(";") or []
            for funder in funders:
                organization = organizations.get(funder)
                if not organization:
                    organization = mhd_domain.Organization(name=funder)
                    mhd_builder.add(organization)
                    organizations[funder] = organization
            mhd_builder.link(
                mhd_study,
                "funded-by",
                organization,
                reverse_relationship_name="funds",
            )
        grants = comments.get("Grant Identifier")
        if grants and grants.value:
            if isinstance(grants.value, str):
                identifiers = [
                    x.strip()
                    for x in grants.value.split(";")
                    if x and len(x.strip()) > 1
                ]
            else:
                identifiers = [
                    x.strip()
                    for x in grants.value[0].split(";")
                    if x and len(x.strip()) > 1
                ]
            if identifiers:
                grant_ids.extend(identifiers)
        if grant_ids:
            mhd_study.grant_identifier_list = grant_ids

    def add_publications(
        self,
        data: MetabolightsStudyModel,
        mhd_builder: MhDatasetBuilder,
        mhd_study: mhd_domain.Study,
    ):
        study = data.investigation.studies[0]
        mhd_publications = []
        valid_doi = False
        for publication in study.study_publications.publications:
            doi = None
            if publication.doi:
                match = re.match(r"(.*doi.org/)?(.+)", publication.doi)
                if match:
                    try:
                        doi = DOI(match.groups()[1])
                        valid_doi = True
                    except Exception as e:
                        logger.error(
                            "Error extracting DOI from %s: %s", study.identifier, e
                        )

            if valid_doi:
                title = publication.title or ""
                mhd_publication = mhd_domain.Publication(
                    title=title,
                    doi=doi,
                    pub_med_id=publication.pub_med_id or None,
                    authors=(
                        [
                            x.strip()
                            for x in publication.author_list.split(",")
                            if x.strip()
                        ]
                        if publication.author_list
                        else None
                    ),
                )
                mhd_builder.add(mhd_publication)
                mhd_publications.append(mhd_publication)
                mhd_builder.link(
                    mhd_study,
                    "has-publication",
                    mhd_publication,
                    reverse_relationship_name="describes",
                )

        if not mhd_publications:
            if not study.study_publications.publications:
                no_publication = create_cv_term_object(
                    type_="descriptor",
                    accession="MS:1002853",
                    source="MS",
                    name="Dataset with no associated published manuscript",
                )
                mhd_builder.add(no_publication)
                mhd_builder.link(
                    mhd_study,
                    "defined-as",
                    no_publication,
                    reverse_relationship_name="publication-status-of",
                )
            else:
                pending_publication = create_cv_term_object(
                    type_="descriptor",
                    accession="MS:1002858",
                    source="MS",
                    name="Dataset with its publication pending",
                )
                mhd_builder.add(pending_publication)
                mhd_builder.link(
                    mhd_study,
                    "defined-as",
                    pending_publication,
                    reverse_relationship_name="publication-status-of",
                )

    def add_metadata_files(
        self,
        mhd_builder: MhDatasetBuilder,
        mhd_study: mhd_domain.Study,
        data: MetabolightsStudyModel,
        selected_assays: list[Assay],
        config: Mtbls2MhdConfiguration,
        build_type: BuildType = BuildType.FULL,
    ):
        isa_tab_format = create_cv_term_object(
            type_="descriptor", accession="EDAM:3687", source="EDAM", name="ISA-TAB"
        )
        study_id = ""
        metadata_files = []
        metadata_files_map = {}
        metadata_files.append(data.investigation_file_path)
        if data.investigation.studies:
            study = data.investigation.studies[0]
            study_id = study.identifier
            if study.file_name in data.samples:
                metadata_files.append(study.file_name)

            for assay in selected_assays:
                if assay.file_name in data.assays:
                    metadata_files.append(assay.file_name)
        format_appended = False
        if study_id:
            for file in metadata_files:
                if build_type == BuildType.MINIMUM:
                    meta = mhd_domain.MetadataFile(
                        name=file,
                        url_list=[
                            f"{config.public_http_base_url}/{study_id}/{file}",
                            f"{config.public_ftp_base_url}/{study_id}/{file}",
                        ],
                    )
                else:
                    format_appended = True
                    meta = mhd_domain.MetadataFile(
                        name=file,
                        extension=Path(file).suffix,
                        format_ref=isa_tab_format.id_,
                        url_list=[
                            f"{config.public_http_base_url}/{study_id}/{file}",
                            f"{config.public_ftp_base_url}/{study_id}/{file}",
                        ],
                    )
                mhd_builder.add_node(meta)
                mhd_builder.link(mhd_study, "has-metadata-file", meta)
                mhd_builder.link(meta, "describes", mhd_study)
                metadata_files_map[file] = meta

            if format_appended:
                mhd_builder.add_node(isa_tab_format)

        return metadata_files_map

    def add_result_files(
        self,
        mhd_builder: MhDatasetBuilder,
        mhd_study: mhd_domain.Study,
        data: MetabolightsStudyModel,
        config: Mtbls2MhdConfiguration,
    ):
        result_file_map: dict[str, mhd_domain.ResultFile] = {}
        tsv_format = create_cv_term_object(
            type_="descriptor", accession="EDAM:3475", source="EDAM", name="TSV"
        )
        study_id = mhd_study.repository_identifier

        for idx, file in enumerate(data.metabolite_assignments):
            result_file = mhd_domain.ResultFile(
                name=file,
                extension=Path(file).suffix,
                format_ref=tsv_format.id_,
                url_list=[
                    f"{config.public_ftp_base_url}/{study_id}/{file}",
                    f"{config.public_http_base_url}/{study_id}/{file}",
                ],
            )
            mhd_builder.add_node(result_file)

            if idx == 0:
                mhd_builder.add_node(tsv_format)

            # mhd_study.result_file_refs.append(result_file.id_)
            mhd_builder.link(mhd_study, "has-result-file", result_file)
            mhd_builder.link(result_file, "created-in", mhd_study)
            result_file_map[file] = result_file
        return result_file_map

    def add_study_factor_definitions(
        self,
        mhd_builder: MhDatasetBuilder,
        mhd_study: mhd_domain.Study,
        data: MetabolightsStudyModel,
        build_type: BuildType = BuildType.FULL,
    ):
        if build_type == BuildType.MINIMUM:
            return
        study = data.investigation.studies[0]
        for item in study.study_factors.factors:
            factor_name = item.name.lower()
            if factor_name in MANAGED_STUDY_FACTOR_MAP:
                cv_term = MANAGED_STUDY_FACTOR_MAP[factor_name]
                type_definition = "x-mtbls-factor-type"
                if cv_term.accession in COMMON_STUDY_FACTOR_DEFINITIONS:
                    type_definition = "factor-type"
                factor_type = create_cv_term_object(
                    type_=type_definition,
                    name=cv_term.name,
                    source=cv_term.source,
                    accession=cv_term.accession,
                )
            else:
                if factor_name in MTBLS_STUDY_FACTOR_MAP:
                    cv_term = MTBLS_STUDY_FACTOR_MAP[factor_name]
                else:
                    cv_term = CvTerm(name=factor_name)

                factor_type = create_cv_term_object(
                    type_="x-mtbls-factor-type",
                    name=cv_term.name,
                    source=cv_term.source,
                    accession=cv_term.accession,
                )
            mhd_builder.add(factor_type)

            factor_definition = mhd_domain.FactorDefinition(
                factor_type_ref=factor_type.id_,
                name=item.name,
            )
            mhd_builder.add(factor_definition)
            mhd_builder.link(
                factor_definition,
                "has-type",
                factor_type,
                reverse_relationship_name="type-of",
            )
            mhd_builder.link(
                mhd_study,
                "has-factor-definition",
                factor_definition,
                reverse_relationship_name="used-in",
            )

    def add_assay_protocols(
        self,
        mhd_builder: MhDatasetBuilder,
        mhd_study: mhd_domain.Study,
        data: MetabolightsStudyModel,
        mhd_assay: mhd_domain.Assay,
    ):
        assay_table = data.assays[mhd_assay.name].table

        protocols = {}
        for protocol_key in mhd_study.protocol_refs:
            protocol = mhd_builder.objects[protocol_key]
            protocols[protocol.name] = protocol
            protocol_type = mhd_builder.objects.get(protocol.protocol_type_ref)
            if (
                protocol_type
                and protocol_type.accession
                == "EFO:0005518"  # sample collection protocol
            ):
                if not mhd_assay.protocol_refs:
                    mhd_assay.protocol_refs = []
                mhd_assay.protocol_refs.append(protocol_key)
                mhd_builder.link(
                    mhd_assay, "follows", protocol, reverse_relationship_name="used-in"
                )

        if "Sample Name" not in assay_table.data or not assay_table.data["Sample Name"]:
            return
        for column in assay_table.headers:
            protocol_type = None
            if column.column_header != "Protocol REF":
                continue
            protocol_name = assay_table.data[column.column_name][0]
            if protocol_name in COMMON_PROTOCOLS_MAP:
                protocol_type = COMMON_PROTOCOLS_MAP[protocol_name]
            else:
                protocol_type = CvTerm(
                    source="",
                    accession="",
                    name=protocol_name,
                )

            if protocol_name and protocol_name in protocols:
                ref = protocols[protocol_name].id_
                if not mhd_assay.protocol_refs:
                    mhd_assay.protocol_refs = []
                mhd_assay.protocol_refs.append(ref)
                mhd_builder.link(mhd_assay, "follows", protocols[protocol_name])
                mhd_builder.link(protocols[protocol_name], "used-in", mhd_assay)

    def add_characteristic_definitions(
        self,
        mhd_builder: MhDatasetBuilder,
        mhd_study: mhd_domain.Study,
        sample_file: SamplesFile,
        build_type: BuildType = BuildType.FULL,
    ):
        characteristics_map: dict[str, mhd_domain.CvTermObject] = {}
        for item in sample_file.table.headers:
            if item.column_header.startswith("Characteristics["):
                name = (
                    item.column_header.removeprefix("Characteristics[")
                    .removesuffix("]")
                    .lower()
                )
                if name not in MANAGED_CHARACTERISTICS_MAP and build_type in (
                    BuildType.MINIMUM,
                    BuildType.FULL,
                ):
                    continue

                if name in MANAGED_CHARACTERISTICS_MAP:
                    cv_term = MANAGED_CHARACTERISTICS_MAP[name]
                    characteristic_type = create_cv_term_object(
                        type_="characteristic-type",
                        name=cv_term.name,
                        accession=cv_term.accession,
                        source=cv_term.source,
                    )
                else:
                    if name in MTBLS_CHARACTERISTICS_MAP:
                        cv_term = MTBLS_CHARACTERISTICS_MAP[name]
                    else:
                        cv_term = CvTerm(name=name)
                    characteristic_type = create_cv_term_object(
                        type_="x-mtbls-characteristic-type",
                        name=cv_term.name,
                        accession=cv_term.accession,
                        source=cv_term.source,
                    )
                key = characteristic_type.accession or characteristic_type.name
                if key not in characteristics_map:
                    characteristics_map[key] = characteristic_type

                    mhd_builder.add(characteristic_type)

                characteristic_type = characteristics_map.get(key)
                characteristic = mhd_domain.CharacteristicDefinition(
                    characteristic_type_ref=characteristic_type.id_,
                    name=name,
                )
                mhd_builder.link(
                    characteristic,
                    "has-type",
                    characteristic_type,
                    reverse_relationship_name="type-of",
                )
                mhd_builder.add(characteristic)
                mhd_builder.link(
                    mhd_study,
                    "has-characteristic-definition",
                    characteristic,
                    reverse_relationship_name="used-in",
                )

    def add_samples(
        self,
        mhd_builder: MhDatasetBuilder,
        mhd_study: mhd_domain.Study,
        sample_file: SamplesFile,
        build_type: BuildType = BuildType.FULL,
    ) -> dict[str, mhd_domain.Sample]:
        samples_map: dict[str, mhd_domain.Sample] = {}

        factor_definitions: list[mhd_domain.CvTermObject] = []
        characteristics: list[mhd_domain.CvTermObject] = []

        for idx in mhd_builder.objects:
            item = mhd_builder.objects[idx]
            if isinstance(item, Relationship):
                if item.source_ref == mhd_study.id_:
                    if item.relationship_name == "has-factor-definition":
                        factor_definitions.append(mhd_builder.objects[item.target_ref])
                    elif item.relationship_name == "has-characteristic-definition":
                        characteristics.append(mhd_builder.objects[item.target_ref])

        data = sample_file.table.data
        columns_map = {x.lower(): x for x in sample_file.table.columns}
        factors_map = OrderedDict()
        characteristics_map = OrderedDict()
        parameters_map = OrderedDict()
        for x in sample_file.table.columns:
            match = re.match(r"(.+)\s*\[([^]]+)\](.*)", x)
            if match:
                prefix = match.groups()[0]
                val = match.groups()[1]
                if prefix == "Factor Value":
                    factors_map[val.lower()] = x
                elif prefix == "Characteristics":
                    characteristics_map[val.lower()] = x
                elif prefix == "Parameter Value":
                    parameters_map[val.lower()] = x
            columns_map[x.lower()] = x
        subject_map: dict[str, mhd_domain.Subject] = {}
        sample_map: dict[str, mhd_domain.Sample] = {}

        sample_sources_map: dict[str, set[str]] = {}
        characteristic_values_map: dict[str, dict[str, mhd_domain.CvTermObject]] = {}
        factors_values_map: dict[str, dict[str, mhd_domain.CvTermObject]] = {}
        for idx, name in enumerate(data["Sample Name"]):
            if build_type == BuildType.MINIMUM:
                characteristic_values = self.create_values(
                    mhd_builder,
                    idx,
                    sample_file,
                    characteristics,
                    characteristics_map,
                    characteristic_values_map,
                    MANAGED_CHARACTERISTICS_MAP,
                    "characteristic-value",
                    definition_type_property="characteristic_type_ref",
                )
                continue

            subject_name = data["Source Name"][idx]
            if subject_name not in subject_map:
                subject_map[subject_name] = mhd_domain.Subject(
                    name=subject_name, repository_identifier=subject_name
                )
                mhd_builder.add(subject_map[subject_name])
            subject = subject_map[subject_name]
            if name not in sample_map:
                samples_map[name] = mhd_domain.Sample(
                    name=name, repository_identifier=name
                )
                sample_sources_map[name] = set()
                mhd_builder.add(samples_map[name])
            sample: mhd_domain.Sample = samples_map[name]
            if subject_name not in sample_sources_map[name]:
                sample_sources_map[name].add(subject_name)
                mhd_builder.link(
                    sample,
                    "derived-from",
                    subject,
                    reverse_relationship_name="source-of",
                )

            mhd_builder.link(
                mhd_study,
                "has-sample",
                sample,
                reverse_relationship_name="used-in",
            )

            characteristic_values = self.create_values(
                mhd_builder,
                idx,
                sample_file,
                characteristics,
                characteristics_map,
                characteristic_values_map,
                MANAGED_CHARACTERISTICS_MAP,
                "characteristic-value",
                definition_type_property="characteristic_type_ref",
            )
            if characteristic_values:
                for value in characteristic_values:
                    mhd_builder.link(
                        subject,
                        "has-characteristic-value",
                        value,
                        reverse_relationship_name="value-of",
                    )
                # subject.characteristic_values = characteristic_values

            factor_values = self.create_values(
                mhd_builder,
                idx,
                sample_file,
                factor_definitions,
                factors_map,
                factors_values_map,
                MANAGED_STUDY_FACTOR_MAP,
                "factor-value",
                definition_type_property="factor_type_ref",
            )
            if factor_values:
                for value in factor_values:
                    mhd_builder.link(
                        sample,
                        "has-factor-value",
                        value,
                        reverse_relationship_name="value-of",
                    )
                # sample.factor_values = factor_values

        return samples_map

    def sanitize_string(self, input_string: str) -> str:
        sanitized = re.sub(r"[^0-9a-zA-Z-]", "", input_string)
        return sanitized

    def create_values(
        self,
        mhd_builder: MhDatasetBuilder,
        idx: int,
        sample_file: SamplesFile,
        terms: list[mhd_domain.CvTermObject | mhd_domain.CvTermValueObject],
        column_map: dict[str, str],
        values_map: dict[
            str, dict[str, mhd_domain.CvTermObject | mhd_domain.CvTermValueObject]
        ],
        common_map: dict[
            str, dict[str, mhd_domain.CvTermObject | mhd_domain.CvTermValueObject]
        ],
        name_prefix: str,
        definition_type_property: str,
    ):
        data = sample_file.table.data
        values = []
        columns = sample_file.table.columns
        for term in terms:
            type_key = getattr(term, definition_type_property, None)
            type_obj = mhd_builder.objects.get(type_key, None)
            key = term.name.lower()

            if key not in column_map:
                continue

            column_name = column_map[key]
            name = data[column_name][idx]

            if not name:
                continue
            object_name = name_prefix
            if key.lower() not in common_map:
                object_name = "x-mtbls-" + name_prefix

            values_map[key] = {}
            index = columns.index(column_name)

            if name not in values_map[key]:
                if index + 1 < len(columns) and columns[index + 1].startswith(
                    "Term Source REF"
                ):
                    source = data[columns[index + 1]][idx]
                    accession = (
                        self.convert_to_curie(source, data[columns[index + 2]][idx])
                        if index + 2 < len(columns)
                        else ""
                    )
                    item = create_cv_term_value_object(
                        type_=object_name,
                        source=source,
                        accession=accession,
                        name=name,
                    )

                elif index + 1 < len(columns) and columns[index + 1].startswith("Unit"):
                    unit = (
                        data[columns[index + 1]][idx]
                        if index + 1 < len(columns)
                        else ""
                    )
                    source = (
                        data[columns[index + 2]][idx]
                        if index + 2 < len(columns)
                        else ""
                    )
                    accession = (
                        self.convert_to_curie(source, data[columns[index + 3]][idx])
                        if index + 3 < len(columns)
                        else ""
                    )
                    item = create_cv_term_value_object(
                        type_=object_name,
                        value=name,
                        unit=UnitCvTerm(
                            source=source,
                            accession=accession,
                            name=unit,
                        ),
                    )
                else:
                    item = create_cv_term_object(
                        type_=object_name,
                        name=name,
                    )
                if item:
                    values_map[key][name] = item
                    mhd_builder.add(item)
                    mhd_builder.link(
                        term,
                        "has-instance",
                        item,
                        reverse_relationship_name="instance-of",
                    )
                    if hasattr(term, definition_type_property):
                        if type_obj:
                            mhd_builder.link(
                                type_obj,
                                "type-of",
                                item,
                                reverse_relationship_name="has-type",
                            )

            value = values_map[key][name]
            values.append(value)
        return values

    def add_sample_runs(
        self,
        mhd_builder: MhDatasetBuilder,
        mhd_study: mhd_domain.Study,
        mhd_assay: mhd_domain.Assay,
        assay_file: AssayFile,
        files_map: dict[str, mhd_domain.BaseFile],
        samples: dict[str, mhd_domain.Sample],
        protocol_summaries: OrderedDict[str, ProtocolRunSummary],
    ) -> dict[str, OrderedDict[str, str]]:
        assay_table = assay_file.table
        if "Sample Name" not in assay_table.data or not assay_table.data["Sample Name"]:
            return
        protocol_run_map: dict[str, OrderedDict[str, str]] = {}
        protocols = {}

        parameter_definitions: OrderedDict[str, mhd_domain.CvTermObject] = OrderedDict()

        for x in mhd_builder.objects.values():
            if (
                isinstance(x, Relationship)
                and x.relationship_name == "has-parameter-definition"
            ):
                if x.source_ref in mhd_study.protocol_refs:
                    definition_obj = mhd_builder.objects[x.target_ref]
                    definition = mhd_builder.objects[definition_obj.parameter_type_ref]
                    if definition.accession:
                        parameter_definitions[definition.accession] = definition_obj
                    else:
                        parameter_definitions[definition.name] = definition_obj

        assay_protocols: OrderedDict[str, mhd_domain.Protocol] = OrderedDict()
        for protocol_key in mhd_study.protocol_refs:
            protocol: mhd_domain.Protocol = mhd_builder.objects[protocol_key]
            protocol_type_obj = mhd_builder.objects[protocol.protocol_type_ref]
            protocol_type = CvTerm.model_validate(
                protocol_type_obj.model_dump(by_alias=True)
            )
            protocols[protocol_type] = protocol

        for header in assay_table.headers:
            if header.column_header == "Protocol REF":
                protocol_name = assay_table.data[header.column_name][0]
                protocol_type = MTBLS_PROTOCOLS_MAP.get(protocol_name)
                if protocol_type:
                    if protocol_type in protocols:
                        protocol = protocols[protocol_type]
                        assay_protocols[protocol_name] = protocol
                    else:
                        logger.error(
                            "Assay protocol name '%s' is not consistent in %s %s",
                            protocol_name,
                            mhd_study.mhd_identifier,
                            assay_file.file_path,
                        )
                else:
                    protocol_key = CvTerm(name=protocol_name)
                    protocol = protocols.get(protocol_key)
                    if protocol:
                        assay_protocols[protocol_name] = protocol
                    else:
                        logger.error(
                            "Assay protocol name '%s' is not consistent in %s %s",
                            protocol_name,
                            mhd_study.mhd_identifier,
                            assay_file.file_path,
                        )

        protocol_columns: OrderedDict[str, ProtocolRunSummary] = OrderedDict()
        for header in assay_table.headers:
            if header.column_header == "Protocol REF":
                protocol_name = assay_table.data[header.column_name][0]
                if protocol_name not in assay_protocols:
                    continue
                if header.column_name not in protocol_summaries:
                    protocol_summaries[header.column_name] = ProtocolRunSummary(
                        protocol_name=protocol_name,
                        column=header,
                        protocol=assay_protocols[protocol_name],
                    )
                protocol_columns[header.column_name] = protocol_summaries[
                    header.column_name
                ]

        sample_row_indices: dict[str, int] = {}
        for row_idx, sample_name in enumerate(assay_table.data["Sample Name"]):
            sample_row_indices[sample_name] = row_idx
            protocol_run_map[sample_name] = OrderedDict()
            value = ""
            for column_name, protocol_run_summary in protocol_columns.items():
                header = protocol_run_summary.column
                col_idx = header.column_index
                protocol_name = protocol_run_summary.protocol_name
                current_idx = col_idx
                params: OrderedDict[str, tuple] = OrderedDict()
                while current_idx < len(assay_table.headers) - 1:
                    current_idx += 1
                    if assay_table.headers[current_idx].column_header == "Protocol REF":
                        break
                    if not assay_table.headers[current_idx].column_header.startswith(
                        "Parameter Value["
                    ):
                        continue
                    header = assay_table.headers[current_idx]
                    column_name = header.column_name
                    if header.column_structure == ColumnsStructure.SINGLE_COLUMN:
                        value = (assay_table.data[column_name][row_idx],)
                    elif header.column_structure == ColumnsStructure.ONTOLOGY_COLUMN:
                        source = assay_table.columns[header.column_index + 1]
                        accession = assay_table.columns[header.column_index + 2]
                        parameter_value = assay_table.data[column_name][row_idx]
                        source_value = assay_table.data[source][row_idx]
                        accession_value = assay_table.data[accession][row_idx]
                        value = (parameter_value, source_value, accession_value)
                        current_idx += 2
                    elif (
                        header.column_structure
                        == ColumnsStructure.SINGLE_COLUMN_AND_UNIT_ONTOLOGY
                    ):
                        unit = assay_table.columns[header.column_index + 1]
                        source = assay_table.columns[header.column_index + 2]
                        accession = assay_table.columns[header.column_index + 3]
                        parameter_value = assay_table.data[column_name][row_idx]
                        unit_value = assay_table.data[unit][row_idx]
                        source_value = assay_table.data[source][row_idx]
                        accession_value = assay_table.data[accession][row_idx]
                        current_idx += 3

                        value = (
                            parameter_value,
                            unit_value,
                            source_value,
                            accession_value,
                        )
                    params[column_name] = value
                if not "".join(["".join(x) for x in params.values()]):
                    continue
                key = ",".join([":".join(x) for x in params.values()])
                if key not in protocol_run_summary.sample_run_configurations:
                    parameter_values = []

                    for name, values in params.items():
                        parameter = name.split("]")[0].split("[")[-1]
                        cv = self.get_parameter_cv(protocol_name, parameter)
                        definition = None
                        if not cv:
                            definition = parameter_definitions.get(parameter.lower())
                        else:
                            definition = parameter_definitions.get(cv.accession)
                            # definition = parameter_definitions[cv.accession]
                        if not definition:
                            continue
                        definition_value = "".join([x for x in values if x])
                        if not definition_value:
                            continue
                        object_name = "x-mtbls-parameter-value"
                        # term_name = (
                        #     definition.name.lower().replace("   ", " ").replace(" ", "-")
                        # )
                        if cv and cv.accession in COMMON_PARAMETER_DEFINITIONS:
                            object_name = "parameter-value"

                        item = None
                        if len(values) == 1:
                            item = create_cv_term_value_object(
                                type_=object_name, name=values[0]
                            )
                            # definition_value = DefinitionValue(
                            #     key=definition_ref, value=values[0]
                            # )
                        elif len(values) == 3:
                            accession = self.convert_to_curie(values[1], values[2])
                            item = create_cv_term_value_object(
                                type_=object_name,
                                name=values[0],
                                source=values[1],
                                accession=accession,
                            )
                        elif len(values) == 4:
                            accession = self.convert_to_curie(values[2], values[3])
                            item = create_cv_term_value_object(
                                type_=object_name,
                                value=values[0],
                                unit=UnitCvTerm(
                                    name=values[1],
                                    source=values[2],
                                    accession=accession,
                                ),
                            )

                        if item:
                            parameter_values.append(item)
                            mhd_builder.add(item)
                            mhd_builder.link(
                                definition,
                                "has-instance",
                                item,
                                reverse_relationship_name="instance-of",
                            )
                            if hasattr(definition, "parameter_type_ref"):
                                val = getattr(definition, "parameter_type_ref")
                                node_type = mhd_builder.objects.get(val)
                                if node_type:
                                    mhd_builder.link(
                                        node_type,
                                        "type-of",
                                        item,
                                        reverse_relationship_name="has-type",
                                    )
                    if parameter_values:
                        config = mhd_domain.SampleRunConfiguration(
                            protocol_ref=protocol_run_summary.protocol.id_,
                            parameter_value_refs=[x.id_ for x in parameter_values],
                        )
                        protocol_run_summary.sample_run_configurations[key] = config
                    else:
                        protocol_run_summary.sample_run_configurations[key] = None
                if key in protocol_run_summary.sample_run_configurations:
                    config = protocol_run_summary.sample_run_configurations[key]
                    if config:
                        protocol_run_map[sample_name][protocol_name] = config.id_

        for protocol_run_summary in protocol_columns.values():
            for item in protocol_run_summary.sample_run_configurations.values():
                if item:
                    mhd_builder.add(item)
        file_colums = []
        for header in assay_table.headers:
            if header.column_header.endswith(" Data File"):
                file_colums.append(header.column_name)

        for sample_name, protocols in protocol_run_map.items():
            # if not sample:
            idx = sample_row_indices[sample_name]
            result_file_refs = []
            if "Metabolite Assignment File" in assay_table.data:
                filename = assay_table.data["Metabolite Assignment File"][idx]
                if filename and filename in files_map:
                    result_file_refs = [files_map[filename].id_]

            data_files = []

            for column in file_colums:
                filename = assay_table.data[column][idx]
                if filename and filename in files_map:
                    data_files.append(files_map[filename])

            raw_file_refs = [
                x.id_ for x in data_files if isinstance(x, mhd_domain.RawDataFile)
            ]
            derived_file_refs = [
                x.id_ for x in data_files if isinstance(x, mhd_domain.DerivedDataFile)
            ]
            if "MS Assay Name" in assay_table.data:
                name = assay_table.data["MS Assay Name"][idx]
            if not name and "NMR Assay Name" in assay_table.data:
                name = assay_table.data["NMR Assay Name"][idx]
            else:
                name = sample_name

            sample_run_configuration_refs = list(protocols.values())

            sample = samples.get(sample_name)
            mhd_sample_run = mhd_domain.SampleRun(
                name=name,
                sample_ref=sample.id_ if sample else None,
                sample_run_configuration_refs=(
                    sample_run_configuration_refs
                    if sample_run_configuration_refs
                    else None
                ),
                raw_data_file_refs=raw_file_refs if raw_file_refs else None,
                derived_file_refs=derived_file_refs if derived_file_refs else None,
                result_file_refs=result_file_refs if result_file_refs else None,
            )
            mhd_builder.add(mhd_sample_run)
            if not mhd_assay.sample_run_refs:
                mhd_assay.sample_run_refs = []
            mhd_assay.sample_run_refs.append(mhd_sample_run.id_)

    def get_parameter_cv(
        self, protocol_name: str, parameter_name: str
    ) -> CvTerm | None:
        if protocol_name in MTBLS_PROTOCOL_PARAMETER_DEFINITION_MAP:
            params = MTBLS_PROTOCOL_PARAMETER_DEFINITION_MAP[protocol_name]
            if parameter_name in params and params[parameter_name]:
                return params[parameter_name]
        return None

    def add_protocols(
        self, mhd_builder: MhDatasetBuilder, mhd_study: mhd_domain.Study, study: Study
    ) -> dict[str, mhd_domain.Protocol]:
        parameters_map: dict[str, mhd_domain.CvTermObject] = {}
        protocols: dict[str, mhd_domain.Protocol] = {}
        for protocol in study.study_protocols.protocols:
            name = protocol.name

            mhd_protocol = None
            parameters: list[mhd_domain.CvTermObject] = []
            for x in protocol.parameters:
                if x.term:
                    param_cv = self.get_parameter_cv(protocol.name, x.term)
                    definition_type = "x-mtbls-parameter-type"
                    if not param_cv:
                        definition_type = create_cv_term_object(
                            type_=definition_type,
                            name=x.term.lower(),
                            accession=x.term_accession_number or "",
                            source=x.term_source_ref or "",
                        )
                    else:
                        if param_cv.accession in COMMON_PARAMETER_DEFINITIONS:
                            definition_type = "parameter-type"
                        definition_type = create_cv_term_object(
                            type_=definition_type,
                            name=param_cv.name,
                            accession=param_cv.accession,
                            source=param_cv.source,
                        )
                    key = definition_type.accession or definition_type.name
                    if key not in parameters_map:
                        parameters_map[key] = definition_type
                        mhd_builder.add(definition_type)
                        mhd_builder.link(
                            mhd_study,
                            "defines",
                            definition_type,
                            reverse_relationship_name="defined-in",
                        )
                    definition_type = parameters_map.get(key)
                    definition = mhd_domain.ParameterDefinition(
                        parameter_type_ref=definition_type.id_, name=x.term
                    )
                    mhd_builder.link(
                        definition,
                        "has-type",
                        definition_type,
                        reverse_relationship_name="type-of",
                    )
                    mhd_builder.add(definition)
                    parameters.append(definition)

            if name in COMMON_PROTOCOLS_MAP:
                protocol_type = COMMON_PROTOCOLS_MAP[name]
            else:
                if name in MTBLS_PROTOCOLS_MAP:
                    protocol_type = MTBLS_PROTOCOLS_MAP[name]
                else:
                    protocol_type = CvTerm(name=protocol.name)

            definition_refs = None
            if parameters:
                definition_refs = [x.id_ for x in parameters]
            protocol_type_obj = create_cv_term_object(
                type_="protocol-type",
                source=protocol_type.source or "",
                accession=self.convert_to_curie(
                    protocol_type.source,
                    protocol_type.accession,
                )
                or "",
                name=protocol_type.name or "",
            )
            mhd_builder.add(protocol_type_obj)
            mhd_protocol = mhd_domain.Protocol(
                name=name,
                protocol_type_ref=protocol_type_obj.id_,
                description=protocol.description,
                parameter_definition_refs=definition_refs,
            )
            mhd_builder.link(
                mhd_protocol,
                "has-type",
                protocol_type_obj,
                reverse_relationship_name="type-of",
            )
            for param in parameters:
                mhd_builder.link(
                    mhd_protocol,
                    "has-parameter-definition",
                    param,
                    reverse_relationship_name="used-in",
                )
            protocols[name] = mhd_protocol
            if mhd_study.protocol_refs is None:
                mhd_study.protocol_refs = []
            mhd_builder.add(mhd_protocol)
            mhd_study.protocol_refs.append(mhd_protocol.id_)
            mhd_builder.link(
                mhd_study,
                "has-protocol",
                mhd_protocol,
                reverse_relationship_name="used-in",
            )
        return protocols

    def add_keywords(
        self,
        mhd_builder: MhDatasetBuilder,
        mhd_study: mhd_domain.Study,
        study: Study,
    ):
        for item in study.study_design_descriptors.design_types:
            keyword = create_cv_term_object(
                type_="descriptor",
                source=item.term_source_ref or "",
                accession=self.convert_to_curie(
                    item.term_source_ref,
                    item.term_accession_number,
                )
                or "",
                name=item.term or "",
            )
            mhd_builder.add_node(keyword)

            if not item.source or item.source.lower() in ("submitter",):
                mhd_builder.link(
                    mhd_study,
                    "has-submitter-keyword",
                    keyword,
                    reverse_relationship_name="keyword-of",
                )
            else:
                mhd_builder.link(
                    mhd_study,
                    "has-repository-keyword",
                    keyword,
                    reverse_relationship_name="keyword-of",
                )

    def add_assay_keywords(
        self,
        mhd_builder: MhDatasetBuilder,
        assays: dict[str, mhd_domain.Assay],
        study: Study,
    ):
        for assay in study.study_assays.assays:
            mhd_assay = assays.get(assay.file_name)
            if not mhd_assay:
                continue
            for item in assay.assay_descriptors:
                keyword = create_cv_term_object(
                    type_="descriptor",
                    source=item.term_source_ref or "",
                    accession=self.convert_to_curie(
                        item.term_source_ref,
                        item.term_accession_number,
                    )
                    or "",
                    name=item.term or "",
                )
                mhd_builder.add_node(keyword)

                if not item.source or item.source.lower() in ("submitter",):
                    mhd_builder.link(
                        mhd_assay,
                        "has-submitter-keyword",
                        keyword,
                        reverse_relationship_name="keyword-of",
                    )
                else:
                    mhd_builder.link(
                        mhd_assay,
                        "has-repository-keyword",
                        keyword,
                        reverse_relationship_name="keyword-of",
                    )

    def find_file_format(
        self,
        file: str,
        data: MetabolightsStudyModel,
        default_format: mhd_domain.CvTermObject,
        cv_nodes: dict[str, mhd_domain.CvTermObject],
    ) -> tuple[mhd_domain.CvTermObject, mhd_domain.CvTermObject, str]:
        file_path = Path(file)
        suffix = file_path.suffix
        compressed = False

        if file_path.suffix.lower() == ".zip":
            file_path = Path(str(file_path).replace(file_path.suffix, ""))
            suffix = file_path.suffix
            compressed = True
        zip_file_format_node = None
        file_extension = file_path.suffix
        if compressed:
            file_extension += ".zip"

            zip_file_format_node = create_cv_term_object(
                type_="descriptor",
                source="EDAM",
                accession="EDAM:3987",
                name="ZIP Format",
            )
        folder = False
        if file in data.study_folder_metadata.files:
            folder = data.study_folder_metadata.files[file].is_directory
        elif file in data.study_folder_metadata.folders:
            folder = data.study_folder_metadata.folders[file].is_directory
        # else:
        #     return None, None, None

        data_format = FILE_EXTENSIONS.get((suffix.lower(), folder))
        if not data_format:
            data_format = default_format
        data_format_node = None
        if data_format is None or cv_nodes.get(data_format.accession) is None:
            data_format_node = create_cv_term_object(
                type_="descriptor",
                accession=data_format.accession,
                source=data_format.source,
                name=data_format.name,
            )
            cv_nodes[data_format_node.accession] = data_format_node

        data_format_node = cv_nodes[data_format.accession]
        return zip_file_format_node, data_format_node, file_extension

    def add_data_files(
        self,
        mhd_builder: MhDatasetBuilder,
        mhd_study: mhd_domain.Study,
        data: MetabolightsStudyModel,
        metadata_files: dict[str, mhd_domain.CvTermObject],
        result_files: dict[str, mhd_domain.CvTermObject],
        config: Mtbls2MhdConfiguration,
    ):
        study_id = mhd_study.repository_identifier

        result_file_format = create_cv_term_object(
            type_="descriptor",
            source="EDAM",
            accession="EDAM:3475",
            name="TSV",
        )

        mhd_builder.add(result_file_format)

        files_map: dict[str, mhd_domain.CvTermObject] = {}
        cv_nodes: dict[str, mhd_domain.CvTermObject] = {}
        for assay in data.assays.values():
            for file in assay.referenced_raw_files:
                if file in files_map:
                    continue
                compression_format, data_format, file_extension = self.find_file_format(
                    file=file,
                    data=data,
                    default_format=None,
                    cv_nodes=cv_nodes,
                )
                if not data_format:
                    logger.warning(
                        "File format is not found in folder metadata for study %s: %s",
                        study_id,
                        file,
                    )
                if data_format:
                    mhd_builder.add(data_format)
                if compression_format:
                    mhd_builder.add(compression_format)
                referenced_assay = metadata_files.get(assay.file_path)
                file_node = mhd_domain.RawDataFile(
                    name=file,
                    metadata_file_refs=[referenced_assay.id_]
                    if referenced_assay
                    else None,
                    compression_format_ref=(
                        compression_format.id_ if compression_format else None
                    ),
                    format_ref=data_format.id_ if data_format else None,
                    extension=file_extension if file_extension else None,
                    url_list=[f"{config.public_ftp_base_url}/{study_id}/{file}"],
                )
                files_map[file] = file_node
                mhd_builder.add(file_node)
                mhd_builder.link(
                    mhd_study,
                    "has-raw-data-file",
                    file_node,
                    reverse_relationship_name="created-in",
                )
            for file in assay.referenced_derived_files:
                if file in files_map:
                    continue

                compression_format, data_format, file_extension = self.find_file_format(
                    file=file,
                    data=data,
                    default_format=None,
                    cv_nodes=cv_nodes,
                )
                if not data_format:
                    logger.warning(
                        "File format is not found in folder metadata for study %s: %s",
                        study_id,
                        file,
                    )
                if data_format:
                    mhd_builder.add(data_format)
                if compression_format:
                    mhd_builder.add(compression_format)
                referenced_assay = metadata_files.get(assay.file_path)
                file_node = mhd_domain.DerivedDataFile(
                    name=file,
                    metadata_file_refs=[referenced_assay.id_]
                    if referenced_assay
                    else None,
                    compression_format_ref=(
                        compression_format.id_ if compression_format else None
                    ),
                    format_ref=data_format.id_ if data_format else None,
                    extension=file_extension if file_extension else None,
                    url_list=[f"{config.public_ftp_base_url}/{study_id}/{file}"],
                )
                files_map[file] = file_node
                mhd_builder.add(file_node)
                mhd_builder.link(
                    mhd_study,
                    "has-derived-data-file",
                    file_node,
                    reverse_relationship_name="created-in",
                )
        for file in data.study_folder_metadata.files:
            if (
                file not in files_map
                and file not in metadata_files
                and file not in result_files
            ):
                compression_format, data_format, file_extension = self.find_file_format(
                    file=file,
                    data=data,
                    default_format=None,
                    cv_nodes=cv_nodes,
                )
                if not data_format:
                    mhd_builder.add(data_format)
                if compression_format:
                    mhd_builder.add(compression_format)

                file_node = mhd_domain.SupplementaryFile(
                    name=file,
                    compression_format_ref=(
                        compression_format.id_ if compression_format else None
                    ),
                    format_ref=data_format.id_ if data_format else None,
                    extension=file_extension if file_extension else None,
                    url_list=[f"{config.public_ftp_base_url}/{study_id}/{file}"],
                )
                files_map[file] = file_node
                mhd_builder.add(file_node)
                mhd_builder.link(
                    mhd_study,
                    "has-supplementary-file",
                    file_node,
                    reverse_relationship_name="created-in",
                )

        return files_map

    def add_reported_metabolites(
        self,
        mhd_builder: MhDatasetBuilder,
        mhd_study: mhd_domain.Study,
        data: MetabolightsStudyModel,
        result_files: dict[str, mhd_domain.ResultFile],
    ):
        for maf_filename, maf_file in data.metabolite_assignments.items():
            if not maf_file.table.data.get("metabolite_identification"):
                continue
            result_file = result_files.get(maf_filename)
            for idx, name in enumerate(
                maf_file.table.data["metabolite_identification"]
            ):
                if not name or not name.strip():
                    continue
                met = mhd_domain.Metabolite(name=name)
                assignments = {}
                data: dict[str, str] = maf_file.table.data
                submitted_identifiers = []
                assigned_chebi_identifiers = []
                assigned_refmet_identifiers = []
                if maf_file.table.data.get("database_identifier"):
                    submitted_identifiers = [
                        x.strip()
                        for x in data["database_identifier"][idx].split("|")
                        if x
                    ]
                if maf_file.table.data.get("assigned_chebi_identifier"):
                    assigned_chebi_identifiers = [
                        x.strip()
                        for x in data["assigned_chebi_identifier"][idx].split("|")
                        if x
                    ]
                if maf_file.table.data.get("assigned_refmet_identifier"):
                    assigned_refmet_identifiers = [
                        x.strip()
                        for x in data["assigned_refmet_identifier"][idx].split("|")
                        if x
                    ]

                for identifiers in [
                    (submitted_identifiers, ""),
                    (assigned_chebi_identifiers, "CHEBI"),
                    (assigned_refmet_identifiers, "REFMET"),
                ]:
                    for identifiers, compound_source in assignments:
                        if not identifiers:
                            continue
                        for identifier_value in identifiers:
                            identifier = None
                            if (
                                compound_source == "CHEBI"
                                or identifier_value.upper().startswith("CHEBI")
                            ):
                                identifier = create_cv_term_value_object(
                                    type_="metabolite-identifier",
                                    source="CHEMINF",
                                    accession="CHEMINF:000407",
                                    name="ChEBI identifier",
                                    value=identifier_value,
                                )
                            elif identifier_value.upper().startswith("HMDB"):
                                identifier = create_cv_term_value_object(
                                    type_="metabolite-identifier",
                                    source="CHEMINF",
                                    accession="CHEMINF:000408",
                                    name="HMDB identifier",
                                    value=identifier_value,
                                )
                            elif compound_source == "REFMET":
                                identifier = create_cv_term_value_object(
                                    type_="metabolite-identifier",
                                    source="REFMET",
                                    accession="",
                                    name="RefMet identifier",
                                    value=identifier_value,
                                )

                            if identifier:
                                mhd_builder.add(identifier)
                                mhd_builder.link(
                                    met,
                                    "identified-as",
                                    identifier,
                                    reverse_relationship_name="reported-identifier-of",
                                )
                mhd_builder.add(met)
                if result_file:
                    mhd_builder.link(
                        result_file,
                        "reports",
                        met,
                        reverse_relationship_name="reported-in",
                    )
                result_file
                mhd_builder.link(
                    mhd_study,
                    "reports",
                    met,
                    reverse_relationship_name="reported-in",
                )

    def add_assays(
        self,
        mhd_builder: MhDatasetBuilder,
        mhd_study: mhd_domain.Study,
        data: MetabolightsStudyModel,
        selected_assays: list[Assay],
        metadata_files: dict[str, mhd_domain.CvTermObject],
        samples: dict[str, mhd_domain.Sample],
        files_map,
    ) -> dict[str, mhd_domain.Assay]:
        protocol_summaries: OrderedDict[str, ProtocolRunSummary] = OrderedDict()
        assays: OrderedDict[str, mhd_domain.Assay] = OrderedDict()
        for assay in selected_assays:
            if assay.file_name not in data.assays:
                continue
            assay_node = None
            if assay.file_name in metadata_files:
                assay_node = metadata_files[assay.file_name]
            mhd_assay = mhd_domain.Assay(
                name=assay.file_name,
                repository_identifier=assay.file_name,
                metadata_file_ref=assay_node.id_ if assay_node else None,
            )

            mhd_builder.add(mhd_assay)
            assays[assay.file_name] = mhd_assay
            mhd_builder.link(
                mhd_study, "has-assay", mhd_assay, reverse_relationship_name="part-of"
            )
            technology_type_accession = self.convert_to_curie(
                assay.technology_type.term_source_ref,
                assay.technology_type.term_accession_number,
            )
            if technology_type_accession in COMMON_TECHNOLOGY_TYPES:
                technology_type_cv = COMMON_TECHNOLOGY_TYPES[technology_type_accession]
                technology_type = create_cv_term_object(
                    type_="descriptor",
                    source=technology_type_cv.source,
                    accession=technology_type_cv.accession,
                    name=technology_type_cv.name,
                )
            else:
                technology_type = create_cv_term_object(
                    type_="descriptor",
                    source=assay.technology_type.term_source_ref,
                    accession=self.convert_to_curie(
                        assay.technology_type.term_source_ref,
                        assay.technology_type.term_accession_number,
                    ),
                    name=assay.technology_type.term,
                )

            mhd_builder.add(technology_type)
            mhd_assay.technology_type_ref = technology_type.id_
            assay_file = data.assays[assay.file_name]
            assay_type_cv = None
            if assay_file.assay_technique.name in MTBLS_ASSAY_TYPES:
                assay_type_cv = MTBLS_ASSAY_TYPES[assay_file.assay_technique.name]
            if assay_type_cv:
                assay_type = create_cv_term_object(
                    type_="descriptor",
                    source=assay_type_cv.source,
                    accession=assay_type_cv.accession,
                    name=assay_type_cv.name,
                )
                mhd_builder.add(assay_type)
                mhd_assay.assay_type_ref = assay_type.id_
            omics_types: list[mhd_domain.CvTermObject] = []
            measurement_types: list[mhd_domain.CvTermObject] = []
            measurement = None
            if "untargeted" in assay.measurement_type.term.lower():
                measurement = MTBLS_MEASUREMENT_TYPES["untargeted"]
            elif "targeted" in assay.measurement_type.term.lower():
                measurement = MTBLS_MEASUREMENT_TYPES["targeted"]

            inv_study = data.investigation.studies[0]
            design_types = inv_study.study_design_descriptors.design_types

            for descriptor in design_types:
                if not measurement:
                    if "untargeted" in descriptor.term.lower():
                        measurement = MTBLS_MEASUREMENT_TYPES["untargeted"]
                    elif "targeted" in descriptor.term.lower():
                        measurement = MTBLS_MEASUREMENT_TYPES["targeted"]

                    if measurement:
                        measurement_type = create_cv_term_object(
                            type_="descriptor",
                            source=measurement.source,
                            accession=measurement.accession,
                            name=measurement.name,
                        )
                        measurement_types.append(measurement_type)
                for v in COMMON_OMICS_TYPES.values():
                    if descriptor.term.lower() == v.name.lower():
                        omics_type = create_cv_term_object(
                            type_="descriptor",
                            source=v.source,
                            accession=v.accession,
                            name=v.name,
                        )
                        omics_types.append(omics_type)

            if len(measurement_types) == 1:
                measurement_type = measurement_types[0]
            else:
                default_type = DEFAULT_MEASUREMENT_TYPE
                measurement_type = create_cv_term_object(
                    type_="descriptor",
                    source=default_type.source,
                    accession=default_type.accession,
                    name=default_type.name,
                )
            mhd_builder.add(measurement_type)
            mhd_assay.measurement_type_ref = measurement_type.id_

            if len(omics_types) == 1:
                omics_type = omics_types[0]
            else:
                default_type = DEFAULT_OMICS_TYPE
                omics_type = create_cv_term_object(
                    type_="descriptor",
                    source=default_type.source,
                    accession=default_type.accession,
                    name=default_type.name,
                )
            mhd_builder.add(omics_type)
            mhd_assay.omics_type_ref = omics_type.id_

            self.add_sample_runs(
                mhd_builder,
                mhd_study,
                mhd_assay,
                assay_file,
                files_map,
                samples,
                protocol_summaries,
            )
        for _, mhd_assay in assays.items():
            self.add_assay_protocols(mhd_builder, mhd_study, data, mhd_assay)
        return assays

    def build(
        self,
        mhd_id: None | str,
        mhd_output_folder_path: Path,
        mtbls_study_id: str,
        mtbls_study_path: Path,
        mtbls_study_repository_url: str,
        target_mhd_model_schema_uri: str,
        target_mhd_model_profile_uri: str,
        config: Mtbls2MhdConfiguration,
        repository_name: str,
        cached_mtbls_model_file_path: None | Path = None,
        revision: None | Revision = None,
        build_type: BuildType = BuildType.FULL,
        **kwargs,
    ) -> MhDatasetLegacyProfile:
        mhd_output_filename = kwargs.get("mhd_output_filename", None)
        dataset_provider = create_cv_term_value_object(
            type_="data-provider",
            source="NCIT",
            accession="NCIT:C189151",
            name="Study Data Repository",
            value=repository_name,
        )

        if (
            not cached_mtbls_model_file_path
            or not cached_mtbls_model_file_path.exists()
        ):
            connection = create_postgresql_connection(config)
            db_collector = DbMetadataCollector(config)
            provider = MetabolightsStudyProvider(
                db_metadata_collector=db_collector,
                folder_metadata_collector=LocalFolderMetadataCollector(),
            )
            data: MetabolightsStudyModel = provider.load_study(
                mtbls_study_id,
                study_path=str(mtbls_study_path),
                load_assay_files=True,
                load_sample_file=True,
                load_maf_files=True,
                load_folder_metadata=True,
                connection=connection,
            )
            if cached_mtbls_model_file_path:
                with cached_mtbls_model_file_path.open("w") as fr:
                    json.dump(data.model_dump(by_alias=True), fr)
        else:
            with cached_mtbls_model_file_path.open("r") as fr:
                data: MetabolightsStudyModel = MetabolightsStudyModel.model_validate(
                    json.load(fr)
                )
        if not data.investigation.studies:
            error = f"{data.investigation_file_path} file does not have any study. Skipping..."
            logger.warning(error)
            return False, error
        db_metadata = data.study_db_metadata
        if not revision:
            revision_date = (
                datetime.datetime.strptime(db_metadata.revision_date, "%Y-%m-%d")
                if db_metadata and db_metadata.revision_date
                else None
            )
            if revision_date:
                revision = Revision(
                    revision_datetime=revision_date,
                    revision=db_metadata.revision_number
                    if db_metadata.revision_number
                    else 0,
                    comment=db_metadata.revision_comment
                    if db_metadata.revision_comment
                    else "",
                )
        selected_assays: list[Assay] = []
        study = data.investigation.studies[0]

        if data.study_db_metadata.study_id != study.identifier:
            logger.warning(
                "Study id is not valid in metadata file: Actual: %s, Expected: %s",
                study.identifier,
                data.study_db_metadata.study_id,
            )
        unsupported_assays = []
        for assay in study.study_assays.assays:
            if assay.file_name in data.assays:
                assay_file = data.assays[assay.file_name]
                if assay_file.assay_technique.name in MTBLS_ASSAY_TYPES:
                    selected_assays.append(assay)
                else:
                    unsupported_assays.append(
                        (assay.file_name, assay_file.assay_technique.name)
                    )
        # Check if all assays are supported
        if not selected_assays:
            error = f"Study {study.identifier} does not have any assay in scope."
            logger.error(error)
            return False, error
        if unsupported_assays:
            error = f"Study {study.identifier} has unsupported assays: {str(unsupported_assays)}"
            logger.error(error)
            return False, error
        # TODO get revision, dataset_licence from study
        mhd_builder = MhDatasetBuilder(
            repository_name=repository_name,
            mhd_identifier=None,
            repository_identifier=study.identifier,
            schema_name=target_mhd_model_schema_uri,
            profile_uri=target_mhd_model_profile_uri,
            repository_revision=revision.revision
            if revision and revision and revision.revision
            else 0,
            repository_revision_datetime=revision.revision_datetime
            if revision and revision.revision_datetime
            else None,
            repository_revision_comment=revision.comment
            if revision and revision.comment
            else None,
            change_log=[revision] if revision else None,
        )

        if data.study_db_metadata.submission_date != study.submission_date:
            logger.warning(
                "Submission date is not valid in metadata file: Actual: %s, Expected: %s",
                study.submission_date,
                data.study_db_metadata.submission_date,
            )
        if data.study_db_metadata.release_date != study.public_release_date:
            logger.warning(
                "Public release date is not valid in metadata file: Actual: %s, Expected: %s",
                study.public_release_date,
                data.study_db_metadata.release_date,
            )
        # actual or estimated
        submission_date_str = None
        public_release_date_str = None
        if db_metadata:
            if db_metadata.first_private_date:
                submission_date_str = db_metadata.first_private_date
            elif db_metadata.submission_date:
                submission_date_str = db_metadata.submission_date
            if db_metadata.first_public_date:
                public_release_date_str = db_metadata.first_public_date
            elif db_metadata.release_date:
                public_release_date_str = db_metadata.release_date

        public_release_date = (
            datetime.datetime.strptime(public_release_date_str, "%Y-%m-%d")
            if public_release_date_str
            else None
        )
        submission_date = (
            datetime.datetime.strptime(submission_date_str, "%Y-%m-%d")
            if submission_date_str
            else None
        )
        mhd_study = mhd_domain.Study(
            repository_identifier=study.identifier,
            created_by_ref=dataset_provider.id_,
            mhd_identifier=mhd_id,
            title=study.title,
            description=study.description,
            submission_date=submission_date,
            public_release_date=public_release_date,
            dataset_url_list=[mtbls_study_repository_url],
        )

        mhd_builder.add(mhd_study)
        mhd_builder.add_node(dataset_provider)
        mhd_builder.link(
            dataset_provider,
            "provides",
            mhd_study,
            reverse_relationship_name="provided-by",
        )

        organizations = self.add_contacts(data, mhd_builder, mhd_study, build_type)
        self.add_funders(data, mhd_builder, mhd_study, organizations, build_type)

        metadata_files = self.add_metadata_files(
            mhd_builder,
            mhd_study,
            data,
            selected_assays=selected_assays,
            config=config,
            build_type=build_type,
        )
        sample_file = data.samples[study.file_name]
        self.add_characteristic_definitions(
            mhd_builder, mhd_study, sample_file, build_type
        )

        self.add_study_factor_definitions(mhd_builder, mhd_study, data, build_type)
        samples = self.add_samples(mhd_builder, mhd_study, sample_file, build_type)
        if build_type in (BuildType.FULL, BuildType.FULL_AND_CUSTOM_NODES):
            mhd_study.license = data.study_db_metadata.dataset_license_url
            if not mhd_study.license:
                mhd_study.license = HttpUrl(config.default_dataset_licence_url) or ""

            self.add_publications(data, mhd_builder, mhd_study)
            self.add_protocols(mhd_builder, mhd_study, study)

            result_files = self.add_result_files(
                mhd_builder, mhd_study, data, config=config
            )
            self.add_reported_metabolites(mhd_builder, mhd_study, data, result_files)

            files_map = self.add_data_files(
                mhd_builder,
                mhd_study,
                data,
                metadata_files,
                result_files,
                config=config,
            )
            mhd_assays = self.add_assays(
                mhd_builder,
                mhd_study,
                data,
                selected_assays,
                metadata_files,
                samples,
                files_map,
            )
            self.add_keywords(mhd_builder, mhd_study, study)
            self.add_assay_keywords(mhd_builder, mhd_assays, study)

        mhd_dataset: MhDatasetBaseProfile = mhd_builder.create_dataset(
            start_item_refs=[mhd_study.id_], dataset_class=MhDatasetLegacyProfile
        )
        # mhd_dataset.created_by_ref = dataset_provider.id_
        filename = mhd_id if mhd_id else study.identifier
        mhd_dataset.name = f"{filename} MetabolomicsHub Legacy Dataset"
        mhd_output_folder_path.mkdir(parents=True, exist_ok=True)
        output_path = mhd_output_folder_path / Path(f"{filename}.mhd.json")
        if mhd_output_filename:
            output_path = Path(mhd_output_folder_path) / Path(mhd_output_filename)
        output_path.open("w").write(
            mhd_dataset.model_dump_json(
                indent=2, by_alias=True, exclude_none=True, serialize_as_any=True
            )
        )
        logger.info(
            "%s study MHD file is created with name: %s", mtbls_study_id, output_path
        )
        return True, str(output_path)
