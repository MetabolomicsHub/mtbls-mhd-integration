from __future__ import annotations

import datetime
import hashlib
import json
import logging
import re
from pathlib import Path
from typing import OrderedDict

from bs4 import BeautifulSoup
from metabolights_utils.models.isa import investigation_file
from metabolights_utils.models.isa.assay_file import AssayFile
from metabolights_utils.models.isa.common import IsaTableColumn, IsaTableFile
from metabolights_utils.models.isa.enums import ColumnsStructure
from metabolights_utils.models.isa.samples_file import SamplesFile
from metabolights_utils.models.metabolights.model import (
    MetabolightsStudyModel,
)
from metabolights_utils.provider.study_provider import (
    MetabolightsStudyProvider,
)
from mhd_model.domain_utils import get_urn as default_get_urn
from mhd_model.model.v1_0.dataset.profiles.base import graph_nodes as mhd_domain
from mhd_model.model.v1_0.dataset.profiles.base.profile import (
    MhDatasetBaseProfile_v1_0,
)
from mhd_model.model.v1_0.dataset.profiles.base.relationships import Relationship
from mhd_model.model.v1_0.rules.managed_cv_terms import (
    COMMON_MISSING_DATA_TERMS,
    COMMON_MOLECULAR_ENTITY_IDENTIFIERS,
    COMMON_OMICS_TYPES,
    COMMON_STUDY_FACTOR_DEFINITIONS,
    MISSING_PUBLICATION_REASON,
)
from mhd_model.shared.base import ConfigDict, CvTerm, KeyValue, Revision, UnitCvTerm
from mhd_model.shared.dataset_builder import BaseMhDatasetProfile, MhDatasetBuilder
from mhd_model.shared.fields import DOI
from mhd_model.shared.model import BaseMhdDataset, IdentifiableMhdModel
from mhd_model.shared.validation.cv_term_helper import BaseCvTermHelper
from mhd_model.validation import select_cv_term_helper
from pydantic import BaseModel, HttpUrl, InstanceOf, ValidationError

from mtbls2mhd.commands.output_paths import resolve_output_file_path
from mtbls2mhd.config import BuildType, Mtbls2MhdConfiguration
from mtbls2mhd.user_profile_utils import update_submitter_user_from_keycloak
from mtbls2mhd.utils.db_metadata_collector import (
    DbMetadataCollector,
    create_postgresql_connection,
)
from mtbls2mhd.utils.folder_metadata_collector import (
    LocalFolderMetadataCollector,
)
from mtbls2mhd.v1_0 import definitions as mtbls_definitions
from mtbls2mhd.v1_0.cv_term_creator import OntologyCacheService, OntologyTermCreator

logger = logging.getLogger(__name__)


def get_urn(
    dataset_id: str,
    node_class: type[IdentifiableMhdModel],
    identifier: None | str,
    urn_namespace: str = "mhd",
    repository_short_name: None | str = "mtbls",
) -> str:
    return default_get_urn(
        urn_namespace=urn_namespace,
        dataset_id=dataset_id,
        repository_short_name=repository_short_name,
        node_class=node_class,
        identifier=identifier,
    )


class ParameterDefinitionSummary(BaseModel):
    index: None | int = None
    isa_parameter: None | investigation_file.ParameterDefinition = None
    cv_term: None | CvTerm = None
    definition_node: None | mhd_domain.ParameterDefinition = None
    definition_type: None | mhd_domain.CvTermObject = None


class ProtocolRunSummary(BaseModel):
    protocol_name: str = ""
    assay_protocol: None | AssayProtocolSummary = None
    sample_run_configurations: OrderedDict[str, mhd_domain.SampleRunConfiguration] = (
        OrderedDict()
    )


class SampleRunSummary(BaseModel):
    index: None | int = None
    sample_name: None | str = None
    run_name: None | str = None
    assay_summary: None | AssaySummary = None
    sample_summary: None | SampleSummary = None
    node: None | mhd_domain.SampleRun = None
    sample_run_configurations: list[SampleRunConfigurationSummary] = []
    raw_data_files: dict[str, DataFileSummary] = {}
    derived_data_files: dict[str, DataFileSummary] = {}
    result_files: dict[str, AssignmentFileSummary] = {}


class SampleRunConfigurationSummary(BaseModel):
    first_row_index: None | int = None
    first_assay_summary: None | AssaySummary = None
    key: None | str = None
    node: None | mhd_domain.SampleRunConfiguration = None
    protocol_summary: None | ProtocolSummary = None
    parameter_values: dict[
        str, mhd_domain.CvTermObject | mhd_domain.CvTermValueObject
    ] = {}


class ProtocolSummary(BaseModel):
    index: None | int = None
    isa_protocol: None | investigation_file.Protocol = None
    protocol_name: str = ""
    protocol_node: None | mhd_domain.Protocol = None
    column: None | IsaTableColumn = None
    protocol_type_node: None | mhd_domain.CvTermObject = None
    protocol_type: None | CvTerm = None
    parameter_definitions: dict[str, ParameterDefinitionSummary] = {}
    parameters: None | list[str] = []
    protocol_configs: dict[str, SampleRunConfigurationSummary] = {}


class AssayProtocolSummary(BaseModel):
    index: None | int = None
    column: None | IsaTableColumn = None
    name: None | str = None
    assay_summary: None | AssaySummary = None
    protocol_summary: None | ProtocolSummary = None


class StudyFactorSummary(BaseModel):
    index: None | int = None


class DescriptorSummary(BaseModel):
    index: None | int = None


class SampleSummary(BaseModel):
    index: None | int = None
    name: None | str = None
    sample_node: None | mhd_domain.Sample = None
    subject_nodes: None | list[mhd_domain.Subject] = None


class AssignmentFileSummary(BaseModel):
    index: None | int = None
    name: None | str = None
    file_node: mhd_domain.ResultFile = None
    isa_file_content: None | IsaTableFile = None


class MetadataFileSummary(BaseModel):
    index: None | int = None
    name: None | str = None
    file_node: mhd_domain.MetadataFile = None
    isa_file_content: None | IsaTableFile | investigation_file.Investigation = None


class DataFileSummary(BaseModel):
    index: None | int = None
    name: None | str = None
    file_node: (
        None
        | mhd_domain.RawDataFile
        | mhd_domain.DerivedDataFile
        | mhd_domain.SupplementaryFile
    ) = None
    raw_file: bool = False


class AssaySummary(BaseModel):
    index: None | int = None
    name: None | str = None
    isa_assay: None | investigation_file.Assay = None
    protocols: None | list[AssayProtocolSummary] = []
    assay_node: None | mhd_domain.Assay = None
    isa_file_content: None | AssayFile = None
    sample_runs: dict[str, SampleRunSummary] = {}
    assignment_files: list[AssignmentFileSummary] = []
    raw_data_files: list[DataFileSummary] = []
    derived_data_files: list[DataFileSummary] = []


class StudySummary(BaseModel):
    isa_study: None | investigation_file.Study = None
    study_node: None | mhd_domain.Study = None
    protocols: list[ProtocolSummary] = []
    assays: list[AssaySummary] = []
    assignment_files: list[AssignmentFileSummary] = []
    factors: list[StudyFactorSummary] = []
    design_descriptors: list[DescriptorSummary] = []
    metadata_files: dict[str, MetadataFileSummary] = {}
    raw_data_files: dict[str, DataFileSummary] = {}
    derived_data_files: dict[str, DataFileSummary] = {}
    supplementary_data_files: dict[str, DataFileSummary] = {}
    samples: dict[str, SampleSummary] = {}


class DatasetBuildContext(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    study_id: str
    study: None | StudySummary = None
    data: None | MetabolightsStudyModel = None
    mhd_builder: None | MhDatasetBuilder = None


class MhdDatasetBuilder_v1_0:
    def __init__(
        self,
        config: Mtbls2MhdConfiguration,
        ontology_cache_service: None | OntologyCacheService = None,
        cv_term_helper: None | BaseCvTermHelper = None,
        min_affiliation_length: int = 2,
        **kwargs,
    ):
        self.config = config
        self.ontology_cache_service = ontology_cache_service or OntologyCacheService()

        self.cv_term_helper = cv_term_helper
        if not self.cv_term_helper:
            self.cv_term_helper = select_cv_term_helper(
                db_path=self.config.ontology_lookup_db_path
            )

        self.otc = OntologyTermCreator(
            self.ontology_cache_service,
            cv_term_helper=self.cv_term_helper,
            default_terms=mtbls_definitions.DEFAULT_TERMS,
        )
        self.min_affiliation_length = min_affiliation_length

    def find_cv_term_by_label(
        self, group: str, label: str, allow_synonyms: bool = False
    ):
        mapping = mtbls_definitions.get_cv_terms_mapping()
        item = mapping.get(group, {}).get(label.lower(), None)
        if item:
            return item
        self.cv_term_helper.find_cv_term()
        return None

    def convert_to_curie(self, source_ref: str, uri: str) -> str:
        if not uri:
            return ""
        parts = uri.split("/")
        if len(parts) > 1:
            part = parts[-1]
            if source_ref.upper() == "EDAM":
                return f"{source_ref.upper()}:{part}"
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

    def add_contacts(self, context: DatasetBuildContext):
        study_id = context.study_id
        data = context.data
        mhd_builder = context.mhd_builder
        mhd_study = context.study.study_node
        study = data.investigation.studies[0]
        contacts_by_fullname: dict[str, mhd_domain.Person] = {}
        contacts_by_email: dict[str, mhd_domain.Person] = {}
        organizations = {}
        if self.config.build_type == BuildType.MINIMUM:
            # Add submitters as contacts only
            if data.study_db_metadata and data.study_db_metadata.submitters:
                for submitter in data.study_db_metadata.submitters:
                    mhd_contact = mhd_domain.Person(
                        uri=get_urn(study_id, mhd_domain.Person, submitter.user_name),
                        orcid=submitter.orcid or None,
                        full_name=" ".join(
                            [
                                x if len(x) > 1 else f"{x}."
                                for x in (
                                    submitter.first_name,
                                    submitter.last_name,
                                )
                                if x
                            ]
                        ),
                        email_list=[submitter.user_name],
                        address_list=[submitter.address] if submitter.address else None,
                    )
                    mhd_builder.add(mhd_contact)
                    mhd_builder.link(
                        mhd_contact,
                        "submits",
                        mhd_study,
                        reverse_relationship_name="submitted-by",
                    )
        else:
            contacts: dict[str, mhd_domain.Person] = {}
            contributers = {}
            for contact_idx, contact in enumerate(study.study_contacts.people, start=1):
                mhd_contact = None
                fullname = " ".join(
                    [
                        x if len(x) > 1 else f"{x}."
                        for x in (
                            contact.first_name,
                            contact.mid_initials,
                            contact.last_name,
                        )
                        if x
                    ]
                ).strip()
                email_list = [contact.email] if contact.email else []
                if contact.additional_emails:
                    email_list.extend(contact.additional_emails)

                try:
                    mhd_contact = None
                    existing_contact = False
                    if contact.email:
                        mhd_contact = contacts_by_email.get(contact.email.lower())
                    if not mhd_contact and fullname:
                        mhd_contact = contacts_by_fullname.get(fullname.lower())
                    if mhd_contact:
                        self.update_contact_from_isa_contact(mhd_contact, contact)
                        existing_contact = True
                    else:
                        mhd_contact = mhd_domain.Person(
                            uri=get_urn(
                                study_id, mhd_domain.Person, f"{contact_idx:03}"
                            ),
                            full_name=fullname,
                            email_list=email_list or None,
                            orcid=contact.orcid or None,
                            address_list=[contact.address.strip()]
                            if contact.address
                            else None,
                            phone_list=[contact.phone] if contact.phone else None,
                        )
                        if fullname:
                            contacts_by_fullname[fullname.lower()] = mhd_contact
                        if email_list:
                            contacts_by_email[email_list[0].lower()] = mhd_contact

                except ValidationError as e:
                    logger.error(
                        "%s study %s. contact email or orcid format is not valid. Contact will be ignored. '%s' %s",
                        mhd_study.repository_identifier,
                        contact_idx,
                        contact.email,
                        e,
                    )
                    continue

                if not mhd_contact.full_name:
                    logger.warning(
                        "%s study %s. contact's name is empty",
                        mhd_study.repository_identifier,
                        contact_idx,
                    )
                    continue
                if contact.additional_emails:
                    if mhd_contact.email_list:
                        mhd_contact.email_list.extend(
                            [x for x in contact.additional_emails if x]
                        )
                    else:
                        mhd_contact.email_list = contact.additional_emails
                if not mhd_contact.email_list:
                    logger.warning(
                        "%s study %s. contact's ('%s') email is empty. Contact will be ignored.",
                        mhd_study.repository_identifier,
                        contact_idx,
                        mhd_contact.full_name,
                    )
                    # continue
                else:
                    contacts[mhd_contact.email_list[0]] = mhd_contact
                if not existing_contact:
                    mhd_builder.add(mhd_contact)
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

                affiliation = contact.affiliation or None
                organization = None
                ror_id = contact.affiliation_ror_id or ""
                if ror_id:
                    term, _ = self.cv_term_helper.find_cv_term_with_accession(
                        "ror", ror_id
                    )
                    if term:
                        affiliation = term.name
                    ror_id = term.accession if term else None
                    if ror_id and not ror_id.startswith(("http://", "https://")):
                        ror_id = self.cv_term_helper.get_uri(term)

                if not affiliation:
                    continue
                if len(affiliation) < self.min_affiliation_length:
                    logger.warning(
                        "Affiliation will be skipped for %s. Its min length does not comply with MHD requirements %s",
                        contact.email,
                        affiliation,
                    )
                    continue

                if affiliation not in organizations:
                    organization = mhd_domain.Organization(
                        uri=get_urn(study_id, mhd_domain.Organization, affiliation),
                        name=affiliation,
                        ror_id=ror_id or None,
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
            if data.study_db_metadata and data.study_db_metadata.submitters:
                for submitter in data.study_db_metadata.submitters:
                    if submitter.user_name not in contacts:
                        affiliation = submitter.affiliation or None
                        ror_id = submitter.affiliation_ror_id or None
                        if ror_id:
                            term, _ = self.cv_term_helper.find_cv_term_with_accession(
                                "ror", ror_id
                            )
                            if term:
                                affiliation = term.name
                            ror_id = term.accession if term else None
                            if ror_id and not ror_id.startswith(
                                ("http://", "https://")
                            ):
                                ror_id = self.cv_term_helper.get_uri(term)
                        address = submitter.address or None
                        organization = None
                        if not affiliation or len(affiliation) < 2:
                            continue
                        if affiliation not in organizations:
                            organization = mhd_domain.Organization(
                                uri=get_urn(
                                    study_id, mhd_domain.Organization, affiliation
                                ),
                                name=affiliation,
                                ror_id=ror_id,
                                address=address,
                            )
                            mhd_builder.add(organization)
                            organizations[affiliation] = organization

                        organization = organizations[affiliation]
                        orcid = None
                        if re.match(
                            r"^[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{3}[X0-9]$",
                            submitter.orcid or "",
                        ):
                            orcid = submitter.orcid
                        mhd_contact = None
                        existing_contact = False
                        fullname = " ".join(
                            [
                                x if len(x) > 1 else f"{x}."
                                for x in (
                                    submitter.first_name,
                                    submitter.last_name,
                                )
                                if x
                            ]
                        ).strip()
                        if submitter.user_name:
                            mhd_contact = contacts_by_email.get(
                                submitter.user_name.lower()
                            )
                        if not mhd_contact and fullname:
                            mhd_contact = contacts_by_fullname.get(fullname.lower())

                        if mhd_contact:
                            existing_contact = True
                            mhd_contact.full_name = mhd_contact.full_name or fullname
                            mhd_contact.orcid = mhd_contact.orcid or orcid
                            if mhd_contact.email_list:
                                if submitter.user_name:
                                    mhd_contact.email_list.append(submitter.user_name)
                                else:
                                    mhd_contact.email_list = [submitter.user_name]
                            elif submitter.user_name:
                                mhd_contact.email_list = [submitter.user_name]

                        else:
                            mhd_contact = mhd_domain.Person(
                                uri=get_urn(
                                    study_id, mhd_domain.Person, submitter.user_name
                                ),
                                full_name=fullname,
                                email_list=[submitter.user_name]
                                if submitter.user_name
                                else None,
                                address_list=[submitter.address.strip()]
                                if submitter.address
                                else None,
                                orcid=orcid or None,
                            )
                            if fullname:
                                contacts_by_fullname[fullname.lower()] = mhd_contact
                            if email_list:
                                contacts_by_email[email_list[0].lower()] = mhd_contact

                        if not existing_contact:
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

    def update_contact_from_isa_contact(
        self,
        mhd_contact: mhd_domain.Person,
        contact: investigation_file.Person,
    ):
        fullname = " ".join(
            [
                x if len(x) > 1 else f"{x}."
                for x in (
                    contact.first_name,
                    contact.mid_initials,
                    contact.last_name,
                )
                if x
            ]
        ).strip()
        contact_email = contact.email.lower() if contact.email else None
        if contact_email:
            if mhd_contact.email_list:
                mhd_contact.email_list.append(contact_email)
            else:
                mhd_contact.email_list = [contact_email]

        mhd_contact.full_name = mhd_contact.full_name or fullname or None
        mhd_contact.orcid = mhd_contact.orcid or contact.orcid or None
        contact_address = contact.address.strip() if contact.address else None
        if contact_address:
            if mhd_contact.address_list:
                mhd_contact.address_list.append(contact_address)
            else:
                mhd_contact.address_list = [contact_address]
        contact_phone = contact.phone or None
        if contact_phone:
            if mhd_contact.phone_list:
                mhd_contact.phone_list.append(contact_phone)
            else:
                mhd_contact.phone_list = [contact_phone]

    def add_funders(
        self,
        context: DatasetBuildContext,
        organizations: dict[str, mhd_domain.Organization],
    ):
        study_id = context.study_id
        data = context.data
        mhd_builder = context.mhd_builder
        mhd_study = context.study.study_node
        if self.config.build_type == BuildType.MINIMUM:
            return
        study: investigation_file.Study = data.investigation.studies[0]
        study_id = study.identifier
        comments = {x.name: x for x in study.comments if x and x.name}
        grant_ids = []
        funders = []
        funder_ror_ids = []
        if comments.get("Funder") and comments["Funder"].value:
            if isinstance(comments["Funder"].value, str):
                funders = comments["Funder"].value.split(";") or []
            else:
                funders = comments["Funder"].value[0].split(";") or []
        if comments.get("Funder ROR ID") and comments["Funder ROR ID"].value:
            if isinstance(comments["Funder ROR ID"].value, str):
                funder_ror_ids = comments["Funder ROR ID"].value.split(";") or []
            else:
                funder_ror_ids = comments["Funder ROR ID"].value[0].split(";") or []
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
                grant_ids.extend([x for x in identifiers if x])
        funder_links = {}
        grant_id_node = self.otc.create_cv_term_object(
            type_="descriptor",
            source="OBI",
            accession="OBI:0001629",
            name="grant identifier",
        )
        grant_id_node_is_used = False
        for idx, funder in enumerate(funders or []):
            ror_id = funder_ror_ids[idx] if len(funder_ror_ids) > idx else None
            if ror_id:
                term, _ = self.cv_term_helper.find_cv_term_with_accession("ror", ror_id)
                if term:
                    funder = term.name
                ror_id = term.accession if term else None
                if ror_id and not ror_id.startswith(("http://", "https://")):
                    ror_id = self.cv_term_helper.get_uri(term)
            if not funder:
                continue
            if len(funder or "") < self.min_affiliation_length:
                logger.warning(
                    "Funder '%s' will be skipped for %s. "
                    "Its min length does not comply with MHD requirements",
                    study_id,
                    funder,
                )
                continue
            organization = organizations.get(funder)
            if not organization and funder:
                organization = mhd_domain.Organization(
                    uri=get_urn(study_id, mhd_domain.Organization, funder),
                    ror_id=ror_id or None,
                    name=funder,
                )
                mhd_builder.add(organization)
                organizations[funder] = organization
            grant_id = grant_ids[idx] if len(grant_ids) > idx else None
            grant_key = None
            if grant_id:
                grant_id_node_is_used = True
                grant_key = KeyValue(
                    key=grant_id_node.id_,
                    value=grant_id,
                )
            if organization.name in funder_links:
                if grant_key:
                    for link in funder_links[organization.name]:
                        if link.tag_list:
                            link.tag_list.append(grant_key)
                        else:
                            link.tag_list = [grant_key]
            else:
                links = mhd_builder.link(
                    mhd_study,
                    "funded-by",
                    organization,
                    reverse_relationship_name="funds",
                    tag_list=[grant_key] if grant_key else None,
                )
                funder_links[organization.name] = links
        if grant_id_node_is_used:
            mhd_builder.add(grant_id_node)

    def add_related_datasets(self, context: DatasetBuildContext):
        data = context.data
        mhd_builder = context.mhd_builder
        mhd_study = context.study.study_node
        study = data.investigation.studies[0]

        study.related_datasets = study.related_datasets or []
        url: None | mhd_domain.CvTermObject = None
        for idx, related_dataset in enumerate(study.related_datasets):
            if not related_dataset.url:
                continue
            if idx == 0:
                url = self.otc.create_cv_term_object(
                    accession="NCIT:C42743",
                    name="Uniform Resource Locator",
                    source="NCIT",
                )
                mhd_builder.add_node(url)
            mhd_study.related_dataset_list.append(
                KeyValue(
                    key=url.id_,
                    value=related_dataset.url,
                )
            )

    def add_publications(self, context: DatasetBuildContext):
        study_id = context.study_id
        data = context.data
        mhd_builder = context.mhd_builder
        mhd_study = context.study.study_node
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

            if valid_doi and doi:
                title = publication.title or ""
                mhd_publication = mhd_domain.Publication(
                    uri=get_urn(study_id, mhd_domain.Publication, doi),
                    title=title,
                    doi=doi,
                    pubmed_id=publication.pub_med_id or None,
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
            no_publication_cv_term = MISSING_PUBLICATION_REASON.get("no publication")
            pending_publication_cv_term = MISSING_PUBLICATION_REASON.get(
                "pending publication"
            )
            if not study.study_publications.publications:
                no_publication = self.otc.create_cv_term_object(
                    type_="descriptor",
                    accession=no_publication_cv_term.accession,
                    source=no_publication_cv_term.source,
                    name=no_publication_cv_term.name,
                )
                mhd_builder.add(
                    no_publication,
                    use_label_for_invalid_cv_term=self.config.use_label_for_invalid_cv_term,
                )
                mhd_builder.link(
                    mhd_study,
                    "defined-as",
                    no_publication,
                    reverse_relationship_name="publication-status-of",
                )
            else:
                pending_publication = self.otc.create_cv_term_object(
                    type_="descriptor",
                    accession=pending_publication_cv_term.accession,
                    source=pending_publication_cv_term.source,
                    name=pending_publication_cv_term.name,
                )
                mhd_builder.add(
                    pending_publication,
                    use_label_for_invalid_cv_term=self.config.use_label_for_invalid_cv_term,
                )
                mhd_builder.link(
                    mhd_study,
                    "defined-as",
                    pending_publication,
                    reverse_relationship_name="publication-status-of",
                )

    def add_metadata_files(self, context: DatasetBuildContext):
        study_id = context.study_id
        data = context.data
        mhd_builder = context.mhd_builder
        mhd_study = context.study.study_node
        isa_study = context.study.isa_study
        metadata_files: list[
            tuple[str, IsaTableFile | investigation_file.Investigation, int]
        ] = []

        metadata_files.append(
            (
                data.investigation_file_path,
                data.investigation,
                "isa-investigation/000",
                None,
            )
        )
        sample_content = data.samples.get(isa_study.file_name)
        metadata_files.append(
            (isa_study.file_name, sample_content, "isa-study/000", None)
        )
        for assay_summary in context.study.assays:
            assay_content = data.assays.get(assay_summary.isa_assay.file_name)
            metadata_files.append(
                (
                    assay_summary.isa_assay.file_name,
                    assay_content,
                    f"isa-assay/{assay_summary.index:03}",
                    assay_summary.index,
                )
            )

        appended_formats: set[str] = set()
        formats = {
            "EDAM:format_4066": self.otc.create_cv_term_object(
                type_="descriptor",
                accession="EDAM:format_4066",
                source="EDAM",
                name="ISA-Tab Investigation file",
            ),
            "EDAM:format_4067": self.otc.create_cv_term_object(
                type_="descriptor",
                accession="EDAM:format_4067",
                source="EDAM",
                name="ISA-Tab Study file",
            ),
            "EDAM:format_4068": self.otc.create_cv_term_object(
                type_="descriptor",
                accession="EDAM:format_4068",
                source="EDAM",
                name="ISA-Tab Assay file",
            ),
            "EDAM:format_3687": self.otc.create_cv_term_object(
                type_="descriptor",
                accession="EDAM:format_3687",
                source="EDAM",
                name="ISA-Tab",
            ),
        }

        for file, content, file_id, file_index in metadata_files:
            if self.config.build_type == BuildType.MINIMUM:
                meta = mhd_domain.MetadataFile(
                    uri=get_urn(study_id, mhd_domain.MetadataFile, file),
                    name=file,
                    url_list=[
                        f"{self.config.public_http_base_url}/{study_id}/{file}",
                        f"{self.config.public_ftp_base_url}/{study_id}/{file}",
                    ],
                )
            else:
                selected_format = None
                if file.startswith("i_"):
                    selected_format = "EDAM:format_4066"
                elif file.startswith("s_"):
                    selected_format = "EDAM:format_4067"
                elif file.startswith("a_"):
                    selected_format = "EDAM:format_4068"
                else:
                    selected_format = "EDAM:format_3687"
                appended_formats.add(selected_format)
                format_object = formats[selected_format]
                meta = mhd_domain.MetadataFile(
                    uri=get_urn(study_id, mhd_domain.MetadataFile, file_id),
                    name=file,
                    extension=Path(file).suffix,
                    format_ref=format_object.id_,
                    url_list=[
                        f"{self.config.public_http_base_url}/{study_id}/{file}",
                        f"{self.config.public_ftp_base_url}/{study_id}/{file}",
                    ],
                )
            mhd_builder.add_node(meta)
            context.study.metadata_files[file] = MetadataFileSummary(
                index=file_index,
                file_node=meta,
                isa_file_content=content,
                name=file,
            )

            mhd_builder.link(mhd_study, "has-metadata-file", meta)
            mhd_builder.link(meta, "describes", mhd_study)

        for x in appended_formats or []:
            mhd_builder.add_node(
                formats[x],
                use_label_for_invalid_cv_term=self.config.use_label_for_invalid_cv_term,
            )

    def add_result_files(self, context: DatasetBuildContext):
        study_id = context.study_id
        mhd_builder = context.mhd_builder
        mhd_study = context.study.study_node

        tsv_format = self.otc.create_cv_term_object(
            type_="descriptor",
            accession="EDAM:format_4073",
            source="EDAM",
            name="MetaboLights MAF",
        )
        for assignment_file in context.study.assignment_files:
            maf_idx = assignment_file.index
            file_name = assignment_file.name
            result_file = mhd_domain.ResultFile(
                uri=get_urn(study_id, mhd_domain.ResultFile, f"{maf_idx:03}"),
                name=file_name,
                extension=Path(file_name).suffix,
                format_ref=tsv_format.id_,
                url_list=[
                    f"{self.config.public_ftp_base_url}/{study_id}/{file_name}",
                    f"{self.config.public_http_base_url}/{study_id}/{file_name}",
                ],
            )
            mhd_builder.add_node(result_file)
            assignment_file.file_node = result_file
            if maf_idx == 0:
                mhd_builder.add_node(
                    tsv_format,
                    use_label_for_invalid_cv_term=self.config.use_label_for_invalid_cv_term,
                )
            mhd_builder.link(mhd_study, "has-result-file", result_file)
            mhd_builder.link(result_file, "created-in", mhd_study)

    def add_study_factor_definitions(self, context: DatasetBuildContext):
        study_id = context.study_id
        data = context.data
        mhd_builder = context.mhd_builder
        mhd_study = context.study.study_node
        if self.config.build_type == BuildType.MINIMUM:
            return
        study = data.investigation.studies[0]
        study_id = study.identifier
        for factor_idx, item in enumerate(study.study_factors.factors):
            factor_name = item.name.lower()
            if factor_name in mtbls_definitions.MANAGED_STUDY_FACTOR_MAP:
                cv_term = mtbls_definitions.MANAGED_STUDY_FACTOR_MAP[factor_name]
                type_definition = "x-mtbls-factor-type"
                if factor_name in COMMON_STUDY_FACTOR_DEFINITIONS:
                    type_definition = "factor-type"
                factor_type = self.otc.create_cv_term_object(
                    type_=type_definition,
                    name=cv_term.name,
                    source=cv_term.source,
                    accession=cv_term.accession,
                )
            else:
                if factor_name in mtbls_definitions.MTBLS_STUDY_FACTOR_MAP:
                    cv_term = mtbls_definitions.MTBLS_STUDY_FACTOR_MAP[factor_name]
                else:
                    cv_term = CvTerm(name=factor_name)

                factor_type = self.otc.create_cv_term_object(
                    type_="x-mtbls-factor-type",
                    name=cv_term.name,
                    source=cv_term.source,
                    accession=cv_term.accession,
                )
            mhd_builder.add(
                factor_type,
                use_label_for_invalid_cv_term=self.config.use_label_for_invalid_cv_term,
            )
            factor_definition = mhd_domain.FactorDefinition(
                uri=get_urn(study_id, mhd_domain.FactorDefinition, f"{factor_idx:03}"),
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

    def add_characteristic_definitions(self, context: DatasetBuildContext):
        study_id = context.study_id
        mhd_builder = context.mhd_builder
        mhd_study = context.study.study_node
        study = context.study.isa_study
        sample_file = context.data.samples[study.file_name]
        characteristics_map: dict[str, mhd_domain.CvTermObject] = {}
        created_characteristics = {}
        characteristic_idx = 0
        for item in sample_file.table.headers:
            if item.column_header.startswith("Characteristics["):
                name = (
                    item.column_header.removeprefix("Characteristics[")
                    .removesuffix("]")
                    .lower()
                )
                if (
                    name not in mtbls_definitions.MANAGED_CHARACTERISTICS_MAP
                    and self.config.build_type in (BuildType.MINIMUM, BuildType.FULL)
                ):
                    continue

                if name in mtbls_definitions.MANAGED_CHARACTERISTICS_MAP:
                    cv_term = mtbls_definitions.MANAGED_CHARACTERISTICS_MAP[name]
                    characteristic_type = self.otc.create_cv_term_object(
                        type_="characteristic-type",
                        name=cv_term.name,
                        accession=cv_term.accession,
                        source=cv_term.source,
                    )
                else:
                    if name in mtbls_definitions.MTBLS_CHARACTERISTICS_MAP:
                        cv_term = mtbls_definitions.MTBLS_CHARACTERISTICS_MAP[name]
                    else:
                        cv_term = CvTerm(name=name)
                    characteristic_type = self.otc.create_cv_term_object(
                        type_="x-mtbls-characteristic-type",
                        name=cv_term.name,
                        accession=cv_term.accession,
                        source=cv_term.source,
                    )
                created_characteristics[name] = characteristic_type
                key = characteristic_type.accession or characteristic_type.name
                if key not in characteristics_map:
                    characteristics_map[key] = characteristic_type

                    mhd_builder.add(
                        characteristic_type,
                        use_label_for_invalid_cv_term=self.config.use_label_for_invalid_cv_term,
                    )
                characteristic_idx += 1
                characteristic_type = characteristics_map.get(key)
                characteristic = mhd_domain.CharacteristicDefinition(
                    uri=get_urn(
                        study_id,
                        mhd_domain.CharacteristicDefinition,
                        f"{characteristic_idx:03}",
                    ),
                    characteristic_type_ref=characteristic_type.id_,
                    name=name,
                )
                mhd_builder.add(characteristic)
                mhd_builder.link(
                    characteristic,
                    "has-type",
                    characteristic_type,
                    reverse_relationship_name="type-of",
                )
                mhd_builder.link(
                    mhd_study,
                    "has-characteristic-definition",
                    characteristic,
                    reverse_relationship_name="used-in",
                )
        study_id = mhd_study.repository_identifier
        characteristic_idx = 900
        for name, term in mtbls_definitions.MANAGED_CHARACTERISTICS_MAP.items():
            if name not in created_characteristics:
                characteristic_type = self.otc.create_cv_term_object(
                    type_="characteristic-type",
                    name=term.name,
                    accession=term.accession,
                    source=term.source,
                )
                characteristics_map[term.accession or term.name] = characteristic_type
                mhd_builder.add(
                    characteristic_type,
                    use_label_for_invalid_cv_term=self.config.use_label_for_invalid_cv_term,
                )
                characteristic_idx += 1
                characteristic = mhd_domain.CharacteristicDefinition(
                    uri=get_urn(
                        study_id,
                        mhd_domain.CharacteristicDefinition,
                        f"{characteristic_idx:03}",
                    ),
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
                missing_data = COMMON_MISSING_DATA_TERMS["not available"]
                characteristic_value = mhd_domain.CvTermObject(
                    type_="characteristic-value",
                    name=missing_data.name,
                    accession=missing_data.accession,
                    source=missing_data.source,
                )
                mhd_builder.link(
                    characteristic,
                    "has-instance",
                    characteristic_value,
                    reverse_relationship_name="instance-of",
                )
                mhd_builder.link(
                    characteristic_value,
                    "has-type",
                    characteristic_type,
                    reverse_relationship_name="type-of",
                )

    def add_samples(self, context: DatasetBuildContext) -> dict[str, mhd_domain.Sample]:
        study_id = context.study_id
        mhd_builder = context.mhd_builder
        mhd_study = context.study.study_node
        study = context.study.isa_study
        sample_file = context.data.samples[study.file_name]
        samples_map: dict[str, mhd_domain.Sample] = {}
        factor_definitions: list[mhd_domain.CvTermObject] = []
        characteristics: list[mhd_domain.CvTermObject] = []

        for idx in mhd_builder.objects:
            item = mhd_builder.objects[idx]
            if isinstance(item, Relationship) and item.source_ref == mhd_study.id_:
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
        study_id = mhd_study.repository_identifier
        for sample_idx, name in enumerate(data["Sample Name"]):
            if not name:
                logger.warning("Sample name is not defined at row: %s", sample_idx + 1)
                continue
            if self.config.build_type == BuildType.MINIMUM:
                characteristic_values = self.create_values(
                    mhd_builder,
                    sample_idx,
                    sample_file,
                    characteristics,
                    characteristics_map,
                    characteristic_values_map,
                    mtbls_definitions.MANAGED_CHARACTERISTICS_MAP,
                    "characteristic-value",
                    definition_type_property="characteristic_type_ref",
                )
                continue

            subject_name = (
                data["Source Name"][sample_idx] or data["Sample Name"][sample_idx]
            )
            if not subject_name:
                pass
            if subject_name not in subject_map:
                subject_map[subject_name] = mhd_domain.Subject(
                    name=subject_name,
                    uri=get_urn(study_id, mhd_domain.Subject, f"{sample_idx:05}"),
                )
                mhd_builder.add(subject_map[subject_name])
            subject = subject_map[subject_name]
            if name not in sample_map:
                samples_map[name] = mhd_domain.Sample(
                    name=name,
                    uri=get_urn(study_id, mhd_domain.Sample, f"{sample_idx:05}"),
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
            context.study.samples[name] = SampleSummary(
                index=sample_idx,
                name=name,
                sample_node=sample,
                subject_nodes=[subject],
            )
            mhd_builder.link(
                mhd_study,
                "has-sample",
                sample,
                reverse_relationship_name="used-in",
            )

            characteristic_values = self.create_values(
                mhd_builder,
                sample_idx,
                sample_file,
                characteristics,
                characteristics_map,
                characteristic_values_map,
                mtbls_definitions.MANAGED_CHARACTERISTICS_MAP,
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
                sample_idx,
                sample_file,
                factor_definitions,
                factors_map,
                factors_values_map,
                mtbls_definitions.MANAGED_STUDY_FACTOR_MAP,
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

        missing_data = COMMON_MISSING_DATA_TERMS["not available"]
        characteristic_value = mhd_domain.CvTermObject(
            type_="characteristic-value",
            name=missing_data.name,
            accession=missing_data.accession,
            source=missing_data.source,
        )
        characteristic_labels: dict[str, mhd_domain.CvTermObject] = {
            x.name.lower(): x for x in characteristics
        }
        missing_added = False
        for k in mtbls_definitions.MANAGED_CHARACTERISTICS_MAP:
            if k not in characteristic_values_map:
                if not missing_added:
                    mhd_builder.add(
                        characteristic_value,
                        use_label_for_invalid_cv_term=self.config.use_label_for_invalid_cv_term,
                    )
                    missing_added = True
                mhd_builder.link(
                    characteristic_labels[k],
                    "has-instance",
                    characteristic_value,
                    reverse_relationship_name="instance-of",
                )
                char_type = mhd_builder.objects.get(
                    characteristic_labels[k].characteristic_type_ref
                )
                mhd_builder.link(
                    char_type,
                    "type-of",
                    characteristic_value,
                    reverse_relationship_name="has-type",
                )
        return samples_map

    def sanitize_string(self, input_string: str) -> str:
        sanitized = re.sub(r"[^0-9a-zA-Z-]", "", input_string)
        return sanitized

    def append_cv_term_object(
        self,
        item_list: list,
        type_: str,
        source: None | str = None,
        accession: None | str = None,
        name: None | str = None,
    ):
        item = self.otc.create_cv_term_object(
            type_=type_, source=source, accession=accession, name=name
        )
        if item:
            item_list.append(item)

    def append_cv_term_value_object(
        self,
        item_list: list,
        type_: str,
        source: None | str = "",
        accession: None | str = "",
        name: None | str = "",
        value: None | str = None,
        unit: None | UnitCvTerm = None,
    ):
        item = self.otc.create_cv_term_value_object(
            type_=type_,
            source=source,
            accession=accession,
            name=name,
            value=value,
            unit=unit,
        )
        if item:
            item_list.append(item)

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
        mtbls_cv_terms_mappings = mtbls_definitions.get_cv_terms_mapping()
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
            if len(data[column_name]) < idx:
                continue
            name = data[column_name][idx]

            if not name:
                continue
            object_name = name_prefix
            if key.lower() not in common_map:
                object_name = "x-mtbls-" + name_prefix

            values_map[key] = {}
            index = columns.index(column_name)

            if name not in values_map[key]:
                item = mtbls_cv_terms_mappings.get(key, {}).get(name.lower(), None)
                if item:
                    sources = item.source.split(";")
                    accessions = item.accession.split(";")
                    names = item.name.split(";")

                    item = []
                    for i, val in enumerate(names):
                        source_ref = sources[i] if len(sources) > i else ""
                        iri = (
                            self.convert_to_curie(source_ref, accessions[i])
                            if len(accessions) > i
                            else ""
                        )
                        item.append(
                            self.otc.create_cv_term_object(
                                type_=object_name,
                                source=source_ref,
                                accession=iri,
                                name=val,
                            )
                        )
                elif index + 1 < len(columns) and columns[index + 1].startswith(
                    "Term Source REF"
                ):
                    source = data[columns[index + 1]][idx]
                    accession = (
                        data[columns[index + 2]][idx]
                        if index + 2 < len(columns)
                        else ""
                    )
                    sources = source.split(";")
                    accessions = accession.split(";")
                    names = name.split(";")
                    item = []
                    for i, val in enumerate(names):
                        source_ref = sources[i] if len(sources) > i else ""
                        iri = (
                            self.convert_to_curie(source_ref, accessions[i])
                            if len(accessions) > i
                            else ""
                        )
                        self.append_cv_term_value_object(
                            item_list=item,
                            type_=object_name,
                            source=source_ref,
                            accession=iri,
                            name=val,
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
                        data[columns[index + 3]][idx]
                        if index + 3 < len(columns)
                        else ""
                    )
                    sources = source.split(";")
                    accessions = accession.split(";")
                    names = name.split(";")
                    units = unit.split()
                    item = []
                    for i, val in enumerate(names):
                        source_ref = sources[i] if len(sources) > i else ""
                        iri = (
                            self.convert_to_curie(source_ref, accessions[i])
                            if len(accessions) > i
                            else ""
                        )
                        self.append_cv_term_value_object(
                            item_list=item,
                            type_=object_name,
                            value=val,
                            unit=UnitCvTerm(
                                source=source_ref,
                                accession=iri,
                                name=units[i] if len(units) > i else "",
                            ),
                        )
                else:
                    item = self.otc.create_cv_term_object(
                        type_=object_name, name=name, accession="", source=""
                    )
                if item:
                    if not isinstance(item, list):
                        item = [item]
                    values_map[key][name] = item
                    for sub_item in item:
                        mhd_builder.add(
                            sub_item,
                            use_label_for_invalid_cv_term=self.config.use_label_for_invalid_cv_term,
                        )
                        mhd_builder.link(
                            term,
                            "has-instance",
                            sub_item,
                            reverse_relationship_name="instance-of",
                        )
                        if hasattr(term, definition_type_property) and type_obj:
                            mhd_builder.link(
                                type_obj,
                                "type-of",
                                sub_item,
                                reverse_relationship_name="has-type",
                            )

            value = values_map[key][name]
            if isinstance(value, list):
                values.extend(value)
            else:
                values.append(value)
        return values

    def add_sample_runs(
        self, context: DatasetBuildContext, assay_summary: AssaySummary
    ) -> dict[str, OrderedDict[str, str]]:
        study_id = context.study_id
        assay_table = assay_summary.isa_file_content.table
        mhd_builder = context.mhd_builder
        if not assay_table.data.get("Sample Name"):
            return
        mtbls_cv_terms_mappings = mtbls_definitions.get_cv_terms_mapping()
        index_number_node = self.otc.create_cv_term_object(
            type_="descriptor",
            source="NCIT",
            accession="NCIT:C25390",
            name="Index",
        )
        if index_number_node.id_ not in mhd_builder.objects:
            mhd_builder.add(index_number_node)
        study_protocols = {x.protocol_name: x for x in context.study.protocols}
        assay_protocols: dict[str, AssayProtocolSummary] = {}
        index = 0
        for header in assay_table.headers:
            if header.column_header == "Protocol REF":
                column_idx = assay_table.columns.index(header.column_name)
                protocol_name = assay_table.data[header.column_name][0]
                protocol_summary = study_protocols.get(protocol_name)
                if not protocol_name or not protocol_summary:
                    logger.warning(
                        "%s in %s is not in study protocols",
                        assay_summary.name,
                        protocol_name,
                    )
                    continue
                assay_protocol_summary = AssayProtocolSummary(
                    index=column_idx,
                    protocol_summary=protocol_summary,
                    assay_summary=assay_summary,
                    column=header,
                    name=protocol_name,
                )
                assay_summary.protocols.append(assay_protocol_summary)
                context.mhd_builder.link(
                    assay_summary.assay_node,
                    "follows",
                    assay_protocol_summary.protocol_summary.protocol_node,
                    reverse_relationship_name="used-in",
                    tag_list=[KeyValue(key=index_number_node.id_, value=index)],
                )
                index += 1
                assay_protocols[protocol_name] = assay_protocol_summary
        idx = 1
        sample_row_indices: dict[str, int] = {}

        parameter_definitions: dict[str, dict[str, ParameterDefinitionSummary]] = {}
        for protocol_name, assay_protocol in assay_protocols.items():
            definitions = assay_protocol.protocol_summary.parameter_definitions
            params = {}
            parameter_definitions[protocol_name] = params
            for definition_summary in definitions.values():
                isa_definition = definition_summary.isa_parameter
                if isa_definition.term_accession_number:
                    params[isa_definition.term_accession_number] = definition_summary
                if isa_definition.term:
                    params[isa_definition.term.lower()] = definition_summary
        file_colums = []
        for header in assay_table.headers:
            if header.column_header.endswith(" Data File"):
                file_colums.append(header.column_name)
        assignment_files = {x.name: x for x in context.study.assignment_files or []}
        raw_data_files = context.study.raw_data_files
        derived_data_files = context.study.derived_data_files

        for row_idx, sample_name in enumerate(assay_table.data["Sample Name"]):
            sample_row_indices[sample_name] = row_idx
            run_name = None
            if "MS Assay Name" in assay_table.data:
                run_name = assay_table.data["MS Assay Name"][row_idx]
            if not run_name and "NMR Assay Name" in assay_table.data:
                run_name = assay_table.data["NMR Assay Name"][row_idx]
            if not run_name:
                run_name = sample_name
            sample_run_summary = SampleRunSummary(
                index=row_idx,
                sample_name=sample_name,
                run_name=run_name,
                assay_summary=assay_summary,
                sample_summary=context.study.samples.get(sample_name),
                node=None,
            )
            if "Metabolite Assignment File" in assay_table.data:
                filename = assay_table.data["Metabolite Assignment File"][row_idx]
                result_file = assignment_files.get(filename)
                if result_file and filename not in sample_run_summary.result_files:
                    sample_run_summary.result_files[filename] = result_file

            for data_file_column in file_colums:
                filename = assay_table.data[data_file_column][row_idx]
                data_file = derived_data_files.get(filename)
                if data_file:
                    if filename not in sample_run_summary.derived_data_files:
                        sample_run_summary.derived_data_files[filename] = data_file
                else:
                    data_file = raw_data_files.get(filename)
                    if data_file and filename not in sample_run_summary.raw_data_files:
                        sample_run_summary.raw_data_files[filename] = data_file

            raw_data_file_refs = []
            for v in sample_run_summary.raw_data_files.values() or []:
                raw_data_file_refs.append(v.file_node.id_)

            derived_data_file_refs = []
            for v in sample_run_summary.derived_data_files.values() or []:
                derived_data_file_refs.append(v.file_node.id_)

            result_file_refs = []
            for v in sample_run_summary.result_files.values() or []:
                result_file_refs.append(v.file_node.id_)
            sample_ref = None
            if sample_run_summary.sample_summary:
                sample_ref = sample_run_summary.sample_summary.sample_node.id_

            sample_run_idx = f"{assay_summary.index:03}/{row_idx:06}"
            mhd_sample_run = mhd_domain.SampleRun(
                uri=get_urn(study_id, mhd_domain.SampleRun, sample_run_idx),
                name=sample_name,
                sample_ref=sample_ref or None,
                raw_data_file_refs=raw_data_file_refs or None,
                derived_file_refs=derived_data_file_refs or None,
                result_file_refs=result_file_refs or None,
            )
            context.mhd_builder.add(mhd_sample_run)
            sample_run_summary.node = mhd_sample_run
            mhd_builder.link(
                assay_summary.assay_node,
                "has-sample-run",
                mhd_sample_run,
                reverse_relationship_name="part-of",
                tag_list=[KeyValue(key=index_number_node.id_, value=row_idx)],
            )
            mhd_builder.link(
                context.study.study_node,
                "has-sample-run",
                mhd_sample_run,
                reverse_relationship_name="used-in",
                tag_list=[KeyValue(key=index_number_node.id_, value=row_idx)],
            )
            assay_summary.sample_runs[sample_name] = sample_run_summary
            value = ""
            for protocol_name, assay_protocol_summary in assay_protocols.items():
                header = assay_protocol_summary.column
                col_idx = header.column_index
                current_idx = col_idx
                protocol_params = parameter_definitions.get(protocol_name)
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
                key_str = ",".join([":".join(x) for x in params.values()])
                key = hashlib.sha256(key_str.encode()).hexdigest()
                configs = assay_protocol_summary.protocol_summary.protocol_configs
                if key not in configs:
                    parameter_values = []
                    for name, values in params.items():
                        parameter = name.split("]")[0].split("[")[-1]
                        cv = self.get_parameter_cv(protocol_name, parameter)
                        definition = None
                        if not cv:
                            definition_summary = protocol_params.get(parameter.lower())
                            if definition_summary:
                                definition = definition_summary.definition_node
                        else:
                            definition_summary = protocol_params.get(parameter.lower())
                            if definition_summary:
                                definition = definition_summary.definition_node
                        if not definition:
                            continue
                        definition_value = "".join([x for x in values if x])
                        if not definition_value:
                            continue
                        object_name = "x-mtbls-parameter-value"
                        if (
                            parameter.lower()
                            in mtbls_definitions.ALL_COMMON_PROTOCOL_PARAMETERS
                        ):
                            object_name = "parameter-value"

                        term = mtbls_definitions.ALL_COMMON_PROTOCOL_PARAMETERS.get(
                            parameter.lower(), None
                        )
                        if (
                            values
                            and term
                            and term.name.lower() in mtbls_cv_terms_mappings
                        ):
                            mapping = mtbls_cv_terms_mappings[term.name.lower()]
                            mapped_value = mapping.get(values[0].lower())
                            # CONVERT TO REFERENCE TERM
                            if mapped_value:
                                if len(values) <= 3:
                                    values = (
                                        mapped_value.name,
                                        mapped_value.source,
                                        mapped_value.accession,
                                    )
                                elif len(values) == 4:
                                    values = (
                                        mapped_value.name,
                                        values[1],
                                        values[2],
                                        values[3],
                                    )

                        item = None
                        if len(values) == 1:
                            item = self.otc.create_cv_term_value_object(
                                type_=object_name, name=values[0]
                            )
                        elif len(values) == 3:
                            accession = self.convert_to_curie(values[1], values[2])
                            item = self.otc.create_cv_term_value_object(
                                type_=object_name,
                                name=values[0],
                                source=values[1],
                                accession=accession,
                            )
                        elif len(values) == 4:
                            accession = self.convert_to_curie(values[2], values[3])
                            item = self.otc.create_cv_term_value_object(
                                type_=object_name,
                                value=values[0],
                                unit=UnitCvTerm(
                                    name=values[1],
                                    source=values[2],
                                    accession=accession,
                                ),
                            )

                        if item:
                            if not isinstance(item, list):
                                item = [item]

                            parameter_values.extend(item)
                            for sub_item in item:
                                mhd_builder.add(
                                    sub_item,
                                    use_label_for_invalid_cv_term=self.config.use_label_for_invalid_cv_term,
                                )
                                mhd_builder.link(
                                    definition,
                                    "has-instance",
                                    sub_item,
                                    reverse_relationship_name="instance-of",
                                )
                                if hasattr(definition, "parameter_type_ref"):
                                    val = definition.parameter_type_ref
                                    node_type = mhd_builder.objects.get(val)
                                    if node_type:
                                        mhd_builder.link(
                                            node_type,
                                            "type-of",
                                            sub_item,
                                            reverse_relationship_name="has-type",
                                        )
                    run_config = None
                    if parameter_values:
                        assay_idx = assay_protocol_summary.assay_summary.index
                        protocol_idx = assay_protocol_summary.index
                        name = f"{assay_idx:03}/{protocol_idx:02}/{row_idx:06}"
                        run_config = mhd_domain.SampleRunConfiguration(
                            uri=get_urn(
                                study_id, mhd_domain.SampleRunConfiguration, name
                            ),
                            protocol_ref=assay_protocol_summary.protocol_summary.protocol_node.id_,
                            parameter_value_refs=[x.id_ for x in parameter_values],
                        )
                        context.mhd_builder.add(run_config)
                        configs[key] = SampleRunConfigurationSummary(
                            first_assay_summary=assay_protocol_summary.assay_summary,
                            first_row_index=row_idx,
                            key=key,
                            node=run_config,
                            protocol_summary=assay_protocol_summary.protocol_summary,
                            parameter_values={x.id_: x for x in parameter_values},
                        )
                        idx = idx + 1

                run_config = configs.get(key)

                if run_config:
                    if not sample_run_summary.node.sample_run_configuration_refs:
                        sample_run_summary.node.sample_run_configuration_refs = []
                    sample_run_summary.node.sample_run_configuration_refs.append(
                        run_config.node.id_
                    )
                    sample_run_summary.sample_run_configurations.append(run_config)

    def get_parameter_cv(
        self, protocol_name: str, parameter_name: str
    ) -> CvTerm | None:
        parameters_dict = mtbls_definitions.MTBLS_PROTOCOL_PARAMETER_DEFINITION_MAP.get(
            protocol_name.lower(), {}
        )
        return parameters_dict.get(parameter_name.lower(), None)

    def add_protocols(self, context: DatasetBuildContext):
        study_id = context.study_id
        data = context.data
        mhd_builder = context.mhd_builder
        mhd_study = context.study.study_node
        study = data.investigation.studies[0]
        protocol_types: dict[str, mhd_domain.CvTermObject] = {}
        parameter_types: dict[str, mhd_domain.CvTermObject] = {}
        protocol_parameter_types: dict[str, mhd_domain.CvTermObject] = {}

        index_number_node = self.otc.create_cv_term_object(
            type_="descriptor",
            source="NCIT",
            accession="NCIT:C25390",
            name="Index",
        )
        if index_number_node.id_ not in mhd_builder.objects:
            mhd_builder.add(index_number_node)

        for protocol_idx, protocol in enumerate(study.study_protocols.protocols):
            name = protocol.name
            protocol_summary = ProtocolSummary(
                index=protocol_idx, isa_protocol=protocol, protocol_name=protocol.name
            )
            context.study.protocols.append(protocol_summary)
            protocol_parameter_types[name] = {}
            mhd_protocol = None
            parameters: list[mhd_domain.CvTermObject] = []
            for param_idx, x in enumerate(protocol.parameters):
                if x.term:
                    param_type = re.sub(r"\s+\d+$", "", x.term.strip()).lower()
                    selected_parameter_type = parameter_types.get(param_type)
                    if not selected_parameter_type:
                        definition_type = "x-mtbls-parameter-type"
                        param_cv = self.get_parameter_cv(protocol.name, param_type)
                        if (
                            x.term.lower()
                            in mtbls_definitions.ALL_COMMON_PROTOCOL_PARAMETERS
                            and param_cv
                        ):
                            definition_type = "parameter-type"

                        if not param_cv:
                            definition_type = self.otc.create_cv_term_object(
                                type_=definition_type,
                                name=x.term.lower(),
                                accession=x.term_accession_number or "",
                                source=x.term_source_ref or "",
                            )
                        else:
                            definition_type = self.otc.create_cv_term_object(
                                type_=definition_type,
                                name=param_cv.name,
                                accession=param_cv.accession,
                                source=param_cv.source,
                            )

                        mhd_builder.add(
                            definition_type,
                            use_label_for_invalid_cv_term=self.config.use_label_for_invalid_cv_term,
                        )
                        mhd_builder.link(
                            mhd_study,
                            "defines",
                            definition_type,
                            reverse_relationship_name="defined-in",
                        )
                        parameter_types[param_type] = definition_type
                        selected_parameter_type = definition_type
                        protocol_parameter_types[name][x.term] = selected_parameter_type

                    definition_name = f"{protocol_idx:03}/{param_idx:03}"
                    definition = mhd_domain.ParameterDefinition(
                        uri=get_urn(
                            study_id, mhd_domain.ParameterDefinition, definition_name
                        ),
                        parameter_type_ref=selected_parameter_type.id_,
                        name=x.term,
                    )
                    protocol_summary.parameters.append(x.term)
                    protocol_summary.parameter_definitions[x.term] = (
                        ParameterDefinitionSummary(
                            index=param_idx,
                            isa_parameter=x,
                            cv_term=CvTerm.model_validate(
                                x.model_dump(by_alias=True), from_attributes=True
                            ),
                            definition_node=definition,
                            definition_type=selected_parameter_type,
                        )
                    )

                    mhd_builder.link(
                        definition,
                        "has-type",
                        selected_parameter_type,
                        reverse_relationship_name="type-of",
                    )
                    mhd_builder.add(definition)
                    parameters.append(definition)
                else:
                    logger.warning(
                        "Protocol parameter '%s' in protocol '%s' does not have a term",
                        x.term,
                        protocol.name,
                    )

            protocol_type_term = re.sub(r"\s+\d+$", "", name.strip()).lower()
            protocol_type = mtbls_definitions.COMMON_PROTOCOLS_MAP.get(
                protocol_type_term
            )
            if not protocol_type:
                protocol_type = mtbls_definitions.MTBLS_PROTOCOLS_MAP.get(
                    protocol_type_term
                )
            if not protocol_type:
                protocol_type = CvTerm(name=protocol.name.lower())

            protocol_type_name = (
                "protocol-type"
                if protocol_type_term in mtbls_definitions.COMMON_PROTOCOLS_MAP
                else "x-mtbls-protocol-type"
            )
            definition_refs = None
            if parameters:
                definition_refs = [x.id_ for x in parameters]
            protocol_type_obj = protocol_types.get(protocol_type)
            if not protocol_type_obj:
                protocol_type_obj = self.otc.create_cv_term_object(
                    type_=protocol_type_name,
                    source=protocol_type.source or "",
                    accession=self.convert_to_curie(
                        protocol_type.source,
                        protocol_type.accession,
                    )
                    or "",
                    name=protocol_type.name or "",
                )
                mhd_builder.add(
                    protocol_type_obj,
                    use_label_for_invalid_cv_term=self.config.use_label_for_invalid_cv_term,
                )
                protocol_types[protocol_type] = protocol_type_obj
            mhd_protocol = mhd_domain.Protocol(
                uri=get_urn(study_id, mhd_domain.Protocol, f"{protocol_idx:03}"),
                name=name,
                protocol_type_ref=protocol_type_obj.id_,
                description=BeautifulSoup(
                    protocol.description, "html.parser"
                ).get_text(),
                parameter_definition_refs=definition_refs,
            )
            protocol_summary.protocol_node = mhd_protocol
            protocol_summary.protocol_type_node = protocol_type_obj

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

            mhd_builder.add(mhd_protocol)
            mhd_builder.link(
                mhd_study,
                "has-protocol",
                mhd_protocol,
                reverse_relationship_name="used-in",
                tag_list=[KeyValue(key=index_number_node.id_, value=protocol_idx)],
            )

    def add_keywords(self, context: DatasetBuildContext):
        data = context.data
        mhd_builder = context.mhd_builder
        mhd_study = context.study.study_node
        study = data.investigation.studies[0]
        for item in study.study_design_descriptors.design_types:
            keyword = self.otc.create_cv_term_object(
                type_="descriptor",
                source=item.term_source_ref or "",
                accession=self.convert_to_curie(
                    item.term_source_ref,
                    item.term_accession_number,
                )
                or "",
                name=item.term or "",
            )
            if not keyword:
                continue
            mhd_builder.add_node(
                keyword,
                use_label_for_invalid_cv_term=self.config.use_label_for_invalid_cv_term,
            )

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
        context: DatasetBuildContext,
    ):
        data = context.data
        mhd_builder = context.mhd_builder
        study = data.investigation.studies[0]
        assay_comments = {x.name: x for x in study.study_assays.comments}
        selected_assay_indices = {x.index for x in context.study.assays}
        assay_summaries = {x.name: x for x in context.study.assays}
        for idx, assay in enumerate(study.study_assays.assays):
            if idx in selected_assay_indices or assay.file_name not in assay_summaries:
                continue

            assay_descriptors = assay_comments.get("Assay Descriptor", [])
            assay_descriptor_term_sources = assay_comments.get(
                "Assay Descriptor Term Source REF", []
            )
            assay_descriptor_accessions = assay_comments.get(
                "Assay Descriptor Term Accession Number", []
            )
            if (
                assay_descriptors
                and isinstance(assay_descriptors.value, list)
                and idx < len(assay_descriptors.value)
            ):
                terms = assay_descriptors.value[idx]
                if not terms or not terms.strip():
                    continue
                term_sources = (
                    assay_descriptor_term_sources.value[idx]
                    if idx < len(assay_descriptor_term_sources.value)
                    else ""
                )
                accessions = (
                    assay_descriptor_accessions.value[idx]
                    if idx < len(assay_descriptor_accessions.value)
                    else ""
                )
                term_list = terms.split(";") if terms else []
                term_sources_list = term_sources.split(";") if term_sources else []
                term_accessions_list = accessions.split(";") if accessions else []

                for desc_idx, term in enumerate(term_list):
                    if not term:
                        continue
                    keyword = self.otc.create_cv_term_object(
                        type_="descriptor",
                        source=term_sources_list[desc_idx]
                        if desc_idx < len(term_sources_list)
                        else "",
                        accession=self.convert_to_curie(
                            term_sources_list[desc_idx]
                            if desc_idx < len(term_sources_list)
                            else "",
                            term_accessions_list[desc_idx]
                            if desc_idx < len(term_accessions_list)
                            else "",
                        )
                        or "",
                        name=term or "",
                    )
                    if not keyword:
                        continue
                    mhd_builder.add_node(
                        keyword,
                        use_label_for_invalid_cv_term=self.config.use_label_for_invalid_cv_term,
                    )
                    mhd_assay = assay_summaries.get(assay.file_name)
                    mhd_builder.link(
                        mhd_assay,
                        "has-submitter-keyword",
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

            zip_file_format_node = self.otc.create_cv_term_object(
                type_="descriptor",
                source="EDAM",
                accession="EDAM:format_3987",
                name="ZIP Format",
            )

        folder = False
        data_format_node = None
        if not compressed:
            if file in data.study_folder_metadata.files:
                folder = data.study_folder_metadata.files[file].is_directory
            elif file in data.study_folder_metadata.folders:
                folder = data.study_folder_metadata.folders[file].is_directory

            data_format = mtbls_definitions.FILE_EXTENSIONS.get(
                (suffix.lower(), folder)
            )
            if not data_format:
                double_extension = f"{Path(file_path.stem).suffix}{suffix}".lower()
                data_format = mtbls_definitions.FILE_EXTENSIONS.get(
                    (double_extension.lower(), folder)
                )
                if not data_format:
                    data_format = default_format

            if data_format:
                if not cv_nodes.get(data_format.accession):
                    data_format_node = self.otc.create_cv_term_object(
                        type_="descriptor",
                        accession=data_format.accession,
                        source=data_format.source,
                        name=data_format.name,
                    )
                    if data_format_node:
                        cv_nodes[data_format_node.accession] = data_format_node

                data_format_node = cv_nodes[data_format.accession]
        return zip_file_format_node, data_format_node, file_extension

    def add_data_files(self, context: DatasetBuildContext):
        study_id = context.study_id
        data = context.data
        mhd_builder = context.mhd_builder
        mhd_study = context.study.study_node
        files_map: dict[str, mhd_domain.CvTermObject] = {}
        cv_nodes: dict[str, mhd_domain.CvTermObject] = {}

        metadata_files = context.study.metadata_files
        assays_summary = {x.name: x for x in context.study.assays}
        result_files = {x.name: x for x in context.study.assignment_files}
        for assay in data.assays.values():
            assay_summary = assays_summary.get(assay.file_path)
            if not assay_summary:
                continue
            for file in assay.referenced_raw_files:
                if file in files_map:
                    continue
                compression_format, data_format, file_extension = self.find_file_format(
                    file=file,
                    data=data,
                    default_format=None,
                    cv_nodes=cv_nodes,
                )
                if not data_format and not compression_format:
                    logger.warning(
                        "File format is not found in folder metadata for study %s: %s",
                        study_id,
                        file,
                    )
                if data_format:
                    mhd_builder.add(
                        data_format,
                        use_label_for_invalid_cv_term=self.config.use_label_for_invalid_cv_term,
                    )
                if compression_format:
                    mhd_builder.add(
                        compression_format,
                        use_label_for_invalid_cv_term=self.config.use_label_for_invalid_cv_term,
                    )
                referenced_assay = metadata_files.get(assay.file_path).file_node
                file_node = mhd_domain.RawDataFile(
                    uri=get_urn(study_id, mhd_domain.RawDataFile, file),
                    name=file,
                    metadata_file_refs=[referenced_assay.id_]
                    if referenced_assay
                    else None,
                    compression_format_ref=(
                        compression_format.id_ if compression_format else None
                    ),
                    format_ref=data_format.id_ if data_format else None,
                    extension=file_extension if file_extension else None,
                    url_list=[f"{self.config.public_ftp_base_url}/{study_id}/{file}"],
                )
                summary = DataFileSummary(name=file, file_node=file_node, raw_file=True)
                assay_summary.raw_data_files.append(summary)
                if file not in context.study.raw_data_files:
                    context.study.raw_data_files[file] = summary
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
                    mhd_builder.add(
                        data_format,
                        use_label_for_invalid_cv_term=self.config.use_label_for_invalid_cv_term,
                    )
                if compression_format:
                    mhd_builder.add(
                        compression_format,
                        use_label_for_invalid_cv_term=self.config.use_label_for_invalid_cv_term,
                    )
                referenced_assay = metadata_files.get(assay.file_path).file_node

                # Metabolights accepts mzML files as Derived Data File
                # It must be converted as Raw Data File
                if (
                    file_extension
                    and file_extension.lower() in mtbls_definitions.RAW_FILE_EXCEPTIONS
                ):
                    selected_file_class = mhd_domain.RawDataFile
                else:
                    selected_file_class = mhd_domain.DerivedDataFile
                file_node = selected_file_class(
                    uri=get_urn(
                        dataset_id=study_id,
                        node_class=selected_file_class,
                        identifier=file,
                    ),
                    name=file,
                    metadata_file_refs=[referenced_assay.id_]
                    if referenced_assay
                    else None,
                    compression_format_ref=(
                        compression_format.id_ if compression_format else None
                    ),
                    format_ref=data_format.id_ if data_format else None,
                    extension=file_extension if file_extension else None,
                    url_list=[f"{self.config.public_ftp_base_url}/{study_id}/{file}"],
                )

                if selected_file_class == mhd_domain.DerivedDataFile:
                    summary = DataFileSummary(name=file, file_node=file_node)
                    assay_summary.derived_data_files.append(summary)
                    if file not in context.study.derived_data_files:
                        context.study.derived_data_files[file] = summary
                else:
                    summary = DataFileSummary(
                        name=file, file_node=file_node, raw_file=True
                    )
                    assay_summary.raw_data_files.append(summary)
                    if file not in context.study.raw_data_files:
                        context.study.raw_data_files[file] = summary
                files_map[file] = file_node
                mhd_builder.add(file_node)
                mhd_builder.link(
                    mhd_study,
                    "has-derived-data-file",
                    file_node,
                    reverse_relationship_name="created-in",
                )
        for file in data.study_folder_metadata.files or []:
            descriptor = data.study_folder_metadata.files[file]
            if not descriptor or descriptor.is_directory:
                continue
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
                if data_format:
                    mhd_builder.add(
                        data_format,
                        use_label_for_invalid_cv_term=self.config.use_label_for_invalid_cv_term,
                    )
                if compression_format:
                    mhd_builder.add(
                        compression_format,
                        use_label_for_invalid_cv_term=self.config.use_label_for_invalid_cv_term,
                    )
                file_node = mhd_domain.SupplementaryFile(
                    uri=get_urn(study_id, mhd_domain.SupplementaryFile, file),
                    name=file,
                    compression_format_ref=(
                        compression_format.id_ if compression_format else None
                    ),
                    format_ref=data_format.id_ if data_format else None,
                    extension=file_extension if file_extension else None,
                    url_list=[f"{self.config.public_ftp_base_url}/{study_id}/{file}"],
                )
                if file not in context.study.supplementary_data_files:
                    summary = DataFileSummary(name=file, file_node=file_node)
                    context.study.supplementary_data_files[file] = summary
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
        self, context: DatasetBuildContext, max_molecular_entity_limit: int
    ):
        study_id = context.study_id
        mhd_builder = context.mhd_builder
        mhd_study = context.study.study_node
        maf_table_column_descriptors = {}
        used_maf_table_column_descriptors = set()

        if len(context.study.assignment_files) < 1:
            return
        total_number_node = self.otc.create_cv_term_object(
            type_="descriptor",
            source="NCIT",
            accession="NCIT:C127787",
            name="Total Number",
        )
        mhd_builder.add(total_number_node)

        for field, term in mtbls_definitions.MAF_TABLE_COLUMN_TERMS.items():
            maf_table_column_descriptors[field] = self.otc.create_cv_term_value_object(
                type_="descriptor",
                source=term.source,
                accession=term.accession,
                name=term.name,
            )
        molecular_entity_count = 0
        max_molecular_entity_limit_in_a_file = int(
            1.0 * max_molecular_entity_limit / len(context.study.assignment_files)
        )
        for assignment_file in context.study.assignment_files:
            maf_idx = assignment_file.index
            result_file = assignment_file.file_node
            maf_file = assignment_file.isa_file_content

            if not maf_file.table.data.get("metabolite_identification"):
                continue

            count = len(
                assignment_file.isa_file_content.table.data.get(
                    "metabolite_identification", []
                )
            )
            assignment_file.file_node.tag_list = [
                KeyValue(key=total_number_node.id_, value=f"{count}")
            ]
            molecular_entity_count_in_a_file = 0
            for row_idx, name in enumerate(
                maf_file.table.data["metabolite_identification"]
            ):
                if not name or not name.strip() or len(name.strip()) < 2:
                    continue
                if molecular_entity_count >= max_molecular_entity_limit:
                    break
                if (
                    molecular_entity_count_in_a_file
                    >= max_molecular_entity_limit_in_a_file
                ):
                    break

                entity = name.strip()
                met = mhd_domain.MolecularEntity(
                    uri=get_urn(
                        study_id,
                        mhd_domain.MolecularEntity,
                        f"{maf_idx:03}/{row_idx:06}",
                    ),
                    name=entity,
                )
                table_data: dict[str, str] = maf_file.table.data
                submitted_identifiers = []
                assigned_chebi_identifiers = []
                assigned_refmet_identifiers = []
                for field in mtbls_definitions.MAF_TABLE_COLUMN_TERMS:
                    values = table_data.get(field, [])
                    selected_value = None
                    if values and len(values) > row_idx:
                        selected_value = values[row_idx] or None

                    if selected_value:
                        descriptor_node = maf_table_column_descriptors[field]
                        used_maf_table_column_descriptors.add(field)
                        tag = KeyValue(key=descriptor_node.id_, value=selected_value)
                        if not met.tag_list:
                            met.tag_list = [tag]
                        else:
                            met.tag_list.append(tag)

                if table_data.get("database_identifier"):
                    submitted_identifiers = [
                        x.strip()
                        for x in table_data["database_identifier"][row_idx].split("|")
                        if x
                    ]
                if table_data.get("assigned_chebi_identifier"):
                    assigned_chebi_identifiers = [
                        x.strip()
                        for x in table_data["assigned_chebi_identifier"][row_idx].split(
                            "|"
                        )
                        if x
                    ]
                if table_data.get("assigned_refmet_identifier"):
                    assigned_refmet_identifiers = [
                        x.strip()
                        for x in table_data["assigned_refmet_identifier"][
                            row_idx
                        ].split("|")
                        if x
                    ]

                for identifiers, compound_source in [
                    (submitted_identifiers, ""),
                    (assigned_chebi_identifiers, "chebi"),
                    (assigned_refmet_identifiers, "refmet"),
                ]:
                    if not identifiers:
                        continue
                    for identifier_value in identifiers:
                        identifier = None
                        if (
                            compound_source == "chebi"
                            or identifier_value.lower().startswith("chebi")
                        ):
                            identifier_type = COMMON_MOLECULAR_ENTITY_IDENTIFIERS[
                                "chebi"
                            ]
                            identifier = self.otc.create_cv_term_value_object(
                                type_="molecular-entity-identifier",
                                source=identifier_type.source,
                                accession=identifier_type.accession,
                                name=identifier_type.name,
                                value=identifier_value,
                            )
                        elif identifier_value.lower().startswith("hmdb"):
                            identifier = self.otc.create_cv_term_value_object(
                                type_="molecular-entity-identifier",
                                source="EDAM",
                                accession="EDAM:data_2622",
                                name="Compound ID (HMDB)",
                                value=identifier_value,
                            )
                        elif compound_source == "refmet":
                            identifier_type = COMMON_MOLECULAR_ENTITY_IDENTIFIERS[
                                "refmet"
                            ]
                            identifier = self.otc.create_cv_term_value_object(
                                type_="molecular-entity-identifier",
                                source=identifier_type.source,
                                accession=identifier_type.accession,
                                name=identifier_type.name,
                                value=identifier_value,
                            )

                        if identifier:
                            mhd_builder.add(
                                identifier,
                                use_label_for_invalid_cv_term=self.config.use_label_for_invalid_cv_term,
                            )
                            mhd_builder.link(
                                met,
                                "identified-as",
                                identifier,
                                reverse_relationship_name="reported-identifier-of",
                            )
                mhd_builder.add(met)
                molecular_entity_count += 1
                molecular_entity_count_in_a_file += 1

                if result_file:
                    mhd_builder.link(
                        result_file,
                        "reports",
                        met,
                        reverse_relationship_name="reported-in",
                    )
                mhd_builder.link(
                    mhd_study,
                    "reports",
                    met,
                    reverse_relationship_name="reported-in",
                )

        for field in used_maf_table_column_descriptors:
            node = maf_table_column_descriptors.get(field)
            if node:
                context.mhd_builder.add(node)

    def add_assays(self, context: DatasetBuildContext) -> dict[str, mhd_domain.Assay]:
        study_id = context.study_id
        data = context.data
        mhd_builder = context.mhd_builder
        mhd_study = context.study.study_node

        assays: OrderedDict[str, AssaySummary] = OrderedDict()
        metadata_files = context.study.metadata_files
        for assay_summary in context.study.assays:
            assay_idx = assay_summary.index
            metadata_file = metadata_files.get(assay_summary.name)
            mhd_assay = mhd_domain.Assay(
                name=assay_summary.name,
                uri=get_urn(study_id, mhd_domain.Assay, f"000/{assay_idx:03}"),
                metadata_file_ref=metadata_file.file_node.id_
                if metadata_file
                else None,
            )
            mhd_builder.add(mhd_assay)
            assay_summary.assay_node = mhd_assay
            assays[assay_summary.name] = assay_summary
            mhd_builder.link(
                mhd_study, "has-assay", mhd_assay, reverse_relationship_name="part-of"
            )
            if (
                assay_summary.isa_assay.technology_type.term.lower()
                in mtbls_definitions.MTBLS_TECHNOLOGY_TYPES
            ):
                technology_type_cv = mtbls_definitions.MTBLS_TECHNOLOGY_TYPES[
                    assay_summary.isa_assay.technology_type.term.lower()
                ]
                technology_type = self.otc.create_cv_term_object(
                    type_="descriptor",
                    source=technology_type_cv.source,
                    accession=technology_type_cv.accession,
                    name=technology_type_cv.name,
                )
            else:
                technology_type = self.otc.create_cv_term_object(
                    type_="descriptor",
                    source=assay_summary.isa_assay.technology_type.term_source_ref,
                    accession=self.convert_to_curie(
                        assay_summary.isa_assay.technology_type.term_source_ref,
                        assay_summary.isa_assay.technology_type.term_accession_number,
                    ),
                    name=assay_summary.isa_assay.technology_type.term,
                )

            mhd_builder.add(
                technology_type,
                use_label_for_invalid_cv_term=self.config.use_label_for_invalid_cv_term,
            )
            mhd_assay.technology_type_ref = technology_type.id_
            assay_file: AssayFile = metadata_file.isa_file_content
            assay_type_cv = None
            if assay_file.assay_technique.name in mtbls_definitions.MTBLS_ASSAY_TYPES:
                assay_type_cv = mtbls_definitions.MTBLS_ASSAY_TYPES[
                    assay_file.assay_technique.name
                ]
            if assay_type_cv:
                assay_type = self.otc.create_cv_term_object(
                    type_="descriptor",
                    source=assay_type_cv.source,
                    accession=assay_type_cv.accession,
                    name=assay_type_cv.name,
                )
                mhd_builder.add(
                    assay_type,
                    use_label_for_invalid_cv_term=self.config.use_label_for_invalid_cv_term,
                )
                mhd_assay.assay_type_ref = assay_type.id_

            omics_types: list[mhd_domain.CvTermObject] = []
            measurement_types: list[mhd_domain.CvTermObject] = []
            measurement_type = None
            if "untargeted" in assay_summary.isa_assay.measurement_type.term.lower():
                measurement_type = mtbls_definitions.MTBLS_MEASUREMENT_TYPES[
                    "untargeted"
                ]
            elif (
                "semi-targeted" in assay_summary.isa_assay.measurement_type.term.lower()
            ):
                measurement_type = mtbls_definitions.MTBLS_MEASUREMENT_TYPES[
                    "semi-targeted"
                ]
            elif "targeted" in assay_summary.isa_assay.measurement_type.term.lower():
                measurement_type = mtbls_definitions.MTBLS_MEASUREMENT_TYPES["targeted"]

            inv_study = data.investigation.studies[0]
            assay_omics_type = None
            for comment in inv_study.study_assays.comments:
                if (
                    comment.name.lower() == "omics type"
                    and len(comment.value) > assay_idx
                ):
                    assay_omics_name = comment.value[assay_idx]
                    v = COMMON_OMICS_TYPES.get(assay_omics_name.lower())
                    if v:
                        assay_omics_type = self.otc.create_cv_term_object(
                            type_="descriptor",
                            source=v.source,
                            accession=v.accession,
                            name=v.name,
                        )
                    break
            design_types = inv_study.study_design_descriptors.design_types

            for descriptor in design_types:
                if not measurement_type:
                    item = None
                    if "untargeted" in descriptor.term.lower():
                        item = mtbls_definitions.MTBLS_MEASUREMENT_TYPES["untargeted"]
                    if "semi-targeted" in descriptor.term.lower():
                        item = mtbls_definitions.MTBLS_MEASUREMENT_TYPES[
                            "semi-targeted"
                        ]
                    elif "targeted" in descriptor.term.lower():
                        item = mtbls_definitions.MTBLS_MEASUREMENT_TYPES["targeted"]

                    if item:
                        measurement_types.append(item)
                if not assay_omics_type:
                    for v in COMMON_OMICS_TYPES.values():
                        if descriptor.term.lower() == v.name.lower():
                            omics_types.append(v)
            selected_measurement_type = None
            if measurement_type:
                selected_measurement_type = measurement_type
            elif len(measurement_types) == 1:
                selected_measurement_type = measurement_types[0]
            else:
                selected_measurement_type = mtbls_definitions.DEFAULT_MEASUREMENT_TYPE

            measurement_type = self.otc.create_cv_term_object(
                type_="descriptor",
                source=selected_measurement_type.source,
                accession=selected_measurement_type.accession,
                name=selected_measurement_type.name,
            )
            mhd_builder.add(
                measurement_type,
                use_label_for_invalid_cv_term=self.config.use_label_for_invalid_cv_term,
            )
            mhd_assay.measurement_type_ref = measurement_type.id_

            if assay_omics_type:
                selected_omics_type = assay_omics_type
            elif len(omics_types) == 1:
                selected_omics_type = omics_types[0]
            else:
                selected_omics_type = mtbls_definitions.DEFAULT_OMICS_TYPE
            omics_type = self.otc.create_cv_term_object(
                type_="descriptor",
                source=selected_omics_type.source,
                accession=selected_omics_type.accession,
                name=selected_omics_type.name,
            )
            mhd_builder.add(
                omics_type,
                use_label_for_invalid_cv_term=self.config.use_label_for_invalid_cv_term,
            )
            mhd_assay.omics_type_ref = omics_type.id_

            self.add_sample_runs(context, assay_summary)
        return assays

    def convert_str_to_datetime(self, val: str):
        if not val or len(val) < 10:
            return None
        if len(val) >= 10:
            return datetime.datetime.strptime(val[:10], "%Y-%m-%d")

        return datetime.datetime.strptime(val, "%Y-%m-%d")

    def build(
        self,
        dataset_class: type[MhDatasetBaseProfile_v1_0],
        mhd_id: None | str,
        mhd_output_folder_path: Path,
        mtbls_study_id: str,
        mtbls_study_path: Path,
        mtbls_study_repository_urls: list[str],
        target_mhd_model_schema_uri: str,
        target_mhd_model_profile_uri: str,
        repository_name: str,
        cached_mtbls_model_file_path: None | Path = None,
        revision: None | Revision = None,
        metabolights_study_model: None | MetabolightsStudyModel = None,
        cv_term_helper: None | BaseCvTermHelper = None,
        max_molecular_entity_limit: None | int = 10000,
        **kwargs,
    ) -> InstanceOf[BaseMhdDataset]:
        mhd_output_filename = kwargs.get("mhd_output_filename", None)
        dataset_provider = self.otc.create_cv_term_value_object(
            type_="data-provider",
            source="NCIT",
            accession="NCIT:C189151",
            name="Study Data Repository",
            value=repository_name,
        )
        revisions: list[Revision] = []
        current_revision: None | Revision = None
        if metabolights_study_model:
            data = metabolights_study_model
        elif cached_mtbls_model_file_path and cached_mtbls_model_file_path.exists():
            with cached_mtbls_model_file_path.open("r") as fr:
                data: MetabolightsStudyModel = (
                    MetabolightsStudyModel.model_validate_json(
                        cached_mtbls_model_file_path.read_text()
                    )
                )
        else:
            connection = None
            try:
                connection = create_postgresql_connection(self.config)
                db_collector = DbMetadataCollector(self.config)
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
                cursor = connection.cursor()
                cursor.execute(
                    "select * from study_revisions "
                    "where accession_number = %(study_id)s "
                    "order by revision_number desc;",
                    {"study_id": mtbls_study_id},
                )
                results = cursor.fetchall()
                requested_revision = data.study_db_metadata.revision_number
                for _, item in enumerate(results):
                    revision_no = item["revision_number"]
                    if revision_no > requested_revision:
                        continue
                    rev = Revision(
                        revision=item["revision_number"],
                        comment=item["revision_comment"],
                        revision_datetime=item["revision_datetime"],
                    )
                    if revision_no == data.study_db_metadata.revision_number:
                        current_revision = rev
                    revisions.append(rev)
            finally:
                if connection:
                    connection.close()
            update_submitter_user_from_keycloak(data, self.config)
            if cached_mtbls_model_file_path:
                with cached_mtbls_model_file_path.open("w") as fr:
                    json.dump(data.model_dump(by_alias=True), fr)

        if not data.investigation.studies:
            error = f"{data.investigation_file_path} file does not have any study. Skipping..."
            logger.warning(error)
            return False, error
        db_metadata = data.study_db_metadata
        selected_assays: list[tuple[int, investigation_file.Assay]] = []
        selected_assignment_files: dict[str, list[str]] = {}
        study = data.investigation.studies[0]

        if data.study_db_metadata.study_id != study.identifier:
            logger.warning(
                "Study id is not valid in metadata file: Actual: %s, Expected: %s",
                study.identifier,
                data.study_db_metadata.study_id,
            )
        unsupported_assays = []
        for idx, assay in enumerate(study.study_assays.assays):
            if assay.file_name in data.assays:
                assay_file = data.assays[assay.file_name]
                selected_assignment_files[assay.file_name] = (
                    assay_file.referenced_assignment_files
                )
                if (
                    assay_file.assay_technique.name
                    in mtbls_definitions.MTBLS_ASSAY_TYPES
                ):
                    selected_assays.append((idx, assay))
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
        mhd_id = study.mhd_accession or mhd_id
        mhd_id = mhd_id if mhd_id and mhd_id.startswith("MHD") else None
        identifier = mhd_id if mhd_id else study.identifier
        now = datetime.datetime.now(datetime.UTC)
        dataset = dataset_class(
            uri=get_urn(study.identifier, dataset_class, None),
            repository_short_name="MTBLS",
            created_at=now,
            repository_name=repository_name,
            name=f"{identifier}: {study.title}",
            description=f"{repository_name} dataset with {study.identifier} accession",
            mhd_identifier=mhd_id,
            repository_identifier=study.identifier,
            schema_name=target_mhd_model_schema_uri,
            profile_uri=target_mhd_model_profile_uri,
            repository_revision=current_revision.revision if current_revision else 0,
            repository_revision_datetime=current_revision.revision_datetime
            if current_revision
            else None,
            repository_revision_comment=current_revision.comment
            if current_revision
            else None,
            change_log=revisions if revisions else None,
        )

        mhd_builder = MhDatasetBuilder(dataset=dataset, cv_term_helper=cv_term_helper)

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
        submission_date = None
        public_release_date = None
        if db_metadata:
            if db_metadata.first_private_date:
                submission_date = self.convert_str_to_datetime(
                    db_metadata.first_private_date
                )
            elif db_metadata.submission_date:
                submission_date = self.convert_str_to_datetime(
                    db_metadata.submission_date
                )
            if db_metadata.first_public_date:
                public_release_date = self.convert_str_to_datetime(
                    db_metadata.first_public_date
                )
            elif db_metadata.release_date:
                public_release_date = self.convert_str_to_datetime(
                    db_metadata.release_date
                )

        mhd_study = mhd_domain.Study(
            uri=get_urn(study.identifier, mhd_domain.Study, "000"),
            repository_identifier=study.identifier,
            created_by_ref=dataset_provider.id_,
            mhd_identifier=mhd_id if mhd_id and mhd_id.startswith("MHD") else None,
            title=study.title,
            description=BeautifulSoup(study.description, "html.parser").get_text(),
            submission_date=submission_date,
            public_release_date=public_release_date,
            url_list=mtbls_study_repository_urls or None,
        )
        dataset.url_list = mtbls_study_repository_urls or None
        mhd_builder.add(mhd_study)
        mhd_builder.add_node(
            dataset_provider,
            use_label_for_invalid_cv_term=self.config.use_label_for_invalid_cv_term,
        )
        mhd_builder.link(
            dataset_provider,
            "provides",
            mhd_study,
            reverse_relationship_name="provided-by",
        )
        study_summary = StudySummary(study_node=mhd_study, isa_study=study)
        assignment_files_set = set()
        for x in selected_assignment_files.values():
            assignment_files_set.update(x)
        assignment_file_indices = {
            v: k for k, v in enumerate(sorted(assignment_files_set))
        }

        assignment_files = {
            x: AssignmentFileSummary(
                index=assignment_file_indices.get(x),
                name=x,
                isa_file_content=data.metabolite_assignments.get(x),
            )
            for x in assignment_files_set
        }
        study_summary.assignment_files = sorted(
            assignment_files.values(), key=lambda x: x.index
        )
        for idx, assay in selected_assays:
            study_summary.assays.append(
                AssaySummary(
                    index=idx,
                    isa_assay=assay,
                    name=assay.file_name,
                    isa_file_content=data.assays.get(assay.file_name),
                    assignment_files=[
                        x
                        for x in study_summary.assignment_files
                        if x.name in selected_assignment_files[assay.file_name]
                    ],
                )
            )
        context = DatasetBuildContext(
            study_id=mhd_study.repository_identifier,
            study=study_summary,
            mhd_builder=mhd_builder,
            data=data,
        )
        organizations = self.add_contacts(context)
        self.add_funders(context, organizations)

        self.add_metadata_files(context)

        self.add_characteristic_definitions(context)

        self.add_study_factor_definitions(context)
        self.add_samples(context)
        mhd_study.license = data.study_db_metadata.dataset_license_url
        mhd_study.license_name = data.study_db_metadata.dataset_license
        if not mhd_study.license:
            mhd_study.license = HttpUrl(self.config.default_dataset_licence_url) or None
        dataset.license = mhd_study.license
        dataset.license_name = mhd_study.license_name
        if self.config.build_type in (BuildType.FULL, BuildType.FULL_AND_CUSTOM_NODES):
            self.add_publications(context)
            self.add_protocols(context)

            self.add_result_files(context)
            self.add_reported_metabolites(context, max_molecular_entity_limit)

            self.add_data_files(context)
            self.add_assays(context)
            self.add_keywords(context)
            self.add_assay_keywords(context)

        mhd_dataset: BaseMhDatasetProfile = mhd_builder.build_dataset(
            start_item_refs=[mhd_study.id_]
        )
        # mhd_dataset.created_by_ref = dataset_provider.id_
        filename = study.identifier

        output_path = resolve_output_file_path(
            mhd_output_folder_path, f"{filename}.mhd.json"
        )
        if mhd_output_filename:
            output_path = resolve_output_file_path(
                mhd_output_folder_path, mhd_output_filename
            )
        output_path.write_text(
            mhd_dataset.model_dump_json(
                indent=2, by_alias=True, exclude_none=True, serialize_as_any=True
            )
        )
        logger.info(
            "%s study MHD file is created with name: %s", mtbls_study_id, output_path
        )
        return True, str(output_path)
