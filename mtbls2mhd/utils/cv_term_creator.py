import logging

from mhd_model.model.v0_1.dataset.profiles.base import graph_nodes as mhd_domain
from mhd_model.shared.model import CvTerm, UnitCvTerm
from mhd_model.shared.validation.cv_term_helper import (
    CvTermHelper,
)

logger = logging.getLogger(__name__)


class OntologyCacheService:
    def get_cv_term_by_accession(self, source: str, accession: str) -> CvTerm | None:
        return None

    def get_cv_term_by_source_and_term(self, source: str, term: str) -> CvTerm | None:
        return None


class OntologyTermCreator:
    def __init__(
        self,
        cache_service: None | OntologyCacheService = None,
        cv_term_helper: None | CvTermHelper = None,
        default_terms: None | dict[str, CvTerm] = None,
    ):
        self.cache_service = cache_service
        self.cv_term_helper = cv_term_helper
        if not self.cache_service:
            self.cache_service = OntologyCacheService()
        if not self.cv_term_helper:
            self.cv_term_helper = CvTermHelper()
        self.default_terms = default_terms

    def create_cv_term_object(
        self,
        type_: str,
        accession: str,
        source: str,
        name: str,
    ) -> mhd_domain.CvTermObject:
        return create_cv_term_node(
            cv_term_helper=self.cv_term_helper,
            type_=type_,
            accession=accession,
            source=source,
            name=name,
            cache_service=self.cache_service,
            default_terms=self.default_terms,
        )

    def create_cv_term_value_object(
        self,
        type_: str,
        accession: str = "",
        source: str = "",
        name: str = "",
        value: None | str = None,
        unit: None | UnitCvTerm = None,
    ) -> mhd_domain.CvTermValueObject:
        return create_cv_term_value_node(
            cv_term_helper=self.cv_term_helper,
            type_=type_,
            accession=accession,
            source=source,
            name=name,
            value=value,
            unit=unit,
            cache_service=self.cache_service,
            default_terms=self.default_terms,
        )


def create_cv_term_node(
    cv_term_helper: CvTermHelper,
    type_: str,
    accession: str,
    source: str,
    name: str,
    cache_service: None | OntologyCacheService = None,
    default_terms: None | dict[str, CvTerm] = None,
) -> mhd_domain.CvTermObject:
    name = name or ""
    accession = accession or ""
    source = source or ""
    accession_key = accession.lower()
    if accession_key and source:
        predefined_cv_term = None
        if default_terms:
            predefined_cv_term = default_terms.get(accession_key)
            if (
                predefined_cv_term
                and predefined_cv_term.name.lower() == name.lower()
                and predefined_cv_term.source.lower() == source.lower()
            ):
                return mhd_domain.CvTermObject(
                    type_=type_,
                    accession=predefined_cv_term.accession,
                    source=predefined_cv_term.source,
                    name=predefined_cv_term.name,
                )
        if cache_service:
            predefined_cv_term = cache_service.get_cv_term_by_accession(
                source, accession
            )
            if (
                predefined_cv_term
                and predefined_cv_term.name.lower() == name.lower()
                and predefined_cv_term.source.lower() == source.lower()
            ):
                return mhd_domain.CvTermObject(
                    type_=type_,
                    accession=predefined_cv_term.accession,
                    source=predefined_cv_term.source,
                    name=predefined_cv_term.name,
                )
    if accession and accession.lower().startswith("mtbls"):
        accession = ""
    if source and source.lower().startswith("mtbls"):
        source = ""
    if not source or not accession:
        return mhd_domain.CvTermObject(type_=type_, name=name)
    default_cv_term = CvTerm(name=name, accession=accession, source=source)
    search_accession = cv_term_helper.get_uri(default_cv_term)
    s_term = cv_term_helper.find_cv_term(
        source, name or search_accession, allow_synonym_search=True
    )
    if s_term and s_term.accession.lower() == accession.lower():
        return mhd_domain.CvTermObject(
            type_=type_,
            accession=s_term.accession,
            source=s_term.source,
            name=s_term.name,
        )
    return mhd_domain.CvTermObject(type_=type_, name=name)


def create_cv_term_value_node(
    cv_term_helper: CvTermHelper,
    type_: str,
    accession: str = "",
    source: str = "",
    name: str = "",
    value: None | str = None,
    unit: None | UnitCvTerm = None,
    cache_service: None | OntologyCacheService = None,
    default_terms: None | dict[str, CvTerm] = None,
) -> mhd_domain.CvTermValueObject:
    # Remove accession and source if they start with MTBLS.
    # they are placeholders and do not have meaning outside of MTBLS context.

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
            unit_cv = UnitCvTerm(name=unit.name) if unit and unit.name else None
    if unit_cv:
        if not unit_cv.accession or not unit_cv.source:
            unit_cv = UnitCvTerm(type_=type_, name=unit.name)
        else:
            default_cv_term = CvTerm(
                name=unit_cv.name or "",
                accession=unit_cv.accession,
                source=unit_cv.source,
            )
            search_accession = cv_term_helper.get_uri(default_cv_term)

            s_unit = None
            if cache_service:
                cache_val = cache_service.get_cv_term_by_accession(
                    unit_cv.source, unit_cv.accession
                )
                if cache_val and cache_val.name.lower() == unit_cv.name.lower():
                    s_unit = cache_val
            if not s_unit:
                s_unit = cv_term_helper.find_cv_term(
                    source, unit_cv.name or search_accession, allow_synonym_search=True
                )
            if (
                s_unit
                and unit_cv.name.lower() == s_unit.name.lower()
                and unit_cv.source.lower() == s_unit.source.lower()
            ):
                unit_cv = UnitCvTerm(
                    type_=type_,
                    name=s_unit.name,
                    accession=s_unit.accession,
                    source=s_unit.source,
                )
            else:
                logger.warning(
                    "CV term '%s' with source '%s' and accession '%s' not found. "
                    "CV term will be created without source and accession.",
                    name,
                    source,
                    accession,
                )
                unit_cv = UnitCvTerm(type_=type_, name=unit.name)

    if not source or not accession:
        return mhd_domain.CvTermValueObject(
            type_=type_,
            name=name,
            value=value,
            unit=unit_cv,
        )

    if source:
        accession_key = accession.lower() if accession else ""
        if source and accession_key:
            predefined_cv_term = None
            if default_terms:
                predefined_cv_term = default_terms.get(accession_key)

                if not predefined_cv_term and cache_service:
                    predefined_cv_term = cache_service.get_cv_term_by_accession(
                        source, accession
                    )
                if (
                    predefined_cv_term
                    and predefined_cv_term.name.lower() == name.lower()
                    and predefined_cv_term.source.lower() == source.lower()
                ):
                    return mhd_domain.CvTermValueObject(
                        type_=type_,
                        accession=predefined_cv_term.accession,
                        source=predefined_cv_term.source,
                        name=predefined_cv_term.name,
                        value=value,
                        unit=unit_cv,
                    )

        default_cv_term = CvTerm(name=name, accession=accession, source=source)
        search_accession = cv_term_helper.get_uri(default_cv_term)

        s_term = None
        if cache_service:
            s_term = cache_service.get_cv_term_by_accession(source, accession)
        if not s_term:
            s_term = cv_term_helper.find_cv_term(
                source, name or search_accession, allow_synonym_search=True
            )
        if s_term and s_term.accession.lower() == accession.lower():
            return mhd_domain.CvTermValueObject(
                type_=type_,
                accession=s_term.accession,
                source=s_term.source,
                name=s_term.name,
                value=value,
                unit=unit_cv,
            )

    if source or accession:
        logger.warning(
            "CV term '%s' with source '%s' and accession '%s' not found. "
            "CV term will be created without source and accession.",
            name,
            source,
            accession,
        )
    return mhd_domain.CvTermValueObject(
        type_=type_,
        name=name,
        value=value,
        unit=unit_cv,
    )
