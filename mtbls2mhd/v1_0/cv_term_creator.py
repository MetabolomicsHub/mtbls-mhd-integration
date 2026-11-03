import logging

from mhd_model.cv_mapping.cv_term_suggestion import CvTermSuggestion
from mhd_model.model.v1_0.dataset.profiles.base import graph_nodes as mhd_domain
from mhd_model.shared.base import CvTerm, UnitCvTerm

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
        cv_term_suggestion: None | CvTermSuggestion = None,
    ):
        self.cache_service = cache_service
        if not self.cache_service:
            self.cache_service = OntologyCacheService()
        self.cv_term_suggestion = cv_term_suggestion

    def create_cv_term_object(
        self,
        type_: str,
        accession: str,
        source: str,
        name: str,
        group: str = "",
    ) -> mhd_domain.CvTermObject:
        name = name or ""
        accession = accession or ""
        source = source or ""

        match = self.cv_term_suggestion.match_cv_term(
            cv_term=CvTerm(accession=accession, source=source, name=name),
            group=group,
        )
        if match:
            return mhd_domain.CvTermObject(
                type_=type_,
                accession=match.accession,
                source=match.source,
                name=match.name,
            )
        else:
            return mhd_domain.CvTermObject(type_=type_, name=name)

    def create_cv_term_value_object(
        self,
        type_: str,
        accession: str = "",
        source: str = "",
        name: str = "",
        value: None | str = None,
        unit: None | UnitCvTerm = None,
        group: None | str = None,
        unit_group: None | str = None,
    ) -> None | mhd_domain.CvTermValueObject:
        name = name or ""
        accession = accession or ""
        source = source or ""
        value = value or None
        if not name and not source and not accession and not value:
            return None
        if unit:
            match = self.cv_term_suggestion.match_cv_term(
                cv_term=CvTerm(
                    accession=unit.accession, source=unit.source, name=unit.name
                ),
                group=unit_group,
            )
            if match:
                unit = UnitCvTerm(**match.model_dump(by_alias=True))
            else:
                unit.accession = ""
                unit.source = ""
            if not unit.name:
                unit = None
        term_match = self.cv_term_suggestion.match_cv_term(
            cv_term=CvTerm(accession=accession, source=source, name=name),
            group=group,
        )
        if term_match:
            name = term_match.name
            source = term_match.source
            accession = term_match.accession
        else:
            source = ""
            accession = ""

        return mhd_domain.CvTermValueObject(
            type_=type_,
            name=name,
            source=source,
            accession=accession,
            value=value,
            unit=unit,
        )
