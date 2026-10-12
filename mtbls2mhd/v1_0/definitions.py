import csv
import logging
from importlib import resources

from cachetools import TTLCache, cached
from mhd_model.model.v1_0.rules.managed_cv_terms import (
    COMMON_ASSAY_TYPES,
    COMMON_CHARACTERISTIC_DEFINITIONS,
    COMMON_MEASUREMENT_TYPES,
    COMMON_OMICS_TYPES,
    COMMON_PARAMETER_DEFINITIONS,
    COMMON_PROTOCOLS,
    COMMON_STUDY_FACTOR_DEFINITIONS,
    COMMON_TECHNOLOGY_TYPES,
    PREDEFINED_CV_TERMS,
)
from mhd_model.shared.base import CvTerm

import mtbls2mhd

logger = logging.getLogger(__name__)

REPOSITORY_NAME = "MetaboLights"
RAW_FILE_EXCEPTIONS = {".mzml", ".imzml", ".mzmlb", ".mzxml", ".cdf"}


## MTBLS RELATED CONFIGURATION ###
##############################################################################################################
MTBLS_ASSAY_TYPES = {
    "LC-MS": COMMON_ASSAY_TYPES["lc-ms"],
    "GC-MS": COMMON_ASSAY_TYPES["gc-ms"],
    "CE-MS": COMMON_ASSAY_TYPES["ce-ms"],
    "GCxGC-MS": COMMON_ASSAY_TYPES["gc-ms"],
    "FIA-MS": COMMON_ASSAY_TYPES["ms"],
    "MALDI-MS": COMMON_ASSAY_TYPES["ms"],
    "DI-MS": COMMON_ASSAY_TYPES["ms"],
    "MS": COMMON_ASSAY_TYPES["ms"],
}
MTBLS_MEASUREMENT_TYPES = {
    "targeted": COMMON_MEASUREMENT_TYPES["targeted"],
    "untargeted": COMMON_MEASUREMENT_TYPES["untargeted"],
    "semi-targeted": COMMON_MEASUREMENT_TYPES["semi-targeted"],
}

MTBLS_TECHNOLOGY_TYPES = {
    "mass spectrometry": COMMON_TECHNOLOGY_TYPES["mass spectrometry assay"],
    "mass spectrometry assay": COMMON_TECHNOLOGY_TYPES["mass spectrometry assay"],
}

DEFAULT_MEASUREMENT_TYPE = COMMON_MEASUREMENT_TYPES["untargeted"]

DEFAULT_OMICS_TYPE = COMMON_OMICS_TYPES["metabolomics"]

COMMON_PROTOCOLS_MAP = {
    "mass spectrometry": COMMON_PROTOCOLS["mass spectrometry"],
    "chromatography": COMMON_PROTOCOLS["chromatography"],
    "sample collection": COMMON_PROTOCOLS["sample collection"],
    "extraction": COMMON_PROTOCOLS["sample preparation"],
    "treatment": COMMON_PROTOCOLS["treatment"],
}

MTBLS_PROTOCOLS_MAP = COMMON_PROTOCOLS_MAP.copy()

MTBLS_PROTOCOLS_MAP.update(
    {
        "direct infusion": CvTerm(
            source="MS",
            accession="MS:1000060",
            name="infusion",
        ),
        "data transformation": CvTerm(
            source="OBI",
            accession="OBI:0200000",
            name="data transformation",
        ),
        "metabolite identification": CvTerm(
            source="EDAM",
            accession="EDAM:operation_4074",
            name="Compound identification",
        ),
        "flow injection analysis": CvTerm(
            source="CHMO",
            accession="CHMO:0002891",
            name="flow-injection analysis",
        ),
        "capillary electrophoresis": CvTerm(
            source="CHMO",
            accession="CHMO:0001024",
            name="capillary electrophoresis",
        ),
    }
)
MANAGED_CHARACTERISTICS_MAP = {
    "organism": COMMON_CHARACTERISTIC_DEFINITIONS["organism"],
    "organism part": COMMON_CHARACTERISTIC_DEFINITIONS["organism part"],
    "disease": COMMON_CHARACTERISTIC_DEFINITIONS["disease"],
    "cell type": COMMON_CHARACTERISTIC_DEFINITIONS["cell type"],
    # "geographic location": COMMON_CHARACTERISTIC_DEFINITIONS["GAZ:00000448"],
}

MTBLS_CHARACTERISTICS_MAP: dict[str, CvTerm] = MANAGED_CHARACTERISTICS_MAP.copy()
MTBLS_CHARACTERISTICS_MAP.update(
    {
        "sample type": CvTerm(
            source="NCIT", accession="NCIT:C210102", name="Sample Type"
        ),
        "variant": CvTerm(source="PATO", accession="PATO:0001227", name="variant"),
        "phenotype": CvTerm(source="EFO", accession="EFO:0000651", name="phenotype"),
        "cell line": CvTerm(source="CLO", accession="CLO:0000031", name="cell line"),
    }
)

MANAGED_STUDY_FACTOR_MAP = {
    "disease": COMMON_STUDY_FACTOR_DEFINITIONS["disease"],
    "treatment": CvTerm(source="EFO", accession="EFO:0000727", name="treatment"),
}
MTBLS_STUDY_FACTOR_MAP = MANAGED_STUDY_FACTOR_MAP.copy()
MTBLS_STUDY_FACTOR_MAP.update({})


COMMON_PROTOCOL_PARAMETER_VALUE_MAP = {
    "mass spectrometry": {
        "instrument": COMMON_PARAMETER_DEFINITIONS["mass spectrometry instrument"],
        "scan polarity": COMMON_PARAMETER_DEFINITIONS["acquisition polarity"],
        "ion source": COMMON_PARAMETER_DEFINITIONS["ionization type"],
        "mass analyzer": COMMON_PARAMETER_DEFINITIONS["instrument class"],
        # "Inlet type": COMMON_PARAMETER_DEFINITIONS["inlet type"],
    },
    "chromatography": {
        "chromatography instrument": COMMON_PARAMETER_DEFINITIONS[
            "chromatography instrument"
        ],
        "column model": COMMON_PARAMETER_DEFINITIONS["chromatography column"],
        "column type": COMMON_PARAMETER_DEFINITIONS["chromatography separation"],
        # "Solvent": COMMON_PARAMETER_DEFINITIONS["solvent"],
        # "Chromatographic additive": COMMON_PARAMETER_DEFINITIONS["chromatographic additive"],
    },
    "capillary electrophoresis": {
        "column model": COMMON_PARAMETER_DEFINITIONS["chromatography column"],
        "column type": COMMON_PARAMETER_DEFINITIONS["chromatography separation"],
        # "Solvent": COMMON_PARAMETER_DEFINITIONS["solvent"],
        # "Chromatographic additive": COMMON_PARAMETER_DEFINITIONS["chromatographic additive"],
    },
}
ALL_COMMON_PROTOCOL_PARAMETERS = {}
for items in COMMON_PROTOCOL_PARAMETER_VALUE_MAP.values():
    for name, term in items.items():
        ALL_COMMON_PROTOCOL_PARAMETERS[name] = term

MTBLS_PROTOCOL_PARAMETER_DEFINITION_MAP: dict[str, dict[str, CvTerm]] = {}

MTBLS_PROTOCOL_PARAMETER_DEFINITION_MAP.update(
    {
        "mass spectrometry": {
            **COMMON_PROTOCOL_PARAMETER_VALUE_MAP["mass spectrometry"],
            # "scan m/z range": COMMON_PARAMETER_DEFINITIONS["MTBLS:50020"],
            # "fia instrument": COMMON_PARAMETER_DEFINITIONS["MTBLS:50021"],
        },
        "chromatography": {
            **COMMON_PROTOCOL_PARAMETER_VALUE_MAP["chromatography"],
            # "guard column": COMMON_PARAMETER_DEFINITIONS["MTBLS:50003"],
            # "autosampler model": COMMON_PARAMETER_DEFINITIONS["MTBLS:50004"],
        },
        "capillary electrophoresis": {
            "ce instrument": CvTerm(
                source="OBI",
                accession="OBI:0001132",
                name="capillary electrophoresis instrument",
            ),
            **COMMON_PROTOCOL_PARAMETER_VALUE_MAP["capillary electrophoresis"],
        },
        "extraction": {
            # "post extraction": COMMON_PARAMETER_DEFINITIONS["MTBLS:50010"],
            # "derivatization": COMMON_PARAMETER_DEFINITIONS["MTBLS:50011"],
        },
    }
)


FILE_EXTENSIONS: dict[tuple[str, bool], CvTerm] = {
    # --- Raw file formats ---
    # baf, cmp, d, dat, fid, hr, ibd, jpf, lcd, ms, peg, qgd, raw, ser,
    # wiff + wiff.scan, smp
    (".baf", False): CvTerm(
        source="MS", accession="MS:1000815", name="Bruker BAF format"
    ),
    (".d", True): CvTerm(
        source="MS", accession="MS:1002302", name="Bruker Container format"
    ),
    (".fid", False): CvTerm(
        source="MS", accession="MS:1000825", name="Bruker FID format"
    ),
    (".ibd", False): CvTerm(
        source="EDAM", accession="EDAM:format_3839", name="ibd"
    ),  # imaging MS binary; no MS ontology equivalent
    (".lcd", False): CvTerm(
        source="MS", accession="MS:1003009", name="Shimadzu Biotech LCD format"
    ),
    (".ms", False): CvTerm(
        source="MS", accession="MS:1001509", name="Agilent MassHunter format"
    ),
    (".qgd", False): CvTerm(
        source="MS", accession="MS:1000930", name="Shimadzu Biotech database entity"
    ),
    (".raw", False): CvTerm(
        source="MS", accession="MS:1000563", name="Thermo RAW format"
    ),
    (".raw", True): CvTerm(
        source="MS", accession="MS:1000526", name="Waters raw format"
    ),
    (".wiff", False): CvTerm(
        source="MS", accession="MS:1000562", name="ABI WIFF format"
    ),
    (".wiff.scan", False): CvTerm(
        source="MS", accession="MS:1000562", name="ABI WIFF format"
    ),
    # --- Derived file formats ---
    # cdf, cef, cnx, dx, imzml, mgf, msp, mzml, mzPeak, mzxml, peakml, xlsx, xy
    (".cdf", False): CvTerm(source="MS", accession="MS:1002441", name="Andi-MS format"),
    (".dx", False): CvTerm(
        source="EDAM", accession="EDAM:format_3859", name="JCAMP-DX"
    ),
    (".imzml", False): CvTerm(source="MS", accession="MS:1003611", name="imzML format"),
    (".mgf", False): CvTerm(
        source="MS", accession="MS:1001062", name="Mascot MGF format"
    ),
    (".msp", False): CvTerm(source="EDAM", accession="EDAM:format_4039", name="MSP"),
    (".mzdata", False): CvTerm(
        source="MS", accession="MS:1000564", name="PSI mzData format"
    ),
    (".mzml", False): CvTerm(source="MS", accession="MS:1000584", name="mzML format"),
    (".mzpeak", False): CvTerm(
        source="MS", accession="MS:1003610", name="mzPeak format"
    ),
    (".mzxml", False): CvTerm(
        source="MS", accession="MS:1000566", name="ISB mzXML format"
    ),
    (".xlsx", False): CvTerm(source="EDAM", accession="EDAM:format_3620", name="xlsx"),
    (".csv", False): CvTerm(source="EDAM", accession="EDAM:format_3752", name="CSV"),
    (".xml", False): CvTerm(source="EDAM", accession="EDAM:format_2332", name="XML"),
}

# DEFAULT_RAW_DATA_FILE_FORMAT = CvTerm(
#     source="EDAM", accession="EDAM:3245", name="Mass spectrometry data format"
# )

# DEFAULT_DERIVED_DATA_FILE_FORMAT = CvTerm(
#     source="EDAM", accession="EDAM:3245", name="Mass spectrometry data format"
# )


_mtbls_term_mappings: dict[str, dict[str, CvTerm]] | None = None


@cached(
    cache=TTLCache(maxsize=1, ttl=60),
)
def get_mtbls_terms_mapping() -> dict[str, dict[str, CvTerm]]:
    """loads MTBLS term 2 MS ontology term mappings file

    Returns:
        dict[str, dict[str, CvTerm]]: dictionary of MTBLS term name (lowercase)
        to a CvTerm object grouped by MHD common parameter definition
    """
    global _mtbls_term_mappings
    if _mtbls_term_mappings:
        return _mtbls_term_mappings
    with (
        resources.files(mtbls2mhd.__name__)
        .joinpath("cv_mapping_table.csv")
        .open("r", encoding="utf-8") as f
    ):
        # MAP TO MHD common parameter/characteristic definition
        definition_mappings = {
            "mass spectrometry instrument": "mass spectrometry instrument",
            "liquid chromatography instrument": "chromatography instrument",
            "gas chromatography instrument": "chromatography instrument",
            "ionization type": "ionization type",
            "chromatography separation": "chromatography separation",
            "inlet type": "inlet type",
            "acquisition polarity": "acquisition polarity",
            # TODO: Define mappings for the following parameters.
            # "Ionization polarity": "ionization polarity",
            # "Instrument class": "instrument class",
            # "Chromatography column": "chromatography column?",
            "organism": "organism",
            "organism part": "organism part",
            "cell type": "cell type",
            "disease": "disease",
        }
        # Column names and their index in the cv_mappings.csv file.
        HEADERS = {
            "REF_ACCESSION": 0,
            "REF_TERM": 1,
            "MTBLS_CV_NAMES": 1,
            "MTBLS_CV_NAMES2": 2,
            "MTBLS_CV_NAMES3": 3,
            "INSTANCE": 5,
        }

        reader = csv.reader(f)
        mtbls_term_mappings = {}
        for idx, row in enumerate(reader):
            if idx == 0:
                continue
            accession = row[HEADERS["REF_ACCESSION"]].replace("_", ":")
            source = accession.split(":")[0]
            term = row[HEADERS["REF_TERM"]]
            mtbls_cv_names_set: set[str] = set()
            for x in ["MTBLS_CV_NAMES", "MTBLS_CV_NAMES2", "MTBLS_CV_NAMES3"]:
                mtbls_cv_names_set.update(
                    {
                        name.strip().lower().split(":")[1]
                        if ":" in name
                        else name.strip().lower()
                        for name in row[HEADERS[x]].split(";")
                        if name and name.strip()
                    }
                )
            # mtbls_cv_names = list(mtbls_cv_names_set)
            instance = row[HEADERS["INSTANCE"]]
            if not mtbls_cv_names_set or not term or not accession:
                continue
            if not instance:
                logger.warning(
                    "MTBLS term mapping file row %s with accession %s and term '%s' "
                    "does not have an instance value. Row will be ignored.",
                    idx + 1,
                    accession,
                    term,
                )
                continue
            definition = definition_mappings.get(instance.lower())
            if definition not in mtbls_term_mappings:
                mtbls_term_mappings[definition] = {}
            for mtbls_cv_name in mtbls_cv_names_set:
                mtbls_term_mappings[definition][mtbls_cv_name.lower()] = CvTerm(
                    source=source,
                    accession=accession,
                    name=term,
                )
    _mtbls_term_mappings = mtbls_term_mappings
    return _mtbls_term_mappings


MTBLS_CV_TERM_MAPPINGS: dict[str, CvTerm] = {}
for map_val in get_mtbls_terms_mapping().values():
    MTBLS_CV_TERM_MAPPINGS.update({x.accession.lower(): x for x in map_val.values()})

MTBLS_PROTOCOL_PARAMETERS_MAPPINGS: dict[str, CvTerm] = {}
for map_val in MTBLS_PROTOCOL_PARAMETER_DEFINITION_MAP.values():
    MTBLS_PROTOCOL_PARAMETERS_MAPPINGS.update(
        {x.accession.lower(): x for x in map_val.values()}
    )
DEFAULT_TERMS: dict[str, CvTerm] = {}

MAF_TABLE_COLUMN_TERMS = {
    "mass_to_charge": CvTerm(
        source="NCIT", accession="NCIT:C48049", name="Mass-to-Charge Ratio"
    ),
    "retention_time": CvTerm(
        source="MS", accession="MS:1000894", name="retention time"
    ),
    "chemical_formula": CvTerm(
        source="EDAM", accession="EDAM:data_0846", name="Chemical formula"
    ),
}

for mtbls_default_term_map in [
    PREDEFINED_CV_TERMS,
    MTBLS_ASSAY_TYPES,
    MTBLS_MEASUREMENT_TYPES,
    MTBLS_TECHNOLOGY_TYPES,
    FILE_EXTENSIONS,
    MTBLS_PROTOCOLS_MAP,
    MTBLS_CV_TERM_MAPPINGS,
    MTBLS_CHARACTERISTICS_MAP,
    MTBLS_STUDY_FACTOR_MAP,
    ALL_COMMON_PROTOCOL_PARAMETERS,
    MAF_TABLE_COLUMN_TERMS,
]:
    DEFAULT_TERMS.update(
        {x.accession.lower(): x for x in mtbls_default_term_map.values()}
    )
