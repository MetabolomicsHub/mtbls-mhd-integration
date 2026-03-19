import logging
import sys


def setup_basic_logging_config():
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
        datefmt="%d/%b/%Y %H:%M:%S",
        stream=sys.stdout,
    )
    logging.getLogger("fake_useragent").setLevel(logging.ERROR)
    logging.getLogger("mhd_model.model.v0_1.dataset.validation.base").setLevel(
        logging.WARNING
    )
    logging.getLogger("httpx").setLevel(logging.ERROR)
