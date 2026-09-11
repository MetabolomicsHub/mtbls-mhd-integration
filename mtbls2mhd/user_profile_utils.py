import logging
import re

from keycloak import KeycloakAdmin
from metabolights_utils.models.metabolights.model import (
    MetabolightsStudyModel,
)

from mtbls2mhd.config import Mtbls2MhdConfiguration

logger = logging.getLogger(__name__)


def update_submitter_user_from_keycloak(
    data: MetabolightsStudyModel, config: Mtbls2MhdConfiguration
):
    if (
        not config.mtbls_auth_server_url
        or not config.mtbls_auth_client_id
        or not config.mtbls_auth_client_secret
        or not data.study_db_metadata
    ):
        logger.warning(
            "Keycloak auth service is not available. Skipping user profile updates"
        )
        return

    if (
        config.mtbls_auth_server_url
        and config.mtbls_auth_client_id
        and config.mtbls_auth_client_secret
        and data.study_db_metadata
        and data.study_db_metadata.submitters
    ):
        keycloak_admin = KeycloakAdmin(
            server_url=config.mtbls_auth_server_url,
            realm_name=config.mtbls_auth_realm_name,
            client_secret_key=config.mtbls_auth_client_secret,
            client_id=config.mtbls_auth_client_id,
            verify=True,
        )

        for submitter in data.study_db_metadata.submitters:
            user_id = keycloak_admin.get_user_id(submitter.user_name)
            if not user_id:
                continue
            user = keycloak_admin.get_user(user_id=user_id, user_profile_metadata=True)
            if user:
                payload: dict = user.get("attributes", {})
                orcid = payload.get("orcid") or [""]
                orcid = re.sub(r"https?://orcid\.org/", "", (orcid[0] or "").lower())
                submitter.first_name = user.get("firstName")
                submitter.last_name = user.get("lastName")
                submitter.affiliation = (payload.get("affiliation") or [""])[0]
                submitter.address = (
                    (payload.get("affiliationAddress") or [""])[0]
                    + " "
                    + (payload.get("country") or [""])[0]
                )
                submitter.affiliation_url = (payload.get("affiliationUrl") or [""])[0]
