"""
    Import metadata from DataAccess-Service
"""
from __future__ import absolute_import

import logging
import pytz

logger = logging.getLogger(__name__)

def get_timezone(data_access_client, locality):
    """
    Fetch the timezone for a locality from the metadata
    """
    tz = "Etc/UTC"
    # We set priority to 0, as the scrapers should have lower priority then the frontend
    result = data_access_client.getLocalityMetadata(priority=0, requester=["State Regs Scraper"], locality=locality)
    if result.localityMetadata:
        if result.localityMetadata.timezone:
            tz = result.localityMetadata.timezone
        else:
            logger.warning("No timezone found in metadata for locality %s", locality)
    else:
        logger.warning("No metadata found for locality %s", locality)
    logger.debug("Timezone for locality %s is %s", locality, tz)
    return pytz.timezone(tz)
