"""
    Import metadata from DataAccess-Service
"""
from __future__ import absolute_import

import logging
import pytz
import json
import os
import datetime
from fn_dataaccess_client.blocking.locality_metadata import ttypes

logger = logging.getLogger(__name__)


def get_timezone(data_access_client, locality):
    """
    Fetch the timezone for a locality from the metadata
    """
    tz = "Etc/UTC"
    # We set priority to 0, as the scrapers should have lower priority then the frontend
    result = data_access_client.getLocalityMetadata(priority=0, requester=["Leg Event Scraper"], locality=locality)
    if result.localityMetadata:
        if result.localityMetadata.timezone:
            tz = result.localityMetadata.timezone
        else:
            logger.warning("No timezone found in metadata for locality %s", locality)
    else:
        logger.warning("No metadata found for locality %s", locality)
    logger.debug("Timezone for locality %s is %s", locality, tz)
    return pytz.timezone(tz)


def get_metadata(data_access_client, abbr):
    """
    Fetch metadata for a locality
    """

    _LEGISLATURE_TYPE_MAP = {
        ttypes.LegislatureType.BICAMERAL: "bicameral",
        ttypes.LegislatureType.UNICAMERAL: "unicameral",
    }

    _SESSION_TYPE_MAP = {
        ttypes.SessionType.REGULAR: "regular",
        ttypes.SessionType.SPECIAL: "special",
    }
    mapping_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'metadata_mapping.json'))
    with open(mapping_path) as mapping_file:
        metadata_mapping = json.load(mapping_file)

    metadata = {}
    try:
        thrift_metadata = \
            data_access_client.getLocalityMetadata(0, "Leg Event Scraper", abbr).localityMetadata
        metadata = {
            "name": thrift_metadata.displayName,
            "abbreviation": thrift_metadata.locality,
            "legislature_name": thrift_metadata.legislatureName,
            "legislature_type": _LEGISLATURE_TYPE_MAP[thrift_metadata.legislatureType],
            "legislature_url": thrift_metadata.legislatureUrl,
            "timezone": thrift_metadata.timezone,
            "legislative_session_containers": [],
        }

        for thrift_container in thrift_metadata.sessionContainers:
            container = {
                "id": thrift_container.id,
                "start_year": thrift_container.startYear,
                "end_year": thrift_container.endYear,
                "sessions": [],
            }
            for thrift_session in thrift_container.sessions:
                session = {
                    "id": thrift_session.id,
                    "type": _SESSION_TYPE_MAP[thrift_session.type],
                    "name": thrift_session.name,
                    "subsessions": [],
                }
                if abbr in metadata_mapping and thrift_session.id in metadata_mapping[abbr]:
                    session["external_id"] = metadata_mapping[abbr][thrift_session.id]
                for thrift_subsession in thrift_session.subsessions:
                    subsession = {
                        "start_date": thrift_subsession.startDate
                    }
                    # optional subsession fields

                    if thrift_subsession.endDate:
                        subsession["end_date"] = thrift_subsession.endDate

                    if thrift_subsession.lowerIntroDeadline:
                        subsession["lower_intro_deadline"] = thrift_subsession.lowerIntroDeadline

                    if thrift_subsession.upperIntroDeadline:
                        subsession["upper_intro_deadline"] = thrift_subsession.upperIntroDeadline

                    if thrift_subsession.committeeDeadline:
                        subsession["committee_deadline"] = thrift_subsession.committeeDeadline

                    if thrift_subsession.crossoverDeadline:
                        subsession["crossover_deadline"] = thrift_subsession.crossoverDeadline

                    session["subsessions"].append(subsession)
                container["sessions"].append(session)
            metadata["legislative_session_containers"].append(container)

    except Exception as exc:
        raise Exception("Unable to lookup metadata, seems to be a DataAccessService issue. msg = %s" %
                        exc.message)

    if not metadata:
        raise Exception("No metadata found for locality '%s'" % abbr)

    return metadata


def get_session_from_id(abbr, session_id, meta=None):
    if not meta:
        meta = get_metadata(abbr)

    for container in meta['legislative_session_containers']:
        for session in container['sessions']:
            if session.get('external_id', session['id']) == session_id:
                return session

    raise ValueError("no such session for external id'%s'" % session_id)


def get_session_from_internal_id(abbr, session_id, meta=None):
    if not meta:
        meta = get_metadata(abbr)

    for container in meta['legislative_session_containers']:
        for session in container['sessions']:
            if session.get('id') == session_id:
                return session

    raise ValueError("no such session for id'%s'" % session_id)


def container_for_session(abbr, session_id, meta=None):
    if not meta:
        meta = get_metadata(abbr)

    for container in meta['legislative_session_containers']:
        for session in container['sessions']:
            if session['external_id'] == session_id:
                return container

    raise ValueError("no such container for session '%s'" % session_id)


def _get_latest_sessions(abbr, meta=None):

    if not meta:
        meta = get_metadata(abbr)

    # most recent session containers should be first in the list, return that list of sessions
    return_sessions = meta['legislative_session_containers'][0]['sessions']
    session_ids_return = []
    # Create list of external ids
    for session in return_sessions:
        session_ids_return.append(session.get("external_id", session['id']))

    return session_ids_return


def _is_current_session(abbr, session_id, meta=None):
    if not meta:
        meta = get_metadata(abbr)
    session = get_session_from_internal_id(abbr, session_id, meta)

    print "CHECKING IS SESSIONS %s IS THE LATEST SESSION" % session
    today = datetime.date.today()

    if 'subsessions' not in session:
        return False
    sub_session = session['subsessions'][0]
    start_date = datetime.datetime.strptime(sub_session['start_date'], '%Y-%m-%d').date()
    if start_date <= today:
        if 'end_date' in session['subsessions'][-1]:
            end_date = datetime.datetime.strptime(session['subsessions'][-1]['end_date'], '%Y-%m-%d').date()
            if end_date >= today:
                return True
        else:
            return True

    return False


def _is_most_recent_session(abbr, session_id, meta=None):
    if not meta:
        meta = get_metadata(abbr)
    if _is_current_session(abbr, session_id, meta):
        return True
    today = datetime.date.today()
    today_year = today.year
    for container in meta['legislative_session_containers']:
        if container['start_year'] <= today_year:
            for session in reversed(container['sessions']):
                for sub in reversed(session['subsessions']):
                    start_date = datetime.datetime.strptime(sub['start_date'], '%Y-%m-%d').date()
                    if start_date <= today:
                        return session_id == session['id']
    return False


def is_most_recent_session(abbr, session_id):
    return _is_most_recent_session(abbr, session_id)


def _get_active_sessions(abbr, meta=None, get_internal_id=False):
    if not meta:
        meta = get_metadata(abbr)

    """
    Will determine which sessions are currently active, will take any sessions whose
    start year is the current year as well as any and all FUTURE sessions listed in the metadata
    """

    sessions = []
    session_ids_return = []
    has_dates = False
    today = datetime.date.today()
    today_year = today.year

    # Append all sessions of any session in the present or future
    for container in meta['legislative_session_containers']:
        if today_year <= int(container['end_year']):
            sessions += container['sessions']

    logging.warning("Found session container " + str(sessions))

    # Scrapers require the external_ids fro running things, we will replace these ids with the pillar
    # related ids before we send things through rmq
    for session in sessions:
        if get_internal_id:
            session_ids_return.append(session["id"])
        else:
            session_ids_return.append(session.get("external_id", session["id"]))

    return session_ids_return
