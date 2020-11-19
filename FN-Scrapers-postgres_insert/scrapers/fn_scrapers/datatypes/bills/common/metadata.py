from __future__ import absolute_import

import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class InvalidSession(Exception):
    """
    Exception to be raised when no data exists for a given period
    """
    def __init__(self, session, active_sessions):
        self.session = session
        self.active_sessions = active_sessions

    def __str__(self):
        return 'No data exists for {}, perhaps you meant: {}'.format(
            self.session,
            ', '.join([sess.id for sess in self.active_sessions])
        )


def get_session(metadata_client, locality, session):
    result = metadata_client.getSession(priority=0, requester=["Legislative Scraper"],
                                        locality=locality, id=session)
    return result.session


def validate_sessions(metadata_client, locality, sessions):
    for session in sessions:
        if not get_session(metadata_client, locality, session):
            active_sessions = metadata_client.findCurrentAndFutureSessionsByLocalityAndDate(
                priority=0, requester=["Legislative Scraper"], locality=locality,
                date=datetime.now().strftime('%Y-%m-%d'))
            raise InvalidSession(session, active_sessions)


def get_session_name(metadata_client, locality, session):
    session = get_session(metadata_client, locality, session)
    return session.name
