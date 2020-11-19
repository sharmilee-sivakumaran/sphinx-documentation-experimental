"""
Utility functions for sending events for the purpose of creating Process Reports
"""
from __future__ import absolute_import


def send_create_process_event(publisher, process_id, locality):
    """
    First event sent, which creates links a process id to a task_name in the events engine.
    :param publisher: BlockingEventPublisher
    :param process_id: uuid
    :param locality: Two letter state abbrev
    :return: None
    """
    task_name = "StateRegs{}".format(locality.upper())
    message = u"Linking Process ID {} with Process Name {}".format(process_id, task_name)
    extra_info = {'process_name': task_name}
    send_event(publisher, 'ok', "create_process", process_id, message, extra_info=extra_info)


def send_scrape_failed_event(publisher, process_id, locality, trace):
    """
    Failure event for an scraping error
    :param publisher: BlockingEventPublisher
    :param process_id: uuid
    :param locality: Two letter state abbrev
    :return: None
    """
    message = u"Faliure to scrape {} becauese of error:\n {}".format(locality, trace)
    event_keys = {'locality': locality}
    send_event(publisher, 'critical', "scraper_error", process_id, message, event_keys)


def send_event(publisher, severity, event_type, process_id, message, event_keys=None, extra_info=None):
    """
    Send event to Rabbit
    :param publisher: BlockingEventPublisher used to publish event
    :param severity: Severity of the event
    :param event_type: EventType
    :param process_id: Unique id representing a specific scrape session
    :param message: The event's message
    :param event_keys: Dict of supplementary info for better understanding an event
    :param extra_info: Additional information for special events, i.e. mapping a process id to a process name
    :return: None
    """
    event = {
        "message": message,
        "event_type": event_type,
    }
    if event_keys:
        event["event_keys"] = event_keys
    if extra_info:
        event["extra_info"] = extra_info

    publisher.publish_event(severity, event, process_id)
