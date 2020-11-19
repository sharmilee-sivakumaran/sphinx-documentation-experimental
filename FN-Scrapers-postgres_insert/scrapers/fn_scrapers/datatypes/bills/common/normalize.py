from __future__ import absolute_import

import re


def normalize_bill_id(bill_id):
    """
    Function to take in an external bill id and normalize its format to ensure standard
    references to the bill
    :param bill_id: bill type and number
    :type bill_id: string
    :return: normalized external id
    :rtype: string
    """
    # If there isn't a space between the bill type and the numerical id, add it.
    if ' ' not in bill_id:
        first_digit = re.search(r"\d", bill_id)
        if first_digit:
            bill_id = bill_id[0:first_digit.start()] + ' ' + bill_id[first_digit.start():]

    # Split on the space between type and id
    split_bill_id = bill_id.upper().strip().split()
    if len(split_bill_id) != 2:
        raise AssertionError("bill_id '{}' does not contain exactly 2 parts (<type> <id>)".format(bill_id))

    # remove dots from id type
    id_type = split_bill_id[0].replace('.', '')
    id_type_re = re.compile(r'^[A-Z]+$')
    if not id_type_re.match(id_type):
        raise AssertionError(
            "bill_id '{}' does not have a valid type split, type = '{}'".format(bill_id, id_type))

    # remove leading zeros from id
    id_id = split_bill_id[1].lstrip('0')
    id_id_re = re.compile(r'^(?:[\dA-Z]+|[-\dA-Z]{3,})$')
    if not id_id_re.match(id_id):
        raise AssertionError("bill_id '{}' does not have a valid id split, id = '{}'".format(bill_id, id_id))

    normalized_bill_id = "{} {}".format(id_type, id_id)

    return normalized_bill_id

def get_chamber_from_ahs_type_bill_id(bill_id):
    try:
        chamber  = {'H': u'lower',
                    'A': u'lower',
                    'C': u'lower',
                    'I': u'lower',
                    'S': u'upper',
                    'J': u'upper'
                    } [bill_id[0]]
    except KeyError:
        return None
    return chamber


def get_bill_type_from_normal_bill_id(bill_id):
    """
    Get Bill type from Bill ID
    """
    bill_char = bill_id[1:].split(' ')[0]
    try:
        bill_type = {"B": "bill",
                     "C": "concurrent_resolution",
                     "F": "bill",
                     "O": "bill",
                     "J": "joint_resolution",
                     "P": "bill",
                     "R": "resolution",
                     "M": "memorial",
                     "N": "resolution",
                     "SB": "bill",
                     "SR": "joint_resolution",
                     "CR": "concurrent_resolution",
                     "CM": "joint_memorial",
                     "CJ": "joint_resolution",
                     "CA": "constitutional_amendment",
                     "CB": "bill",
                     "JR": "joint_resolution",
                     "PB": "bill",
                     "JM": "joint_memorial",
                     "MR": "memorial",
                     "CMR": "memorial",
                     "RM": "resolution",
                     "RB": "bill",
                    }[bill_char]
    except KeyError:
        raise AssertionError("bill_id '{}' does not match any bill type".format(bill_id))

    return bill_type
