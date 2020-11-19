'''
Compare bills across environments.

This does things.
'''

from __future__ import print_function, absolute_import
import json

from fn_pillar_models.legislation.bill import (
    Bill, BillForecast, BillCategory, BillSubjectsJoinTable, BillSimilarity,
    BillSponsorLegislator, BillVoter, BillDocument, BillActionMappedType,
    BillActionCommittee)

from fn_pillar_models.legislation.committee import Committee
from fn_pillar_models.legislation.legislator import Legislator

from sql_comp.sql_comp import SqlComp

def _bill_trim(record):
    '''Formats non-root bill records to avoid getting recursive. '''
    return '{}:{}:{}'.format(
        record.locality, record.session, record.external_id)

class BillComp(SqlComp):

    def __init__(self, *args, **kwargs):
        kwargs['env_type'] = 'pillar'
        super(BillComp, self).__init__(*args, **kwargs)

        self.trimmings = {
            Bill: _bill_trim,
            Committee: lambda r: r.name,
            BillCategory: lambda r: r.category.name,
            Legislator: lambda r: r.person.name_full,
            BillForecast: lambda r: '{} {}'.format(
                r.enactment_probability, r.enactment_outlook),
            BillSubjectsJoinTable: lambda r: r.subject.subject,
            BillSimilarity: lambda r: '{} to {}'.format(r.score, _bill_trim(r.bill)),
            BillSponsorLegislator: lambda r: r.legislator.person.name_full,
            BillVoter: lambda r: {'vote': r.vote_cast, 'name': r.name},
            BillActionMappedType: lambda r: r.action_type,
            BillActionCommittee: lambda r: r.committee.name,
        }
        self.json_filters += ('.document_service.url', '~.similarities[',
                              '.forecast')

        self.custom_compares = {
            '$.categories': SqlComp.set_compare,
            '$.sponsor_legislators': SqlComp.set_compare,
            '$.sponsor_unmatched': SqlComp.set_compare,
        }

    @classmethod
    def parser(cls, config, parser):
        '''Bill specific command line arguments. '''
        super(BillComp, cls).parser(config, parser)
        parser.add_argument('locality', help='Bill locality')
        parser.add_argument('session', help='Session to check, e.g. 20172018r')
        parser.add_argument('bills', help='External ID of one or more bills',
                            nargs='*')
        parser.add_argument('--include_inactive', action='store_true',
                            help='Whether to include or skip inactive documents')

    def node_filter(self, obj):
        '''Filters bill documents that are inactive. '''
        if not self.args.include_inactive:
            if isinstance(obj, BillDocument) and not obj.is_active:
                return True
            if isinstance(obj, BillSponsorLegislator) and not obj.is_active:
                return True
        return False

    def run(self):
        '''Compare bills using the default comparison and report tool. '''
        filters=[
            'locality == "{}"'.format(self.args.locality),
            'session == "{}"'.format(self.args.session),
        ]
        if self.args.bills:
            filters.append(
                'external_id in {}'.format(json.dumps(self.args.bills)))

        self.diff(Bill, 'external_id', filters)
