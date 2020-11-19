'''
Compare bills across environments.

This does things.
'''

from __future__ import print_function, absolute_import
import json

from fn_pillar_models.legislation.leg_event import (
    LegEvent, LegEventHostCommittee, LegEventRelatedBill, LegEventSource)

from sql_comp.sql_comp import SqlComp

class EventComp(SqlComp):
    '''
    Compares events. Performs set comparisons across bills.
    '''
    def __init__(self, *args, **kwargs):
        kwargs['env_type'] = 'pillar'
        super(EventComp, self).__init__(*args, **kwargs)

        self.trimmings = {
            LegEventHostCommittee: EventComp._committee_trimming,
            LegEventRelatedBill: EventComp._bill_trimming,
            LegEventSource: SqlComp.trim('url')
        }

        self.custom_compares = {
            '$.related_bills': SqlComp.set_compare
        }

    @classmethod
    def parser(cls, config, parser):
        '''Bill specific command line arguments. '''
        super(EventComp, cls).parser(config, parser)
        parser.add_argument('locality', help='Bill locality')
        parser.add_argument('--start', help='Scraper last scraped time start (Y-m-d)')
        parser.add_argument('--end', help='Scraper last scraped time end (Y-m-d)')

    def run(self):
        '''Compare bills using the default comparison and report tool. '''
        filters=[
            'locality == "{}"'.format(self.args.locality),
            'cancelled_at == null'
        ]

        if self.args.start:
            filters.append('last_scraped_at >= "{}"'.format(self.args.start))
        if self.args.end:
            filters.append('last_scraped_at <= "{}"'.format(self.args.end))

        self.diff(LegEvent, ('description', 'location', 'start_date', 'start_time'), filters)


    @classmethod
    def _bill_trimming(cls, record):
        '''How to trim bills.'''
        val = {
            'external_id': record.external_id, 
            'related_bill': None
        }
        if record.related_bill:
            val['related_bill'] = SqlComp.trim(
                'locality session external_id'
            ).__call__(
                record.related_bill
            )
        return val


    @classmethod
    def _committee_trimming(cls, record):
        '''How to trim committees.'''
        val = {
            'name': record.name, 
            'committee': None
        }
        if record.committee:
            val['committee'] = SqlComp.trim(
                'committee_type name short_name external_id chamber'
            ).__call__(
                record.committee
            )
        return val
