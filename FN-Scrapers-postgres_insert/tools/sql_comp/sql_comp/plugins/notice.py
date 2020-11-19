from datetime import datetime
import json

from sql_comp.sql_comp import SqlComp
from fn_pillar_models.state_regulation.regulation import Regulation
from fn_pillar_models.state_regulation.regulation_notice import RegulationNotice
from fn_pillar_models.state_regulation.regulation_agency import RegulationAgency
from fn_pillar_models.state_regulation.regulation_hearing import RegulationHearing
from fn_pillar_models.state_regulation.regulation_notice_content import RegulationNoticeContent
from fn_pillar_models.state_regulation.regulation_attachment import RegulationAttachment

class StateRegNotice(SqlComp):
    '''Compares State-Reg Notices. '''
    def __init__(self, *args, **kwargs):
        kwargs['env_type'] = 'pillar'
        super(StateRegNotice, self).__init__(*args, **kwargs)

        self.trimmings = {
            Regulation: SqlComp.trim('''
                id locality scraper_regulation_id scraper_regulation_id_hash
                title  message_published initial_publication_date
            '''),
            RegulationAgency: SqlComp.trim('name notice_id'),
            RegulationHearing: SqlComp.trim('''
                description url location hearing_start_datetime hearing_start_date
            '''),
            RegulationNoticeContent: lambda r: r.content,
            RegulationAttachment: SqlComp.trim('''
                document_url document_is_internal_url document_mime_type
            ''')
        }

    @classmethod
    def parser(cls, config, parser):
        '''Regulation Notice specific command line arguments. '''
        super(StateRegNotice, cls).parser(config, parser)
        parser.add_argument('locality', help='Locality')
        parser.add_argument('--regs', help='Regulation ID', nargs='*')
        parser.add_argument('--start', help='Scraper last scraped time start (Y-m-d)')
        parser.add_argument('--end', 
                            help='Scraper last scraped time end (Y-m-d)')
        parser.add_argument('--identity', nargs='*',
                            help='List one or more fields to use as notice identifiers')
        parser.add_argument('--include_inactive', action='store_true',
                            help='Whether to include or skip inactive documents')

    def run(self):
        '''Compare bills using the default comparison and report tool. '''
        filters=[
            'locality == "{}"'.format(self.args.locality),
        ]
        if self.args.start:
            filters.append('last_scraped_at >= "{}"'.format(self.args.start))
        if self.args.end:
            filters.append('last_scraped_at <= "{}"'.format(self.args.end))
        if self.args.regs:
            filters.append(
                'id in {}'.format(json.dumps(self.args.regs)))

        ident = self.args.identity or 'scraper_notice_id'

        self.diff(RegulationNotice, ident, filters)