from __future__ import absolute_import

import itertools
import urllib2


class Vote(dict):

    sequence = itertools.count()

    def __init__(self, chamber, date, motion, passed,
                 yes_count, no_count, other_count, **kwargs):
        """
        Create a new :obj:`Vote`.

        :param chamber: the chamber in which the vote was taken,
          'upper' or 'lower'
        :param date: the date/time when the vote was taken
        :param motion: a string representing the motion that was being voted on
        :param passed: did the vote pass, True or False
        :param yes_count: the number of 'yes' votes
        :param no_count: the number of 'no' votes
        :param other_count: the number of abstentions, 'present' votes,
          or anything else not covered by 'yes' or 'no'.

        Any additional keyword arguments will be associated with this
        vote and stored in the database.

        Examples: ::

          Vote('upper', '', '12/7/08', 'Final passage',
               True, 30, 8, 3)
          Vote('lower', 'Finance Committee', '3/4/03 03:40:22',
               'Recommend passage', 12, 1, 0)
        """
        super(Vote, self).__init__(**kwargs)

        #required
        self['motion'] = motion
        self['chamber'] = chamber
        self['date'] = date
        self['passed'] = passed
        self['yes_count'] = yes_count
        self['no_count'] = no_count
        self['other_count'] = other_count

    def yes(self, legislator):
        """
        Indicate that a legislator (given as a string of their name) voted
        'yes'.

        Examples: ::

           vote.yes('Smith')
           vote.yes('Alan Hoerth')
        """
        if 'yes_votes' not in self:
            self['yes_votes'] = []

        self['yes_votes'].append(legislator)

    def no(self, legislator):
        """
        Indicate that a legislator (given as a string of their name) voted
        'no'.
        """
        if 'no_votes' not in self:
            self['no_votes'] = []
        self['no_votes'].append(legislator)

    def other(self, legislator):
        """
        Indicate that a legislator (given as a string of their name) abstained,
        voted 'present', or made any other vote not covered by 'yes' or 'no'.
        """
        if 'other_votes' not in self:
            self['other_votes'] = []
        self['other_votes'].append(legislator)

    def add_source(self, url):
        """
        Add a source URL from which data related to this object was scraped.
        :param url: The location of the source
        """
        url = urllib2.quote(url, "://?=&%")
        if 'sources' not in self:
            self['sources'] = []
        self['sources'].append(dict(url=url))

    def get_filename(self):
        filename = '%s_%s_%s_seq%s.json' % (self['session'],
                                            self['chamber'],
                                            self['bill_id'],
                                            self.sequence.next())
        return filename

    def __unicode__(self):
        return "%s %s: %s '%s'" % (self['session'], self['chamber'],
                                   self['bill_id'], self['motion'])
