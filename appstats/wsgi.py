"""Authenticated and authorized HIS services."""

from collections import defaultdict

from flask import request

from his import CUSTOMER, authenticated, authorized, Application
from timelib import strpdatetime_or_time
from wsgilib import JSON

from digsigdb import Statistics

__all__ = ['APPLICATION']


APPLICATION = Application('Application statistics', cors=True, debug=True)


def _get_stats(start=None, end=None):
    """Yields the customer's tenant-to-tenant messages."""

    expression = Statistics.customer == CUSTOMER.id

    if start is not None:
        expression &= Statistics.timestamp >= start

    if end is not None:
        expression &= Statistics.timestamp <= end

    return Statistics.select().where(expression)


def _count_stats(statistics):
    """Counts the respective statistics."""

    counts = defaultdict(int)

    for statistic in statistics:
        counts[statistic.document] += 1

    return counts


@authenticated
@authorized('tenant2tenant')
def list_stats():
    """Returns the respective statistics."""

    start = strpdatetime_or_time(request.args.get('from'))
    end = strpdatetime_or_time(request.args.get('until'))
    statistics = _get_stats(start=start, end=end)

    try:
        request.args['raw']
    except KeyError:
        return JSON(_count_stats(statistics))

    return JSON([statistic.to_dict() for statistic in statistics])


ROUTES = (('GET', '/', list_stats, 'list_stats'),)
APPLICATION.add_routes(ROUTES)
