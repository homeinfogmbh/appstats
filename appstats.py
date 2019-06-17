"""WSGI services."""

from collections import defaultdict

from flask import request

from digsigdb import Statistics
from his import CUSTOMER, authenticated, authorized, Application
from terminallib import Deployment
from timelib import strpdatetime
from wsgilib import ACCEPT, JSON, OK


__all__ = ['APPLICATION']


APPLICATION = Application('Application statistics', debug=True)


def _get_stats(deployment, since, until):
    """Yields the customer's tenant-to-tenant messages."""

    expression = Deployment.customer == CUSTOMER.id

    if deployment:
        expression &= Statistics.deployment == deployment

    if since is not None:
        expression &= Statistics.timestamp >= since

    if until is not None:
        expression &= Statistics.timestamp <= until

    return Statistics.select().join(Deployment).where(expression)


def _count_stats(statistics):
    """Counts the respective statistics."""

    stats = defaultdict(lambda: defaultdict(int))

    for statistic in statistics:
        stats[statistic.deployment_id][statistic.document] += 1

    return stats


@authenticated
@authorized('tenant2tenant')
def list_stats():
    """Returns the respective statistics."""

    since = strpdatetime(request.args.get('since'))
    until = strpdatetime(request.args.get('until'))
    deployment = request.args.get('deployment')

    if deployment is not None:
        try:
            deployment = int(deployment)
        except ValueError:
            return ('Invalid deployment ID.', 404)

    statistics = _get_stats(deployment, since, until)

    try:
        request.args['raw']
    except KeyError:
        return JSON(_count_stats(statistics))

    if 'text/csv' in ACCEPT:
        return OK('\r\n'.join(statistic.to_csv() for statistic in statistics))

    return JSON([statistic.to_json() for statistic in statistics])


ROUTES = (('GET', '/', list_stats),)
APPLICATION.add_routes(ROUTES)
