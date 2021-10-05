"""WSGI services."""

from collections import defaultdict
from datetime import datetime
from typing import Iterable, Iterator, Optional, Union

from flask import request, Response

from digsigdb import Statistics
from his import CUSTOMER, authenticated, authorized, Application
from hwdb import Deployment
from mdb import Address
from wsgilib import ACCEPT, Binary, JSON


__all__ = ['APPLICATION']


APPLICATION = Application('Application statistics', debug=True)


def _get_deployment(deployment_id: int) -> Deployment:
    """Returns the respective deployment with its address."""

    join = Deployment.address == Address.id
    return Deployment.select(Deployment, Address).join(Address, on=join).where(
        (Deployment.id == deployment_id)
        & (Deployment.customer == CUSTOMER.id)
    ).get()


def _get_stats(deployment: Union[Deployment, int], since: Optional[datetime],
               until: Optional[datetime]) -> Iterator[Statistics]:
    """Yields the customer's tenant-to-tenant messages."""

    condition = Deployment.customer == CUSTOMER.id

    if deployment is not None:
        condition &= Statistics.deployment == deployment

    if since is not None:
        condition &= Statistics.timestamp >= since

    if until is not None:
        condition &= Statistics.timestamp <= until

    return Statistics.select().join(Deployment).where(condition).iterator()


def _count_stats(statistics: Iterable[Statistics]) -> dict[int, dict[str, int]]:
    """Counts the respective statistics."""

    stats = defaultdict(lambda: defaultdict(int))

    for statistic in statistics:
        stats[statistic.deployment_id][statistic.document] += 1

    return stats


def _stats_to_csv(counted_stats: dict[int, dict[str, int]]) -> Iterator[str]:
    """Yields CSV records."""

    addresses = {
        deployment.id: deployment.address for deployment in
            Deployment.select(Deployment, Address).join(
                Address, on=Deployment.address == Address.id).where(
                Deployment.id << set(counted_stats.keys()))
    }
    total_clicks = 0

    for deployment_id, documents in counted_stats.items():
        deployment_clicks = 0

        for document, clicks in documents.items():
            deployment_clicks += clicks
            yield f'{addresses[deployment_id]};{document};{clicks}'

        total_clicks += deployment_clicks
        yield f'{addresses[deployment_id]};TOTAL;{deployment_clicks}'

    yield f'TOTAL;*;{total_clicks}'


def _get_csv_filename(deployment: Optional[int], since: Optional[datetime],
                      until: Optional[datetime]) -> str:
    """Returns a CSV file name."""

    if deployment is None:
        address = 'all'
    else:
        address = _get_deployment(deployment).address

    since = 'beginning' if since is None else since.isoformat()
    until = 'now' if until is None else until.isoformat()

    return f'statistics-{address}-{since}-{until}.csv'


@authenticated
@authorized('appstats')
def list_stats() -> Response:
    """Returns the respective statistics."""

    if (since := request.args.get('since')) is not None:
        since = datetime.fromisoformat(since)

    if (until := request.args.get('until')) is not None:
        until = datetime.fromisoformat(until)

    deployment = request.args.get('deployment')

    if deployment is not None:
        try:
            deployment = int(deployment)
        except ValueError:
            return ('Invalid deployment ID.', 404)

    statistics = _get_stats(deployment, since, until)

    if request.args.get('raw', False):
        return JSON([statistic.to_json() for statistic in statistics])

    counted_stats = _count_stats(statistics)

    if 'text/csv' in ACCEPT:
        text = '\r\n'.join(_stats_to_csv(counted_stats))
        filename = _get_csv_filename(deployment, since, until)
        return Binary(text.encode(), filename=filename)

    return JSON(counted_stats)


ROUTES = (('GET', '/', list_stats),)
APPLICATION.add_routes(ROUTES)
