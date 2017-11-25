from typing import TypeVar, Type, Set, Iterable, Mapping, List

from datapipelines import CompositeDataSource, CompositeDataSink

from .common import SimpleKVDiskService

T = TypeVar("T")


def _default_services(path: str = None, expirations: Mapping[type, float] = None, plugins: List[str] = None) -> Set[SimpleKVDiskService]:
    if plugins is None:
        plugins = []
    from .staticdata import StaticDataDiskService
    from .champion import ChampionDiskService
    from .summoner import SummonerDiskService
    from .championmastery import ChampionMasteryDiskService
    from .match import MatchDiskService
    from .spectator import SpectatorDiskService
    from .status import ShardStatusDiskService
    from .leagues import LeaguesDiskService
    from .patch import PatchDiskService

    services = {
        StaticDataDiskService(path, expirations=expirations, plugins=plugins),
        ChampionDiskService(path, expirations=expirations, plugins=plugins),
        SummonerDiskService(path, expirations=expirations, plugins=plugins),
        ChampionMasteryDiskService(path, expirations=expirations, plugins=plugins),
        MatchDiskService(path, expirations=expirations, plugins=plugins),
        SpectatorDiskService(path, expirations=expirations, plugins=plugins),
        ShardStatusDiskService(path, expirations=expirations, plugins=plugins),
        LeaguesDiskService(path, expirations=expirations, plugins=plugins),
        PatchDiskService(path, expirations=expirations, plugins=plugins)
    }
    if "ChampionGG" in plugins:
        from .championgg import ChampionGGDiskService
        services.add(ChampionGGDiskService(path, expirations=expirations, plugins=plugins))

    return services


class SimpleKVDiskStore(CompositeDataSource, CompositeDataSink):
    def __init__(self, path: str = None, expirations: Mapping[type, float] = None, services: Iterable[SimpleKVDiskService] = None, plugins: List[str] = None):
        if services is None:
            services = _default_services(path=path, expirations=expirations, plugins=plugins)

        CompositeDataSource.__init__(self, services)
        CompositeDataSink.__init__(self, services)

    def clear(self, type: Type[T] = None):
        sinks = {sink for many_sinks in self._sinks.values() for sink in many_sinks}
        for sink in sinks:
            for key in sink._store:
                if type is None:
                    sink._store.delete(key)
                elif key.startswith("{}.".format(type.__name__)):
                        sink._store.delete(key)

    def delete(self, item: Type[T]):
        raise NotImplemented
