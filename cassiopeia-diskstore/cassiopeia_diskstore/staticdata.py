from typing import Type, TypeVar, MutableMapping, Any, Iterable
import copy

from datapipelines import DataSource, DataSink, PipelineContext, Query, NotFoundError, validate_query

from cassiopeia.data import Platform, Region
from cassiopeia.dto.staticdata.champion import ChampionDto, ChampionListDto
from cassiopeia.dto.staticdata.item import ItemDto, ItemListDto
from cassiopeia.dto.staticdata.summonerspell import SummonerSpellDto, SummonerSpellListDto
from cassiopeia.dto.staticdata.version import VersionListDto
from cassiopeia.dto.staticdata.map import MapDto, MapListDto
from cassiopeia.dto.staticdata.realm import RealmDto
from cassiopeia.dto.staticdata.language import LanguagesDto, LanguageStringsDto
from cassiopeia.dto.staticdata.profileicon import ProfileIconDataDto
from cassiopeia.datastores.riotapi.staticdata import _get_default_locale, _get_latest_version
from cassiopeia.datastores.uniquekeys import convert_region_to_platform
from .common import SimpleKVDiskService

T = TypeVar("T")


class StaticDataDiskService(SimpleKVDiskService):

    @DataSource.dispatch
    def get(self, type: Type[T], query: MutableMapping[str, Any], context: PipelineContext = None) -> T:
        pass

    @DataSource.dispatch
    def get_many(self, type: Type[T], query: MutableMapping[str, Any], context: PipelineContext = None) -> Iterable[T]:
        pass

    @DataSink.dispatch
    def put(self, type: Type[T], item: T, context: PipelineContext = None) -> None:
        pass

    @DataSink.dispatch
    def put_many(self, type: Type[T], items: Iterable[T], context: PipelineContext = None) -> None:
        pass

    ############
    # Versions #
    ############

    _validate_get_versions_query = Query. \
        has("platform").as_(Platform)

    @get.register(VersionListDto)
    @validate_query(_validate_get_versions_query, convert_region_to_platform)
    def get_versions(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> VersionListDto:
        key = "{clsname}.{platform}".format(clsname=VersionListDto.__name__, platform=query["platform"].value)
        return VersionListDto(self._get(key))

    @put.register(VersionListDto)
    def put_versions(self, item: VersionListDto, context: PipelineContext = None) -> None:
        key = "{clsname}.{platform}".format(clsname=VersionListDto.__name__, platform=Region(item["region"]).platform.value)
        self._put(key, item)

    ##########
    # Realms #
    ##########

    _validate_get_realms_query = Query. \
        has("platform").as_(Platform)

    @get.register(RealmDto)
    @validate_query(_validate_get_realms_query, convert_region_to_platform)
    def get_realms(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> RealmDto:
        key = "{clsname}.{platform}".format(clsname=RealmDto.__name__, platform=query["platform"].value)
        return RealmDto(self._get(key))

    @put.register(RealmDto)
    def put_realms(self, item: RealmDto, context: PipelineContext = None) -> None:
        key = "{clsname}.{platform}".format(clsname=RealmDto.__name__, platform=Region(item["region"]).platform.value)
        self._put(key, item)

    #############
    # Champions #
    #############

    _validate_get_champion_query = Query. \
        has("id").as_(int).or_("name").as_(str).also. \
        has("platform").as_(Platform).also. \
        can_have("version").with_default(_get_latest_version, supplies_type=str).also. \
        can_have("locale").with_default(_get_default_locale, supplies_type=str).also. \
        can_have("includedData").with_default({"all"})

    @get.register(ChampionDto)
    @validate_query(_validate_get_champion_query, convert_region_to_platform)
    def get_champion(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> ChampionDto:
        champions_query = copy.deepcopy(query)
        if "id" in champions_query:
            champions_query.pop("id")
        if "name" in champions_query:
            champions_query.pop("name")
        champions = context[context.Keys.PIPELINE].get(ChampionListDto, query=champions_query)

        def find_matching_attribute(list_of_dtos, attrname, attrvalue):
            for dto in list_of_dtos:
                if dto.get(attrname, None) == attrvalue:
                    return dto

        if "id" in query:
            champion = find_matching_attribute(champions["data"].values(), "id", query["id"])
        elif "name" in query:
            champion = find_matching_attribute(champions["data"].values(), "name", query["name"])
        else:
            raise ValueError("Impossible!")
        if champion is None:
            raise NotFoundError
        champion["region"] = query["platform"].region.value
        champion["version"] = query["version"]
        champion["locale"] = query["locale"]
        champion["includedData"] = query["includedData"]
        return ChampionDto(champion)

    _validate_get_champion_list_query = Query. \
        has("platform").as_(Platform).also. \
        can_have("version").with_default(_get_latest_version, supplies_type=str).also. \
        can_have("locale").with_default(_get_default_locale, supplies_type=str).also. \
        can_have("includedData").with_default({"all"}).also. \
        can_have("dataById").with_default(True)

    @get.register(ChampionListDto)
    @validate_query(_validate_get_champion_list_query, convert_region_to_platform)
    def get_champion_list(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> ChampionListDto:
        platform = query["platform"].value
        version = query["version"]
        locale = query["locale"]
        included_data = "|".join(sorted(query["includedData"]))
        data_by_id = str(query["dataById"])
        key = "{clsname}.{platform}.{version}.{locale}.{included_data}.{data_by_id}".format(clsname=ChampionListDto.__name__,
                                                                                            platform=platform,
                                                                                            version=version,
                                                                                            locale=locale,
                                                                                            included_data=included_data,
                                                                                            data_by_id=data_by_id)
        data = self._get(key)
        data["data"] = {key: ChampionDto(champion) for key, champion in data["data"].items()}
        return ChampionListDto(data)

    @put.register(ChampionListDto)
    def put_champion_list(self, item: ChampionListDto, context: PipelineContext = None) -> None:
        platform = Region(item["region"]).platform.value
        included_data = "|".join(sorted(item["includedData"]))
        key = "{clsname}.{platform}.{version}.{locale}.{included_data}.{data_by_id}".format(clsname=ChampionListDto.__name__,
                                                                                            platform=platform,
                                                                                            version=item["version"],
                                                                                            locale=item["locale"],
                                                                                            included_data=included_data,
                                                                                            data_by_id=item["dataById"])
        self._put(key, item)

    #########
    # Items #
    #########

    _validate_get_item_query = Query. \
        has("id").as_(int).or_("name").as_(str).also. \
        has("platform").as_(Platform).also. \
        can_have("version").with_default(_get_latest_version, supplies_type=str).also. \
        can_have("locale").with_default(_get_default_locale, supplies_type=str).also. \
        can_have("includedData").with_default({"all"})

    @get.register(ItemDto)
    @validate_query(_validate_get_item_query, convert_region_to_platform)
    def get_item(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> ItemDto:
        items_query = copy.deepcopy(query)
        if "id" in items_query:
            items_query.pop("id")
        if "name" in items_query:
            items_query.pop("name")
        items = context[context.Keys.PIPELINE].get(ItemListDto, query=items_query)

        def find_matching_attribute(list_of_dtos, attrname, attrvalue):
            for dto in list_of_dtos:
                if dto.get(attrname, None) == attrvalue:
                    return dto

        if "id" in query:
            item = find_matching_attribute(items["data"].values(), "id", query["id"])
        elif "name" in query:
            item = find_matching_attribute(items["data"].values(), "name", query["name"])
        else:
            raise ValueError("Impossible!")
        if item is None:
            raise NotFoundError
        item["region"] = query["platform"].region.value
        item["version"] = query["version"]
        item["locale"] = query["locale"]
        item["includedData"] = query["includedData"]
        return ItemDto(item)

    _validate_get_item_list_query = Query. \
        has("platform").as_(Platform).also. \
        can_have("version").with_default(_get_latest_version, supplies_type=str).also. \
        can_have("locale").with_default(_get_default_locale, supplies_type=str).also. \
        can_have("includedData").with_default({"all"})

    @get.register(ItemListDto)
    @validate_query(_validate_get_item_list_query, convert_region_to_platform)
    def get_item_list(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> ItemListDto:
        platform = query["platform"].value
        version = query["version"]
        locale = query["locale"]
        included_data = "|".join(sorted(query["includedData"]))
        key = "{clsname}.{platform}.{version}.{locale}.{included_data}".format(clsname=ItemListDto.__name__,
                                                                               platform=platform,
                                                                               version=version,
                                                                               locale=locale,
                                                                               included_data=included_data)
        data = self._get(key)
        for key, item in data["data"].items():
            item = ItemDto(item)
            data["data"][key] = item
        return ItemListDto(data)

    @put.register(ItemListDto)
    def put_item_list(self, item: ItemListDto, context: PipelineContext = None) -> None:
        platform = Region(item["region"]).platform.value
        included_data = "|".join(sorted(item["includedData"]))
        key = "{clsname}.{platform}.{version}.{locale}.{included_data}".format(clsname=ItemListDto.__name__,
                                                                               platform=platform,
                                                                               version=item["version"],
                                                                               locale=item["locale"],
                                                                               included_data=included_data)
        self._put(key, item)

    ##################
    # SummonerSpells #
    ##################

    _validate_get_summoner_spell_query = Query. \
        has("id").as_(int).or_("name").as_(str).also. \
        has("platform").as_(Platform).also. \
        can_have("version").with_default(_get_latest_version, supplies_type=str).also. \
        can_have("locale").with_default(_get_default_locale, supplies_type=str).also. \
        can_have("includedData").with_default({"all"})

    @get.register(SummonerSpellDto)
    @validate_query(_validate_get_summoner_spell_query, convert_region_to_platform)
    def get_summoner_spell(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> SummonerSpellDto:
        summoner_spells_query = copy.deepcopy(query)
        if "id" in summoner_spells_query:
            summoner_spells_query.pop("id")
        if "name" in summoner_spells_query:
            summoner_spells_query.pop("name")
        summoner_spells = context[context.Keys.PIPELINE].get(SummonerSpellListDto, query=summoner_spells_query)

        def find_matching_attribute(list_of_dtos, attrname, attrvalue):
            for dto in list_of_dtos:
                if dto.get(attrname, None) == attrvalue:
                    return dto

        if "id" in query:
            summoner_spell = find_matching_attribute(summoner_spells["data"].values(), "id", query["id"])
        elif "name" in query:
            summoner_spell = find_matching_attribute(summoner_spells["data"].values(), "name", query["name"])
        else:
            raise ValueError("Impossible!")
        if summoner_spell is None:
            raise NotFoundError
        summoner_spell["region"] = query["platform"].region.value
        summoner_spell["version"] = query["version"]
        summoner_spell["locale"] = query["locale"]
        summoner_spell["includedData"] = query["includedData"]
        return SummonerSpellDto(summoner_spell)

    _validate_get_summoner_spell_list_query = Query. \
        has("platform").as_(Platform).also. \
        can_have("version").with_default(_get_latest_version, supplies_type=str).also. \
        can_have("locale").with_default(_get_default_locale, supplies_type=str).also. \
        can_have("includedData").with_default({"all"})

    @get.register(SummonerSpellListDto)
    @validate_query(_validate_get_summoner_spell_list_query, convert_region_to_platform)
    def get_summoner_spell_list(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> SummonerSpellListDto:
        platform = query["platform"].value
        version = query["version"]
        locale = query["locale"]
        included_data = "|".join(sorted(query["includedData"]))
        key = "{clsname}.{platform}.{version}.{locale}.{included_data}".format(clsname=SummonerSpellListDto.__name__,
                                                                               platform=platform,
                                                                               version=version,
                                                                               locale=locale,
                                                                               included_data=included_data)
        return SummonerSpellListDto(self._get(key))

    @put.register(SummonerSpellListDto)
    def put_summoner_spell_list(self, item: SummonerSpellListDto, context: PipelineContext = None) -> None:
        platform = Region(item["region"]).platform.value
        included_data = "|".join(sorted(item["includedData"]))
        key = "{clsname}.{platform}.{version}.{locale}.{included_data}".format(clsname=SummonerSpellListDto.__name__,
                                                                               platform=platform,
                                                                               version=item["version"],
                                                                               locale=item["locale"],
                                                                               included_data=included_data)
        self._put(key, item)

    ########
    # Maps #
    ########

    _validate_get_map_query = Query. \
        has("id").as_(int).or_("name").as_(str).also. \
        has("platform").as_(Platform).also. \
        can_have("version").with_default(_get_latest_version, supplies_type=str).also. \
        can_have("locale").with_default(_get_default_locale, supplies_type=str)

    @get.register(MapDto)
    @validate_query(_validate_get_map_query, convert_region_to_platform)
    def get_map(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> MapDto:
        maps_query = copy.deepcopy(query)
        if "id" in maps_query:
            maps_query.pop("id")
        if "name" in maps_query:
            maps_query.pop("name")
        maps = context[context.Keys.PIPELINE].get(MapListDto, query=maps_query)

        def find_matching_attribute(list_of_dtos, attrname, attrvalue):
            for dto in list_of_dtos:
                if dto.get(attrname, None) == attrvalue:
                    return dto

        if "id" in query:
            map = find_matching_attribute(maps["data"].values(), "mapId", str(query["id"]))
        elif "name" in query:
            map = find_matching_attribute(maps["data"].values(), "mapName", query["name"])
        else:
            raise ValueError("Impossible!")
        if map is None:
            raise NotFoundError
        map["region"] = query["platform"].region.value
        map["version"] = query["version"]
        map["locale"] = query["locale"]
        return MapDto(map)

    _validate_get_map_list_query = Query. \
        has("platform").as_(Platform).also. \
        can_have("version").with_default(_get_latest_version, supplies_type=str).also. \
        can_have("locale").with_default(_get_default_locale, supplies_type=str)

    @get.register(MapListDto)
    @validate_query(_validate_get_map_list_query, convert_region_to_platform)
    def get_map_list(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> MapListDto:
        platform = query["platform"].value
        version = query["version"]
        locale = query["locale"]
        key = "{clsname}.{platform}.{version}.{locale}".format(clsname=MapListDto.__name__,
                                                               platform=platform,
                                                               version=version,
                                                               locale=locale)
        return MapListDto(self._get(key))

    @put.register(MapListDto)
    def put_map_list(self, item: MapListDto, context: PipelineContext = None) -> None:
        platform = Region(item["region"]).platform.value
        key = "{clsname}.{platform}.{version}.{locale}".format(clsname=MapListDto.__name__,
                                                               platform=platform,
                                                               version=item["version"],
                                                               locale=item["locale"])
        self._put(key, item)

    #################
    # Profile Icons #
    #################

    _validate_get_profile_icons_query = Query. \
        has("platform").as_(Platform).also. \
        can_have("version").as_(str).also. \
        can_have("locale").with_default(_get_default_locale, supplies_type=str)

    @get.register(ProfileIconDataDto)
    @validate_query(_validate_get_profile_icons_query, convert_region_to_platform)
    def get_profile_icons(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> ProfileIconDataDto:
        platform = query["platform"].value
        version = query["version"]
        locale = query["locale"]
        key = "{clsname}.{platform}.{version}.{locale}".format(clsname=ProfileIconDataDto.__name__,
                                                               platform=platform,
                                                               version=version,
                                                               locale=locale)
        return ProfileIconDataDto(self._get(key))

    @put.register(ProfileIconDataDto)
    def put_profile_icons(self, item: ProfileIconDataDto, context: PipelineContext = None) -> None:
        platform = Region(item["region"]).platform.value
        key = "{clsname}.{platform}.{version}.{locale}".format(clsname=ProfileIconDataDto.__name__,
                                                               platform=platform,
                                                               version=item["version"],
                                                               locale=item["locale"])
        self._put(key, item)

    ############
    # Language #
    ############

    _validate_get_languages_query = Query. \
        has("platform").as_(Platform)

    @get.register(LanguagesDto)
    @validate_query(_validate_get_languages_query, convert_region_to_platform)
    def get_language(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> LanguagesDto:
        platform = query["platform"].value
        key = "{clsname}.{platform}".format(clsname=LanguagesDto.__name__, platform=platform)
        return LanguagesDto(self._get(key))

    _validate_get_many_languages_query = Query. \
        has("platforms").as_(Iterable)

    @put.register(LanguagesDto)
    def put_language(self, item: LanguagesDto, context: PipelineContext = None) -> None:
        platform = Region(item["region"]).platform.value
        key = "{clsname}.{platform}".format(clsname=LanguagesDto.__name__, platform=platform)
        self._put(key, item)

    ####################
    # Language Strings #
    ####################

    _validate_get_language_strings_query = Query. \
        has("platform").as_(Platform).also. \
        can_have("version").as_(str).also. \
        can_have("locale").with_default(_get_default_locale, supplies_type=str)

    @get.register(LanguageStringsDto)
    @validate_query(_validate_get_language_strings_query, convert_region_to_platform)
    def get_language_strings(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> LanguageStringsDto:
        platform = query["platform"].value
        version = query["version"]
        locale = query["locale"]
        key = "{clsname}.{platform}.{version}.{locale}".format(clsname=LanguageStringsDto.__name__,
                                                               platform=platform,
                                                               version=version,
                                                               locale=locale)
        return LanguageStringsDto(self._get(key))

    _validate_get_many_language_strings_query = Query. \
        has("platforms").as_(Iterable).also. \
        can_have("version").as_(str).also. \
        can_have("locale").as_(str)

    @put.register(LanguageStringsDto)
    def put_language_strings(self, item: LanguageStringsDto, context: PipelineContext = None) -> None:
        platform = Region(item["region"]).platform.value
        key = "{clsname}.{platform}.{version}.{locale}".format(clsname=LanguageStringsDto.__name__,
                                                               platform=platform,
                                                               version=item["version"],
                                                               locale=item["locale"])
        self._put(key, item)
