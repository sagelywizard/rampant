-module(rampant_map).

-export([
    init/0,
    create/2,
    lookup/1,
    delete/1,
    group_keys_by_partition/2,
    group_kvs_by_partition/2
]).

-include("rampant.hrl").


-spec init() -> ok | {error, binary()}.
init() ->
    Nodes = [node() | nodes()],
    {ok, _} = mnesia:change_config(extra_db_nodes, Nodes),
    CreateTable = mnesia:create_table(
        rampant_maps,
        [{attributes, [name, type, map]}, {ram_copies, Nodes}]
    ),
    case CreateTable of
        {atomic, ok} -> ok;
        {aborted, {already_exists, rampant_maps}} -> ok;
        Error -> {error, Error}
    end.


-spec create(database_name(), partition_map()) -> ok | {error, binary()}.
create(Name, Map) ->
    RealMap = lists:map(fun(Partition) ->
        case Partition of
            #partition{} ->
                Partition;
            {Node, Start, End} ->
                #partition{
                    name=Name,
                    node=Node,
                    start_key=Start,
                    end_key=End
                }
        end
    end, Map),
    T = fun() -> mnesia:write({rampant_maps, Name, f, RealMap}) end,
    case mnesia:transaction(T) of
        {atomic, ok} -> ok;
        Error -> {error, Error}
    end.


-spec lookup(database_name()) -> {ok, partition_map()} | {error, any()}.
lookup(Name) ->
    T = fun() -> mnesia:read(rampant_maps, Name, write) end,
    case mnesia:transaction(T) of
        {atomic, []} -> {error, not_found};
        {atomic, [{rampant_maps, _Name, Type, Map}]} -> {ok, Type, Map}
    end.


-spec delete(database_name()) -> ok | {error, any()}.
delete(_Name) ->
    {error, not_implemented}.


-spec group_keys_by_partition(partition_map(), [binary()]) -> [{partition(), [binary()]}].
group_keys_by_partition(Map, Keys) ->
    SortedHashKeys = lists:sort([{erlang:crc32(K), K} || K <- Keys]),
    % Sort map by startkey
    SortedMap = lists:keysort(#partition.start_key, Map),
    {GroupedKeys, _} = lists:foldl(fun({Hash, Key}, Acc) ->
        {GroupedKeysAcc, PartsAcc} = Acc,
        [Partition | Rest] = PartsAcc,
        case Hash =< Partition#partition.end_key of
            true ->
                {dict:append(Partition, Key, GroupedKeysAcc), PartsAcc};
            false ->
                {dict:append(hd(Rest), Key, GroupedKeysAcc), Rest}
        end
    end, {dict:new(), SortedMap}, SortedHashKeys),
    dict:to_list(GroupedKeys).


-spec group_kvs_by_partition([partition()], [kv()]) -> [{partition(), [kv()]}].
group_kvs_by_partition(Map, KVs) ->
    SortedHashKVs = lists:sort([{erlang:crc32(K), {K, V}} || {K, V} <- KVs]),
    % Sort map by startkey
    SortedMap = lists:keysort(#partition.start_key, Map),
    {GroupedKVs, _} = lists:foldl(fun({Hash, KV}, Acc) ->
        {GroupedKVsAcc, PartsAcc} = Acc,
        [Partition | Rest] = PartsAcc,
        case Hash =< Partition#partition.end_key of
            true ->
                {dict:append(Partition, KV, GroupedKVsAcc), PartsAcc};
            false ->
                {dict:append(hd(Rest), KV, GroupedKVsAcc), Rest}
        end
    end, {dict:new(), SortedMap}, SortedHashKVs),
    dict:to_list(GroupedKVs).
