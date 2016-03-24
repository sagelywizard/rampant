-module(rampant).

-export([
    start/0,
    create/1,
    create/2,
    client/1,
    client/2,
    read/2,
    write/2
]).

-include("rampant.hrl").


start() ->
    application:ensure_all_started(rampant).


-spec create(database_name()) -> ok | {error, binary()}.
create(Name) ->
    create(Name, []).

-spec create(database_name(), proplist()) -> ok | {error, binary()}.
create(Name, Options) ->
    % Get partition map
    Nodes = [node()|nodes()],
    Range = 4294967296 / length(Nodes),
    {_, DefaultMap} = lists:foldl(fun(Node, {RangeAcc, MapAcc}) ->
        NextRange = RangeAcc + Range,
        {NextRange, [{Node, round(RangeAcc), round(NextRange-1)} | MapAcc]}
    end, {0, []}, Nodes),
    Map = proplists:get_value(map, Options, DefaultMap),
    rampant_map:create(Name, Map).


-spec client(any()) -> {ok, pid()} | {error, any()}.
client(Name) ->
    client(Name, []).


-spec client(any(), proplist()) -> {ok, pid()} | {error, any()}.
client(Name, Options) ->
    ChildSpec = {
        {rampant_client, Name},
        {rampant_client, start_link, [Name, Options]},
        permanent,
        5000,
        worker,
        [rampant_client]
    },
    case supervisor:start_child(rampant_client_sup, ChildSpec) of
        {error, {already_started, Pid}} ->
            {ok, Pid};
        {error, {{bad_return_value, {error, not_found}}, _}} ->
            {error, not_found};
        Other ->
            Other
    end.


-spec read(pid(), [binary()]) -> {ok, [kv()]} | {error, any()}.
read(Client, Keys) ->
    gen_server:call(Client, {read, Keys}).


write(Client, KVs) ->
    gen_server:call(Client, {write, KVs}).
