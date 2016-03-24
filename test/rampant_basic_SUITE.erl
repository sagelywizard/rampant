-module(rampant_basic_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([all/0, groups/0]).
-export([init_per_group/2, end_per_group/2]).
-export([init_per_testcase/2]).
-export([
    create/1,
    client/1,
    write/1,
    read/1
]).


all() ->
    [{group, basic}].


groups() ->
    [{basic, [sequence], [create, client, write, read]}].


init_per_group(basic, Config) ->
    {ok, _} = application:ensure_all_started(rampant),
    Config.


end_per_group(basic, Config) ->
    ok = application:stop(rampant),
    Config.


init_per_testcase(write, Config) ->
    {ok, C} = rampant:client("test", []),
    [{client, C} | Config];

init_per_testcase(read, Config) ->
    {ok, C} = rampant:client("test", []),
    [{client, C} | Config];

init_per_testcase(_, Config) ->
    Config.


create(_Config) ->
    ok = rampant:create("test", []).


client(_Config) ->
    {ok, _C} = rampant:client("test", []).


write(Config) ->
    C = ?config(client, Config),
    ct:pal("Config: ~p~n", [Config]),
    ok = rampant:write(C, [{<<"k">>, <<"v">>}]).


read(Config) ->
    C = ?config(client, Config),
    {ok, KVs}= rampant:read(C, [<<"k">>]),
    {<<"v">>, {_ClientID, 1}} = proplists:get_value(<<"k">>, KVs).
