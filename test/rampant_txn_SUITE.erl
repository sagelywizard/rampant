-module(rampant_txn_SUITE).

-export([
    all/0,
    groups/0,
    init_per_group/2,
    end_per_group/2,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([read_write/1]).

-include_lib("common_test/include/ct.hrl").

all() ->
    [{group, p}].

groups() ->
    [
        {p, [parallel, {repeat, 20}], [
            read_write,
            read_write,
            read_write,
            read_write,
            read_write
        ]}
    ].

init_per_group(p, Config) ->
    {ok, _} = application:ensure_all_started(rampant),
    Node = node(),
    Range = 4294967296 / 16,
    {_, Map} = lists:foldl(fun(_I, {RangeAcc, MapAcc}) ->
        NextRange = RangeAcc + Range,
        {NextRange, [{Node, round(RangeAcc), round(NextRange-1)} | MapAcc]}
    end, {0, []}, lists:seq(1, 16)),
    ok = rampant:create("test", [{map, Map}]),
    Keys = [gen_binary(16) || _ <- lists:seq(1, 128)],
    {ok, C} = rampant:client("test", []),
    [{client, C}, {keys, Keys} | Config];

init_per_group(_, Config) ->
    Config.

end_per_group(_GroupName, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

read_write(Config) ->
    Keys = ?config(keys, Config),
    C = ?config(client, Config),
    Value = gen_binary(128),
    ok = rampant:write(C, [{Key, Value} || Key <- Keys]),
    {ok, Values} = rampant:read(C, Keys),
    ReturnedKeys = lists:sort([Key || {Key, {_Value, _TS}} <- Values]),
    ReturnedKeys = lists:sort(Keys),
    ReturnedValues = [NewValue || {_Key, {NewValue, _TS}} <- Values],
    true = lists:all(fun(V) -> V =:= hd(ReturnedValues) end, ReturnedValues).


gen_binary(Bytes) ->
    base64:encode(crypto:strong_rand_bytes(Bytes)).
