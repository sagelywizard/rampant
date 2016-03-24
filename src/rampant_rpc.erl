-module(rampant_rpc).

-export([
    async/4,
    wait/2,
    prepare/4,
    commit/2,
    get/2
]).

-include("rampant.hrl").


-spec async(partition(), atom(), atom(), [any()]) -> any().
async(Partition, Mod, Fun, Args) ->
    rpc:async_call(Partition#partition.node, Mod, Fun, [Partition] ++ Args).


-spec wait([any()], integer()) -> {ok, [any()]} | {error, binary()}.
wait([], _Timeout) ->
    {ok, []};

wait(Refs, Timeout) ->
    wait_int(Refs, os:timestamp(), Timeout, []).


wait_int([], _Start, _Timeout, Acc) ->
    {ok, Acc};

wait_int([Ref | Refs], Start, Timeout, Acc) ->
    case timer:now_diff(Start, os:timestamp()) of
        Diff when Diff > Timeout ->
            {error, timeout};
        Diff ->
            case rpc:nb_yield(Ref, Timeout-Diff) of
                {value, Value} ->
                    wait_int(Refs, Start, Timeout, [Value | Acc]);
                timeout ->
                    {error, timeout}
            end
    end.


prepare(Partition, Timestamp, KVs, Metadata) ->
    rampant_partition:call(Partition, {prepare, Timestamp, KVs, Metadata}).


commit(Partition, Timestamp) ->
    rampant_partition:call(Partition, {commit, Timestamp}).


-spec get(partition(), [{binary(), timestamp()}]) -> {ok, any()} | {error, binary()}.
get(Partition, KTs) ->
    rampant_partition:call(Partition, {get, KTs}).
