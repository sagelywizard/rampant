-module(rampant_client).

-behaviour(gen_server).

-export([start_link/2]).

-include("rampant.hrl").

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(state, {
    name,
    type,
    map,
    seq,
    client_id
}).


start_link(Name, Options) ->
    gen_server:start_link(?MODULE, [Name, Options], []).


init([Name, _Options]) ->
    case rampant_map:lookup(Name) of
        {ok, Type, Map} ->
            State = #state{
                name=Name,
                type=Type,
                map=Map,
                seq=0,
                client_id=random:uniform(round(math:pow(2, 32)))
            },
            {ok, State};
        {error, Reason} ->
            {error, Reason}
    end.


handle_call({read, Keys}, _From, State) ->
    Result = case State#state.type of
        f ->
            GroupedKeys = rampant_map:group_keys_by_partition(State#state.map, Keys),
            Refs = lists:map(fun({Partition, KeyGroup}) ->
                KGs = [{Key, null} || Key <- KeyGroup],
                rampant_rpc:async(Partition, rampant_rpc, get, [KGs])
            end, GroupedKeys),
            case rampant_rpc:wait(Refs, 1000) of
                {ok, Responses} ->
                    % partition -> k -> ts
                    % Build dict of k->v, k->actual_ts, and k->should_ts
                    {Values, Should} = lists:foldl(fun({Key, Value, Timestamp, MD}, Acc) ->
                        {ValuesAcc, TSAcc} = Acc,
                        NewValuesAcc = dict:store(Key, {Value, Timestamp}, ValuesAcc),
                        NewTSAcc = lists:foldl(fun(MDKey, TSAccIn) ->
                            case dict:find(MDKey, TSAccIn) of
                                {ok, SmallerTS} when SmallerTS < Timestamp ->
                                    TSAccIn;
                                _Other ->
                                    dict:store(MDKey, Timestamp, TSAccIn)
                            end
                        end, TSAcc, MD),
                        {NewValuesAcc, NewTSAcc}
                    end, {dict:new(), dict:new()}, lists:flatten(Responses)),
                    Reread = dict:fold(fun(Key, ShouldTS, Acc) ->
                        case dict:find(Key, Values) of
                            {ok, {_Value, TS}} when TS < ShouldTS ->
                                [{Key, ShouldTS}|Acc];
                            error ->
                                Acc
                        end
                    end, [], Should),
                    Reqs = rampant_map:group_kvs_by_partition(State#state.map, Reread),
                    RereadRefs = lists:map(fun({Partition, KTs}) ->
                        rampant_rpc:async(Partition, rampant_rpc, get, [KTs])
                    end, Reqs),
                    case rampant_rpc:wait(RereadRefs, 1000) of
                        {ok, RereadResps} ->
                            Final = lists:foldl(fun(RereadResp, FinalValuesAcc) ->
                                {K, V, TS, _MD} = RereadResp,
                                dict:store(K, {V, TS}, FinalValuesAcc)
                            end, Values, RereadResps),
                            {ok, dict:to_list(Final)};
                        {error, Reason} ->
                            {error, Reason}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        s ->
            {error, not_implemented};
        h ->
            {error, not_implemented}
    end,
    {reply, Result, State};

handle_call({write, KVs}, _From, State) ->
    #state{name=Name} = State,
    {ok, _Type, Map} = rampant_map:lookup(Name),
    GroupedKVs = rampant_map:group_kvs_by_partition(Map, KVs),
    Seq = State#state.seq + 1,
    Timestamp = {State#state.client_id, Seq},
    PrepareRefs = lists:map(fun({Partition, PartitionKVs}) ->
        Metadata = lists:filtermap(fun(OtherPartitionKVs) ->
            case OtherPartitionKVs of
                {Partition, _KVs} ->
                    false;
                _ ->
                    {_Partition, OtherKVs} = OtherPartitionKVs,
                    {OtherKeys, _} = lists:unzip(OtherKVs),
                    {true, OtherKeys}
                end
        end, GroupedKVs),
        rampant_rpc:async(Partition, rampant_rpc, prepare, [Timestamp, PartitionKVs, Metadata])
    end, GroupedKVs),
    Reply = case rampant_rpc:wait(PrepareRefs, 1000) of
        {error, timeout} ->
            {error, timeout};
        {ok, Responses} ->
            true = lists:all(fun(R) -> R =:= prepared end, Responses),
            CommitRefs = lists:map(fun({Partition, _PKVs}) ->
                rampant_rpc:async(Partition, rampant_rpc, commit, [Timestamp])
            end, GroupedKVs),
            case rampant_rpc:wait(CommitRefs, 1000) of
                {ok, _Resps} ->
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end
    end,
    {reply, Reply, State#state{seq=Seq}}.


handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info(_Info, State) ->
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


