-module(rampant_partition).

-behaviour(gen_server).

-include("rampant.hrl").

-export([start_link/3]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-export([start/3]).

-export([
    call/2,
    to_name/1
]).

-record(state, {
    name,
    range_start,
    range_end,
    versions,
    last_commit
}).


call(Partition, Message) ->
    Name = to_name(Partition),
    #partition{start_key=Start, end_key=End} = Partition,
    {ok, Pid} = case whereis(Name) of
        undefined ->
            case start(Name, Start, End) of
                {error, {already_started, Pid0}} ->
                    {ok, Pid0};
                Other ->
                    Other
            end;
        Pid0 ->
            {ok, Pid0}
    end,
    gen_server:call(Pid, Message).


start(Name, Start, End) ->
    ChildSpec = {
        to_name(Name, Start, End),
        {rampant_partition, start_link, [Name, Start, End]},
        permanent,
        5000,
        worker,
        [rampant_partition]
    },
    supervisor:start_child(rampant_partition_sup, ChildSpec).


start_link(Name, Start, End) ->
    gen_server:start_link({local, to_name(Name, Start, End)}, ?MODULE, [Name, Start, End], []).


init([Name, Start, End]) ->
    State = #state{
        name=Name,
        range_start=Start,
        range_end=End,
        versions=ets:new(versions, [set]),
        last_commit=ets:new(last_commit, [set])
    },
    {ok, State}.


% KTs is a list of {Key, Timestamp}
handle_call({get, KTs}, _From, State) ->
    #state{last_commit=LCTab, versions=VTab} = State,
    Reply = lists:map(fun(KT) ->
        case KT of
            {Key, null} ->
                case ets:lookup(LCTab, Key) of
                    [] ->
                        {Key, undefined, null, []};
                    [{_Key, Value, Timestamp}] ->
                        [{_Timestamp, _KVs, Metadata}] = ets:lookup(VTab, Timestamp),
                        {Key, Value, Timestamp, Metadata}
                end;
            {Key, Timestamp} ->
                [{_Timestamp, KVs, _Metadata}] = ets:lookup(VTab, Timestamp),
                {Key, proplists:get_value(Key, KVs)}
        end
    end, KTs),
    {reply, Reply, State};

handle_call({prepare, Timestamp, KVs, Metadata}, _From, State) ->
    true = ets:insert(State#state.versions, {Timestamp, KVs, Metadata}),
    {reply, prepared, State};

handle_call({commit, Timestamp}, _From, State) ->
    [{_, KVs, _Metadata}] = ets:lookup(State#state.versions, Timestamp),
    lists:foreach(fun({Key, Value}) ->
        true = case ets:lookup(State#state.last_commit, Key) of
            [{_Key, _Value, OldTimestamp}] when OldTimestamp >= Timestamp ->
                true;
            _ ->
                ets:insert(State#state.last_commit, {Key, Value, Timestamp})
        end
    end, KVs),
    {reply, committed, State}.


handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info(_Info, State) ->
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


to_name(Partition) ->
    #partition{name=Name, start_key=Start, end_key=End} = Partition,
    to_name(Name, Start, End).


to_name(Name, Start, End) ->
    % TODO: Don't use list_to_atom
    list_to_atom(lists:concat(['partition_', Name, '_', Start, '_', End])).
