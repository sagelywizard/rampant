-module(rampant_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(CHILD(Mod, Type), {Mod, {Mod, start_link, []}, permanent, 5000, Type, [Mod]}).


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


init([]) ->
    ok = rampant_map:init(),
    Specs = [
        ?CHILD(rampant_client_sup, worker),
        ?CHILD(rampant_partition_sup, worker)
    ],
    {ok, {{one_for_one, 5, 10}, Specs}}.
