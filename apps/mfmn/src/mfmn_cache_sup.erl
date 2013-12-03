%% @doc Supervise the mfmn_write FSM.
-module(mfmn_cache_sup).
-behavior(supervisor).

-export([start_write_fsm/1,
         start_link/0]).
-export([init/1]).


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_write_fsm(Args) ->
    supervisor:start_child(?MODULE, Args).

start_read_fsm(Args) ->
    supervisor:start_child(?MODULE, Args).

init([]) ->
    Cache = {mfmn_cache,
                {mfmn_cache, start_link, []},
                temporary, 5000, worker, [mfmn_cache]},
    {ok, {{simple_one_for_one, 10, 10}, [Cache]}}.
