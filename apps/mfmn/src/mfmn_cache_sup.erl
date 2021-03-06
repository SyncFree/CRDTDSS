%% @doc Supervise the mfmn_write FSM.
-module(mfmn_cache_sup).
-behavior(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Cache = {mfmn_cache,
                {mfmn_cache, start_link, []},
                permanent, 5000, worker, [mfmn_cache]},
    catch  {ok, {{one_for_one, 5, 10}, [Cache]}}.
