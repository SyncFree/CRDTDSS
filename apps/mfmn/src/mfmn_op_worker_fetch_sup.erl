%% @doc Supervise the mfmn_write FSM.
-module(mfmn_op_worker_fetch_sup).
-behavior(supervisor).

-export([start_op_fetch_fsm/1,
         start_link/0]).
-export([init/1]).


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_op_fetch_fsm(Args) ->
    io:format('Creating a new worker/child fetch~n'),
    supervisor:start_child(?MODULE, Args).

init([]) ->
    Worker = {mfmn_op_fetch_fms,
                {mfmn_op_fetch_fsm, start_link, []},
                transient, 5000, worker, [mfmn_op_fetch_fms]},
    {ok, {{simple_one_for_one, 10, 10}, [Worker]}}.
