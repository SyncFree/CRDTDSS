-module(mfmn_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(_Args) ->
    VMaster = { mfmn_vnode_master,
                  {riak_core_vnode_master, start_link, [mfmn_vnode]},
                  permanent, 5000, worker, [riak_core_vnode_master]},
    CacheSup = { mfmn_cache_sup,
                  {mfmn_cache_sup, start_link, []},
                  permanent, 5000, supervisor, [mfmn_cache_sup]},
    OpsSup = { mfmn_op_worker_sup,
                  {mfmn_op_worker_sup, start_link, []},
                  permanent, 5000, supervisor, [mfmn_op_worker_sup]},

    { ok,
        { {one_for_one, 5, 10},
          [VMaster, CacheSup, OpsSup]}}.
