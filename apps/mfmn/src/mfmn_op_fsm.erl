%% @doc The coordinator for stat write opeartions.  This example will
%% show how to properly replicate your data in Riak Core by making use
%% of the _preflist_.
-module(mfmn_op_fsm).
-behavior(gen_fsm).
-include("mfmn.hrl").

%% API
-export([start_link/5]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([prepare/2, execute/2]).

-record(state, {req_id :: pos_integer(),
                from :: pid(),
		op :: atom(),
		key,
                val,
                preflist :: riak_core_apl:preflist2(),
                num_w = 0 :: non_neg_integer()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(ReqID, From, Op, Key, Val) ->
    io:format('The worker is about to start~n'),
    gen_fsm:start_link(?MODULE, [ReqID, From, Op, Key, Val], []).

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state data.
init([ReqID, From, Op, Key, Val]) ->
    SD = #state{req_id=ReqID,
                from=From,
		op=Op,
                key=Key,
                val=Val},
    {ok, prepare, SD, 0}.

%% @doc Prepare the write by calculating the _preference list_.
prepare(timeout, SD0=#state{key=Key}) ->
    DocIdx = riak_core_util:chash_key({<<"basic">>,
                                       term_to_binary(Key)}),
    Preflist = riak_core_apl:get_apl(DocIdx, ?N, mfmn),
    SD = SD0#state{preflist=Preflist},
    {next_state, execute, SD, 0}.

%% @doc Execute the write request and then go into waiting state to
%% verify it has meets consistency requirements.
execute(timeout, SD0=#state{req_id=ReqID,
                            op=Op,
			    key=Key,
                            val=Val,
                            preflist=Preflist}) ->
    mfmn_vnode:Op(Preflist, ReqID, Key, Val),
    {stop, normal, SD0}.

handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.

%%%===================================================================
%%% Internal Functions
%%%===================================================================
