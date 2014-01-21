%% @doc The coordinator for stat write opeartions.  This example will
%% show how to properly replicate your data in Riak Core by making use
%% of the _preflist_.
-module(mfmn_op_strong_fsm).
-behavior(gen_fsm).
-include("mfmn.hrl").

%% API
-export([start_link/5]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([prepare/2, execute/2, waiting/2]).

-record(state, {req_id :: pos_integer(),
                from :: pid(),
		op :: atom(),
		key,
                preflist :: riak_core_apl:preflist2(),
		param,
		lease,
		replies,
                num_r = 0 :: non_neg_integer()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(ReqID, From, Op, Key, Param) ->
    io:format('The worker is about to start~n'),
    gen_fsm:start_link(?MODULE, [ReqID, From, Op, Key, Param], []).

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state data.
init([ReqID, From, Op, Key, Param]) ->
    SD = #state{req_id=ReqID,
                from=From,
		op=Op,
                key=Key,
		param=Param,
		lease=infinite,
		replies=[]},
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
			    param=Param,
                            preflist=Preflist}) ->
    mfmn_vnode:Op(Preflist, ReqID, Key, Param),
    io:format("Operations sent (Strong consisncy)~n"),
    {next_state, waiting, SD0, ?OPTIMEOUT}.

%% @doc Waits for 1 write reqs to respond.
waiting({ReqID, Val, Lease}, SD0=#state{from=From, key=Key, lease=LeaseStored, replies=Replies, num_r=NumR}) ->
    io:format("I have received one reply (Strong consisncy)~n"),
    Replies2=lists:append(Replies, [Val]),
    NumR2=NumR+1,
    if LeaseStored==infinite ->
	Lease2=Lease;
    true->
	if Lease<LeaseStored ->
		Lease2=Lease;
	true->
		Lease2=LeaseStored
	end
    end,
    SD = SD0#state{lease=Lease2, replies=Replies2, num_r=NumR2},
    io:format("NumR2 ~w~n",[NumR2]),
    if NumR2==?N ->
	io:format("Sending the merged reply (Strong consistency)~n"),
    	mfmn_cache:add_key(From, ReqID, Key, mfmn_crdt_controller:merge(Replies2), Lease2),
   	{stop, normal, SD};
    true ->
	{next_state, waiting, SD, ?OPTIMEOUT}
    end;

waiting(timeout, SD) ->
    {stop, normal, SD};

waiting({error, no_key}, SD0=#state{num_r=R}) ->
    NextR=R+1,
    SD = SD0#state{num_r=NextR},
    if NextR==?N ->
        {stop, normal, SD};
    true->
        {next_state, waiting, SD, ?OPTIMEOUT}
    end.

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
