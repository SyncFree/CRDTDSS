-module(mfmn_vnode).
-behaviour(riak_core_vnode).
-include("mfmn.hrl").

-export([start_vnode/1,
         init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_exit/3]).

-export([
	new/5,
         value/4,
	 valueV/4,
         update/5
        ]).


-record(state, {partition, kv}).
%-record(crdt, {type, data}).
-record(value, {queue, lease, crdt}).

-define(MASTER, mfmn_vnode_master).

new(Preflist, ReqID, _, Key, Type) ->
    riak_core_vnode_master:command(Preflist,
				  {new, ReqID, Key, Type},
				   {fsm, undefined, self()},
				   ?MASTER).

update(Preflist, ReqID, Fetch, Key, Param) ->
    riak_core_vnode_master:command(Preflist,
                                   {update, ReqID, Fetch, Key, Param},
                                   {fsm, undefined, self()},
                                   ?MASTER).

value(Preflist, ReqID, _, Key) ->
    riak_core_vnode_master:command(Preflist,
                                   {value, ReqID, Key},
                                   {fsm, undefined, self()},
                                   ?MASTER).

valueV(Preflist, ReqID, Vclock, Key) ->
    riak_core_vnode_master:command(Preflist,
                                   {value, ReqID, Vclock, Key},
                                   {fsm, undefined, self()},
                                   ?MASTER).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    {ok, #state{partition=Partition, kv=dict:new()}}.

%% Sample command: respond to a ping
handle_command(ping, _Sender, State) ->
    {reply, {pong, State#state.partition}, State};

handle_command({value, ReqID, _, Key}, _Sender, State) ->
    case dict:find(Key, State#state.kv) of
	{ok, Value} ->
    	  TStamp=get_time_inseconds(),
	  R1 = get_first(Value),
	  Q = R1#value.queue,
	  Lease = erlang:trunc((TStamp - queue:get(Q))/(queue:len(Q) - 1)),
	  R2= #value{queue=Q, lease=Lease, crdt=R1#value.crdt},
    	  D0 = dict:erase(Key, State#state.kv),
    	  D1 = dict:append(Key, R2, D0),
	  Reply_value = mfmn_crdt_controller:value(R1#value.crdt),
	  {reply, {ReqID, Reply_value, Lease}, State#state{kv=D1}};
	error ->
	  {reply, {error, no_key}, State}
    end;

handle_command({value, ReqID, Key}, _Sender, State) ->
    case dict:find(Key, State#state.kv) of
	{ok, Value} ->
    	  TStamp=get_time_inseconds(),
	  R1 = get_first(Value),
	  Q = R1#value.queue,
	  Lease = erlang:trunc((TStamp - queue:get(Q))/(queue:len(Q) - 1)),
	  R2= #value{queue=Q, lease=Lease, crdt=R1#value.crdt},
    	  D0 = dict:erase(Key, State#state.kv),
    	  D1 = dict:append(Key, R2, D0),
	  Reply_value = mfmn_crdt_controller:value(R1#value.crdt),
	  {reply, {ReqID, Reply_value, Lease}, State#state{kv=D1}};
	error ->
	  {reply, {error, no_key}, State}
    end;

handle_command({new, ReqID, Key, Type}, _Sender, State) ->
	io:format("Handling New key: ~w~w",[Key, Type]),
	CRDT = mfmn_crdt_controller:new(Type),
	R = #value{queue=queue:new(), lease=?DefaultL, crdt= CRDT},
	D0 = dict:append(Key, R, State#state.kv),
	{reply, {ReqID, mfmn_crdt_controller:value(CRDT), ?DefaultL}, State#state{kv=D0}};    

handle_command({update, ReqID, Fetch, Key, Param}, _Sender, State) ->
    TStamp=get_time_inseconds(),
    case dict:find(Key, State#state.kv) of
	{ok, Value_list} ->
	    Old_record = get_first(Value_list),
	    NewCRDT = mfmn_crdt_controller:update(Old_record#value.crdt, Param, _Sender),
	    NewValue = mfmn_crdt_controller:value(NewCRDT),
	    Q1=Old_record#value.queue,
	    Length = queue:len(Q1),
	    if Length>=?L ->
		{_,Q2} = queue:out(Q1),
		Q3 = queue:in(TStamp, Q2);
	    true->
		Q3 = queue:in(TStamp, Q1)
   	    end,
	    Lease = erlang:trunc((TStamp - queue:get(Q3)) / max(queue:len(Q3) - 1, 1)),
	    Record= #value{queue=Q3, lease=Lease, crdt= NewCRDT},
    	    D0 = dict:erase(Key, State#state.kv),
    	    D1 = dict:append(Key, Record, D0);
	error ->
	    %Lease =
	    %D1 = State#state.kv,
	    %io:format("Error!Item not created!~n")
	    Q1=queue:new(),
	    Lease = ?DefaultL,
	    Record= #value{queue=queue:in(TStamp, Q1), lease=Lease, crdt=mfmn_crdt_controller:new(undefined)},
    	    D1 = dict:append(Key, Record, State#state.kv),
	    NewValue=Param
    end,
    if 
	Fetch =:= true ->
	  {reply, {ReqID, NewValue, Lease}, State#state{kv=D1}};
        true ->
	  {noreply, State#state{kv=D1}}
    end;
%handle_command({put, Key, Value}, _Sender, State) ->
%    D0 = dict:erase(Key, State#state.kv),
%    D1 = dict:append(Key, Value, D0),
%    {reply, {'put return', dict:fetch(Key, D1)}, #state{partition=State#state.partition, kv= D1} };
handle_command(Message, _Sender, State) ->
    ?PRINT({unhandled_command, Message}),
    {noreply, State}.

handle_handoff_command(_Message, _Sender, State) ->
    {noreply, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(_Data, State) ->
    {reply, ok, State}.

encode_handoff_item(_ObjectName, _ObjectValue) ->
    <<>>.

is_empty(State) ->
    {true, State}.

delete(State) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%private functions
get_first([Value|_]) -> Value.

get_time_inseconds() -> calendar:datetime_to_gregorian_seconds(calendar:universal_time()).
   
