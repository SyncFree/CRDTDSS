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
         get/4,
         inc/5
        ]).


-record(state, {partition, kv}).
-record(value, {times, value}).

-define(MASTER, mfmn_vnode_master).

inc(Preflist, ReqID, Fetch, Key, Value) ->
    riak_core_vnode_master:command(Preflist,
                                   {inc, ReqID, Fetch, Key, Value},
                                   {fsm, undefined, self()},
                                   ?MASTER).

get(Preflist, ReqID, _, Key) ->
    riak_core_vnode_master:command(Preflist,
                                   {get, ReqID, Key},
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
handle_command({get, ReqID, Key}, _Sender, State) ->
    case dict:find(Key, State#state.kv) of
	{ok, Value} ->
	  {reply, {ReqID, get_first(Value), 5}, State};
	error ->
	  {reply, {error, no_key}, State}
    end;
    

handle_command({inc, ReqID, Fetch, Key, Value}, _Sender, State) ->
    case dict:find(Key, State#state.kv) of
	{ok, Value_list} ->
	    Old_record = get_first(Value_list),
		
	    NewValue=Old_record#value.value + Value,
	    New_queue=Old_record#value.times
	    Record= #value{times=,value=NewValue},
    	    D0 = dict:erase(Key, State#state.kv),
    	    D1 = dict:append(Key, NewValue, D0);
	error ->
    	    D1 = dict:append(Key, Value, State#state.kv),
	    NewValue=Value
    end,
    if 
	Fetch =:= true ->
	  {reply, {ReqID, NewValue, 5}, State#state{kv=D1}};
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
get_first(List) ->
   [Value|_] = List,
    Value.

get_time_insecond() -> calendar:datetime_to_gregorian_seconds(calendar:universal_time()).
