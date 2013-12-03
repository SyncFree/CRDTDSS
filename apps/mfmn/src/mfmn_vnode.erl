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


-record(state, {partition, kv=dict:new()}).

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
    {ok, #state { partition=Partition }}.

%% Sample command: respond to a ping
handle_command(ping, _Sender, State) ->
    {reply, {pong, State#state.partition}, State};
handle_command({get, ReqID, Key}, _Sender, State) ->
    case dict:find(Key) of
	{ok, Value} ->
	  {reply, {ReqID, Value, 30}, State};
	{error} ->
	  {reply, {error, no_key}, State}
    end;
    

handle_command({inc, ReqID, Fetch, Key, Value}, _Sender, State) ->
    case dict:find(Key) of
	{ok, Old_value} ->
	    NewValue=Old_value + Value,
    	    D0 = dict:erase(Key, State#state.kv),
    	    D1 = dict:append(Key, NewValue, D0);
	{error} ->
    	    D1 = dict:append(Key, Value, State#state.kv),
	    NewValue=Value
    end,
    if 
	Fetch =:= true ->
	  {reply, {ReqID, NewValue, 30}, State#state{kv=D1}};
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
