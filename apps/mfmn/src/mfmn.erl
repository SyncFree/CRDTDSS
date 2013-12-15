-module(mfmn).
-include("mfmn.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         ping/0,
	 value/1,
	 inc/1,
	 new/2,
	 add/2
        ]).

-define(TIMEOUT, 5000).

%% Public API

%% @doc Pings a random vnode to make sure communication is functional
ping() ->
    DocIdx = riak_core_util:chash_key({<<"ping">>, term_to_binary(now())}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, mfmn),
    io:format("Printing preflist:~w~n",[PrefList]),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, ping, mfmn_vnode_master).

new(Key, Type) ->
   {ok, _} = mfmn_cache:new(Key, Type).

inc(Key) ->
   {ok, _} = mfmn_cache:update(Key, {increment, 1}).

add(Key, Elem) ->
   {ok, _} = mfmn_cache:update(Key, {add, Elem}).

%%write(Key, Value)->
    %%{ok, ReqID} = mfmn_cache:put(Key, Value),
    %%wait_for_reqid(ReqID, ?TIMEOUT).
    %%DocIdx = riak_core_util:chash_key({<<"key">>, term_to_binary(Key)}),
    %%PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, mfmn),
    %%[{IndexNode, _Type}] = PrefList,
    %%riak_core_vnode_master:sync_spawn_command(IndexNode, {put, Key, Value}, mfmn_vnode_master).

value(Key)->
    case mfmn_cache:value(Key) of
	{ok, Value} ->
	  {Key, Value};
	{wait, ReqID} ->
	  wait_for_reqid(ReqID, ?TIMEOUT)
    end. 

wait_for_reqid(ReqID, Timeout) ->
    receive
        {_, {ReqID, Key, Value}} -> {Key, Value};
        _ -> {error, unhandled_message_client}
    after Timeout ->
            {error, timeout}
    end.
