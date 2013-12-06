-module(mfmn).
-include("mfmn.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         ping/0,
	 read/1,
	 inc/1
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

inc(Key) ->
   {ok, _} = mfmn_cache:inc(Key, 1).

%%write(Key, Value)->
    %%{ok, ReqID} = mfmn_cache:put(Key, Value),
    %%wait_for_reqid(ReqID, ?TIMEOUT).
    %%DocIdx = riak_core_util:chash_key({<<"key">>, term_to_binary(Key)}),
    %%PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, mfmn),
    %%[{IndexNode, _Type}] = PrefList,
    %%riak_core_vnode_master:sync_spawn_command(IndexNode, {put, Key, Value}, mfmn_vnode_master).

read(Key)->
    case mfmn_cache:get(Key) of
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
