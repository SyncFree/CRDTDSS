-module(mfmn_client).

-export([start/0,connect/1, createClient/1, addObject/2, checkClient/1]).

start() ->
   ets:new(servertable,[private, named_table, set]).

createClient(ID) ->
   [{_,Server}] = ets:lookup(servertable, server),
   io:format("~w~n",[Server]),
   rpc:call(Server, mfmn, new, [ID, mfmn_dt_gset]).

addObject(ID, Elem) ->
   [{_,Server}] = ets:lookup(servertable, server),
   io:format("~w~n",[Server]),
   rpc:call(Server, mfmn, add, [ID, Elem]).

checkClient(ID) ->
   [{_,Server}] = ets:lookup(servertable, server),
   io:format("~w~n",[Server]),
   rpc:call(Server, mfmn, value, [ID]).

connect(Server) ->
  ets:insert(servertable, {server, Server}).  
