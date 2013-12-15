-module(mfmn_crdt_controller).
-export([new/1,update/3,value/1]).


new(Type)->
	{Type, Type:new()}.

update(CRDT, Param, Actor) ->
	{Type, Data} = CRDT,
	{ok, NewData} = Type:update(Param, Actor, Data),
	{Type, NewData}.

value(CRDT) ->
	io:format("Value...~w~n",[CRDT]),
	{Type, Data} = CRDT,
	Type:value(Data).





	
