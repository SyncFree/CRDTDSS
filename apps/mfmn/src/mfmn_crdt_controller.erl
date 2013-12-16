-module(mfmn_crdt_controller).
-export([new/1,update/2,value/1,vclock/1]).


new(Type)->
	{Type, Type:new()}.

update(CRDT, Param) ->
	{Type, Data} = CRDT,
	{OpParam , Actor} = Param,
	{ok, NewData} = Type:update(OpParam, Actor, Data),
	{Type, NewData}.

value(CRDT) ->
	%io:format("Value...~w~n",[CRDT]),
	{Type, Data} = CRDT,
	Type:value(Data).

vclock(CRDT) ->
	{Type, Data} = CRDT,
	Type:vclock(Data).



	
