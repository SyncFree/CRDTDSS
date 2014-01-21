-module(mfmn_crdt_controller).
-export([new/1,update/2,value/1,vclock/1, merge/1, merge/2]).


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

vclock({Type, Data}) ->
	Type:vclock(Data).

merge({Type1, CRDT1}, {Type2, CRDT2}) ->
	if Type1==Type2 ->
		{Type1, Type1:merge(CRDT1, CRDT2)};
	true ->
		error
	end.
merge([H|T])->mergeA(T, H).

mergeA([_|[]], Acc)->
	Acc;

mergeA([H|T],Acc)->
	mergeA(T,merge(H, Acc)).
