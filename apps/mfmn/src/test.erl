-module(test).
-export([compute_lease/1, compute_lease/2, get_first/1]).

get_first([H|_]) -> H.

compute_lease(L) -> compute_lease(L, 0).

compute_lease([_|[]], Acc) ->
   Acc/(5-1);

compute_lease([X|T], Acc) ->
   io:format("~w ~w~n",[X,T]),
   compute_lease(T, Acc + get_first(T) - X).

