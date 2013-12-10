-define(PRINT(Var), io:format("DEBUG: ~p:~p - ~p~n~n ~p~n~n", [?MODULE, ?LINE, ??Var, Var])).

%Replication confirguration
-define(N, 3).
-define(R, 2).
-define(W, 2).

%Leases configuration
-define(L, 5).
