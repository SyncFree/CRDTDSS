-define(PRINT(Var), io:format("DEBUG: ~p:~p - ~p~n~n ~p~n~n", [?MODULE, ?LINE, ??Var, Var])).

%REPLICATION CONFIGURATION

%Replication factor
-define(N, 3).
%Not used
%-define(R, 2).
%-define(W, 2).

%LEASES CONFIGURATION

%Number of timestamps collected
-define(L, 5).
%Default Lease when a key is created
-define(DefaultL, 5).
