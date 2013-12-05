-module(mfmn_cache).
-behavior(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([start_link/0, put/2, get/1, add_key/5, inc/2]).

-record(cache , {kv=dict:new(), lease=dict:new(), pendingReqs=dict:new()}). 

% These are all wrappers for calls to the server

start_link() -> 
   io:format('Starting cache process'),
   gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

put(Key, Value) ->
   gen_server:call(?MODULE, {put, Key, Value}).

inc(Key, Value) ->
   gen_server:call(?MODULE, {inc, Key, Value}).

get(Key) ->
   gen_server:call(?MODULE, {get, Key}).

add_key(PID, ReqID, Key, Value, Lease ) ->
   gen_server:call(PID, {updateKey, Key, Value, Lease, ReqID}).

% This is called when a connection is made to the server
init([]) ->
	{ok, #cache{}}.

% handle_call is invoked in response to gen_server:call
% handle_call({put, Key, Value}, _From, Cache) ->
%       case dict:is_key(Key, Cache#cache.kv) of
%	 true ->
%        	C0 = dict:erase(Key, Cache#cache.kv),
%        	C1 = dict:append(Key, Value, C0),
%		mfmn_cache_sup:start_write_fsm({get_reqid(), self(), put, false, Key, Value})
%         false ->
%		C1 = dict:append(Key, Value, Cache#cache.kv),
%		mfmn_cache_sup:start_write_fsm({get_reqid(), self(), put, true, Key, Value})
%        end,
%	{reply, {ok}, C1};

handle_call({inc, Key, Value}, _From, Cache) ->
	ReqID = get_reqid(),
	case dict:find(Key, Cache#cache.kv) of
          {ok, Value} ->
            Old_value = dict:fetch(Key, Cache#cache.kv),
            D0 = dict:erase(Key, Cache#cache.kv),
            C1 = dict:append(Key, Old_value + Value, D0),
	    mfmn_op_worker_sup:start_op_fsm([ReqID, self(), put, false, Key, Value]);
          {error} ->
	    C1 = dict:append(Key, Value, Cache#cache.kv),
	    mfmn_op_worker_sup:start_op_fsm([ReqID, self(), put, true, Key, Value])
          end,
	{reply, {ok, ReqID}, C1};

handle_call({get, Key}, _From, Cache) ->
	ReqID = get_reqid(),
	case dict:is_key(Key, Cache#cache.kv) of
         true ->
		Time = get_time_insecond(),
		Value = dict:fetch(Key, Cache#cache.lease),
		if  Time =< Value ->
		    %if 1 =:= 1 ->
			{reply, {ok,dict:fetch(Key, Cache#cache.kv)}, Cache};
		    true ->
			mfmn_cache_sup:start_op_fsm([ReqID, self(), get, true, Key, undefined]),
			%%When receives some message
			PQ = dict:addpend(ReqID, _From, Cache#cache.pendingReqs),
		    	{reply, {wait, ReqID}, Cache#cache{pendingReqs= PQ}}
		    end;
         false ->
		mfmn_cache_sup:start_read_fsm([ReqID, self(), get, true, Key, undefined]),
		%%When receives some message
		PQ = dict:addpend(ReqID, _From, Cache#cache.pendingReqs),
	    	{reply, {wait, ReqID}, Cache#cache{pendingReqs= PQ}}
        end;

handle_call({updateValue, Key, Value, Lease, ReqID}, _From, Cache) ->
	K0 = dict:erase(Key, Cache#cache.kv),
	L0 = dict:erase(Key, Cache#cache.lease),
	K1 = dict:append(Key, Value, K0),
	L1 = dict:append(Key, Lease + get_time_insecond(), L0),
	IsKey = dict:is_key(ReqID, Cache#cache.pendingReqs),
	if IsKey=:=true ->
	   	gen_server:reply(dict:fetch(ReqID, Cache#cache.pendingReqs), {ReqID, Key, Value}),
		R0 = dict:erase(ReqID, Cache#cache.pendingReqs);
		true ->
		R0 = Cache#cache.pendingReqs
	end,
	{noreply, Cache#cache{kv=K1, lease=L1, pendingReqs=R0}}.

% We get compile warnings from gen_server unless we define these
handle_cast(_Message, Library) -> {noreply, Library}.
handle_info(_Message, Library) -> {noreply, Library}.
terminate(_Reason, _Library) -> ok.
code_change(_OldVersion, Library, _Extra) -> {ok, Library}.

%private function
get_time_insecond() -> calendar:datetime_to_gregorian_seconds(calendar:universal_time()).
get_reqid() -> erlang:phash2(erlang:now()).
