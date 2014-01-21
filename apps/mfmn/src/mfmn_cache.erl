-module(mfmn_cache).
-behavior(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([start_link/0, new/2, value/2, add_key/5, update/2]).

-record(value, {lease, crdt}).
-record(cache , {kv, pendingReqs}). 

% These are all wrappers for calls to the server

start_link() -> 
   gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

new(Key, Type) ->
   gen_server:call(?MODULE, {new, Key, Type}).

update(Key, Param) ->
   gen_server:call(?MODULE, {update, Key, {Param, node()}}).

value(Key, Consistency) ->
   gen_server:call(?MODULE, {value, Key, Consistency}).

add_key(PID, ReqID, Key, CRDT, Lease ) ->
   io:format("Calling add key~n"),
   gen_server:call(PID, {updateKey, Key, CRDT, Lease, ReqID}),
   io:format("Returning add key~n").
   

% This is called when a connection is made to the server
init([]) ->
	{ok, #cache{kv=dict:new(), pendingReqs=dict:new()}}.

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

handle_call({new, Key, Type}, _From, Cache) ->
	ReqID = get_reqid(),
	CRDT = mfmn_crdt_controller:new(Type),
	Lease = get_time_insecond() - 1,
	C0 =  mfmn_map:put(Key, #value{lease=Lease, crdt= CRDT}, Cache#cache.kv),
        %dict:append(Key,  #value{lease=Lease, crdt=CRDT}, Cache#cache.kv),
	mfmn_op_worker_fetch_sup:start_op_fetch_fsm([ReqID, self(), new, Key, Type]),
	{reply, {ok, ReqID}, Cache#cache{kv=C0}};


handle_call({update, Key, Param}, _From, Cache) ->
	ReqID = get_reqid(),
	case mfmn_map:find(Key, Cache#cache.kv) of
          {ok, Old_value} ->
            %C0 = dict:erase(Key, Cache#cache.kv),
	    %io:format("Has value~w~w~w~n",[Old_value#value.crdt, Param, self()]),
	    New_CRDT = mfmn_crdt_controller:update(Old_value#value.crdt, Param),
            C1 = mfmn_map:put(Key, #value{lease=Old_value#value.lease, crdt= New_CRDT}, Cache#cache.kv),
	    mfmn_op_worker_sup:start_op_fsm([ReqID, self(), update, Key, {false,Param}]);
          error ->
	    io:format("Fetching values from remote~n"),
	    C1 = Cache#cache.kv,
	    %C1 = dict:append(Key, Param, Cache#cache.kv),
	    %Lease = get_time_insecond()-1,
	    %io:format("Lease of current key:~w~n",[Lease]),
	    mfmn_op_worker_fetch_sup:start_op_fetch_fsm([ReqID, self(), update, Key, {true, Param}])
          end,
	{reply, {ok, ReqID}, Cache#cache{kv = C1}};

handle_call({value, Key, eventual}, _From, Cache) ->
	ReqID = get_reqid(),
	case mfmn_map:is_key(Key, Cache#cache.kv) of
         true ->
		Time = get_time_insecond(),
		Data = mfmn_map:get(Key, Cache#cache.kv),
		Lease = Data#value.lease,
		io:format("Now:~w Lease:~w ~n",[Time, Lease]),
		if  Time =< Lease ->
		    %if 1 =:= 1 ->
			io:format("Lease has not expired!"),
			%CRDT = get_first(dict:fetch(Key, Cache#cache.kv)),
			Value = mfmn_crdt_controller:value(Data#value.crdt),
			{reply, {ok, Value}, Cache};
		    true ->
			io:format("Fetching from remote..."),
			mfmn_op_worker_fetch_sup:start_op_fetch_fsm([ReqID, self(), value, Key, any]),
			%%When receives some message
			PQ = mfmn_map:put(ReqID, _From, Cache#cache.pendingReqs),
		    	{reply, {wait, ReqID}, Cache#cache{pendingReqs= PQ}}
		    end;
         false ->
		mfmn_op_worker_fetch_sup:start_op_fetch_fsm([ReqID, self(), value, Key, any]),
		%%When receives some message
		PQ = mfmn_map:put(ReqID, _From, Cache#cache.pendingReqs),
	    	{reply, {wait, ReqID}, Cache#cache{pendingReqs= PQ}}
        end;

handle_call({value, Key, strong}, _From, Cache) ->
        ReqID = get_reqid(),
        mfmn_op_worker_strong_sup:start_op_strong_fsm([ReqID, self(), value, Key, any]),
        %%When receives some message
        PQ = mfmn_map:put(ReqID, _From, Cache#cache.pendingReqs),
        {reply, {wait, ReqID}, Cache#cache{pendingReqs= PQ}};

handle_call({value, Key, Consistency}, _From, Cache) ->
        ReqID = get_reqid(),
	case Consistency of
	    %%TODO: Vectors not defined yet.
	    monotonic ->
		ComVC=[];
	    mywrites ->
		ComVC=[]
	end,
        case mfmn_map:is_key(Key, Cache#cache.kv) of
         true ->
                Data = mfmn_map:get(Key, Cache#cache.kv),
		VC = mfmn_crdt_controller:vclock(Data#value.crdt),
		Descends=vclock:descends(VC, ComVC),
		if Descends==true ->
                        Value = mfmn_crdt_controller:value(Data#value.crdt),
                        {reply, {ok, Value}, Cache};
                    true ->
                        io:format("Fetching from remote..."),
                        mfmn_op_worker_vclock_sup:start_op_vclock_fsm([ReqID, self(), value, Key, ComVC]),
                        %%When receives some message
                        PQ = mfmn_map:put(ReqID, _From, Cache#cache.pendingReqs),
                        {reply, {wait, ReqID}, Cache#cache{pendingReqs= PQ}}
                    end;
         false ->
                mfmn_op_worker_vclock_sup:start_op_vclock_fsm([ReqID, self(), value, Key, ComVC]),
                %%When receives some message
                PQ = mfmn_map:put(ReqID, _From, Cache#cache.pendingReqs),
                {reply, {wait, ReqID}, Cache#cache{pendingReqs= PQ}}
        end;

%%TODO: Need to figure out what value to update
handle_call({updateKey, Key, CRDT, Lease, ReqID}, _From, Cache) ->
	io:format("received updated value ~w  ~n",[CRDT]),
	CRDT_record = #value{crdt = CRDT, lease = Lease + get_time_insecond()},
	%K0 = dict:erase(Key, Cache#cache.kv),
	K1 = mfmn_map:put(Key, CRDT_record, Cache#cache.kv),
	IsKey = mfmn_map:is_key(ReqID, Cache#cache.pendingReqs),
	if IsKey=:=true ->
		io:format("Triggered~n"),
		Value = mfmn_crdt_controller:value(CRDT),
	   	gen_server:reply(mfmn_map:get(ReqID, Cache#cache.pendingReqs), {ReqID, Key, Value}),
		R0 = mfmn_map:erase(ReqID, Cache#cache.pendingReqs);
		true ->
		R0 = Cache#cache.pendingReqs
	end,
	io:format("Value updated~n"),
	{reply, {ok}, Cache#cache{kv= K1, pendingReqs=R0}}.

% We get compile warnings from gen_server unless we define these
handle_cast(_Message, Library) -> {noreply, Library}.
handle_info(_Message, Library) -> {noreply, Library}.
terminate(_Reason, _Library) -> ok.
code_change(_OldVersion, Library, _Extra) -> {ok, Library}.

%private function
get_time_insecond() -> calendar:datetime_to_gregorian_seconds(calendar:universal_time()).
get_reqid() -> erlang:phash2(erlang:now()).
%get_first(List) -> 
%   [Value|_] = List,
%    Value.
