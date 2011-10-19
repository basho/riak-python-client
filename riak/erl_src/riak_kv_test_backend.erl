%% -------------------------------------------------------------------
%%
%% riak_kv_test_backend: storage engine based on ETS tables
%%
%%
%% -------------------------------------------------------------------

% @doc riak_kv_test_backend is a Riak storage backend using ets that
%      exposes a reset function for efficiently clearing stored data.

-module(riak_kv_test_backend).
-behavior(riak_kv_backend).
-behavior(gen_server).
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
-export([start/2,stop/1,get/2,put/3,list/1,list_bucket/2,delete/2,
         is_empty/1, drop/1, fold/3, callback/3, reset/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).


% @type state() = term().
-record(state, {t, p}).

% @spec start(Partition :: integer(), Config :: proplist()) ->
%                        {ok, state()} | {{error, Reason :: term()}, state()}
start(Partition, _Config) ->
    gen_server:start_link(?MODULE, [Partition], []).

% @spec reset() -> ok | {error, timeout}
reset() ->
    Pids = lists:foldl(fun(Item, Acc) ->
                               case lists:prefix("test_backend", atom_to_list(Item)) of
                                   true -> [whereis(Item)|Acc];
                                   _ -> Acc
                               end
                       end, [], registered()),
    [gen_server:cast(Pid,{reset, self()})|| Pid <- Pids],
    receive_reset(Pids).

receive_reset([]) -> ok;
receive_reset(Pids) ->
    receive
        {reset, Pid} ->
            receive_reset(lists:delete(Pid, Pids))
    after 1000 ->
            {error, timeout}
    end.

%% @private
init([Partition]) ->
    PName = list_to_atom("test_backend" ++ integer_to_list(Partition)),
    P = list_to_atom(integer_to_list(Partition)),
    register(PName, self()),
    {ok, #state{t=ets:new(P,[]), p=P}}.

%% @private
handle_cast({reset,From}, State) ->
    ets:delete_all_objects(State#state.t),
    From ! {reset, self()},
    {noreply, State};
handle_cast(_, State) -> {noreply, State}.

%% @private
handle_call(stop,_From,State) -> {reply, srv_stop(State), State};
handle_call({get,BKey},_From,State) -> {reply, srv_get(State,BKey), State};
handle_call({put,BKey,Val},_From,State) ->
    {reply, srv_put(State,BKey,Val),State};
handle_call({delete,BKey},_From,State) -> {reply, srv_delete(State,BKey),State};
handle_call(list,_From,State) -> {reply, srv_list(State), State};
handle_call({list_bucket,Bucket},_From,State) ->
    {reply, srv_list_bucket(State, Bucket), State};
handle_call(is_empty, _From, State) ->
    {reply, ets:info(State#state.t, size) =:= 0, State};
handle_call(drop, _From, State) ->
    ets:delete(State#state.t),
    {reply, ok, State};
handle_call({fold, Fun0, Acc}, _From, State) ->
    Fun = fun({{B,K}, V}, AccIn) -> Fun0({B,K}, V, AccIn) end,
    Reply = ets:foldl(Fun, Acc, State#state.t),
    {reply, Reply, State}.

% @spec stop(state()) -> ok | {error, Reason :: term()}
stop(SrvRef) -> gen_server:call(SrvRef,stop).
srv_stop(State) ->
    true = ets:delete(State#state.t),
    ok.

% get(state(), riak_object:bkey()) ->
%   {ok, Val :: binary()} | {error, Reason :: term()}
% key must be 160b
get(SrvRef, BKey) -> gen_server:call(SrvRef,{get,BKey}).
srv_get(State, BKey) ->
    case ets:lookup(State#state.t,BKey) of
        [] -> {error, notfound};
        [{BKey,Val}] -> {ok, Val};
        Err -> {error, Err}
    end.

% put(state(), riak_object:bkey(), Val :: binary()) ->
%   ok | {error, Reason :: term()}
% key must be 160b
put(SrvRef, BKey, Val) -> gen_server:call(SrvRef,{put,BKey,Val}).
srv_put(State,BKey,Val) ->
    true = ets:insert(State#state.t, {BKey,Val}),
    ok.

% delete(state(), riak_object:bkey()) ->
%   ok | {error, Reason :: term()}
% key must be 160b
delete(SrvRef, BKey) -> gen_server:call(SrvRef,{delete,BKey}).
srv_delete(State, BKey) ->
    true = ets:delete(State#state.t, BKey),
    ok.

% list(state()) -> [riak_object:bkey()]
list(SrvRef) -> gen_server:call(SrvRef,list).
srv_list(State) ->
    MList = ets:match(State#state.t,{'$1','_'}),
    list(MList,[]).
list([],Acc) -> Acc;
list([[K]|Rest],Acc) -> list(Rest,[K|Acc]).

% list_bucket(term(), Bucket :: riak_object:bucket()) -> [Key :: binary()]
list_bucket(SrvRef, Bucket) ->
    gen_server:call(SrvRef,{list_bucket, Bucket}).
srv_list_bucket(State, {filter, Bucket, Fun}) ->
    MList = lists:filter(Fun, ets:match(State#state.t,{{Bucket,'$1'},'_'})),
    list(MList,[]);
srv_list_bucket(State, Bucket) ->
    case Bucket of
        '_' -> MatchSpec = {{'$1','_'},'_'};
        _ -> MatchSpec = {{Bucket,'$1'},'_'}
    end,
    MList = ets:match(State#state.t,MatchSpec),
    list(MList,[]).

is_empty(SrvRef) -> gen_server:call(SrvRef, is_empty).

drop(SrvRef) -> gen_server:call(SrvRef, drop).

fold(SrvRef, Fun, Acc0) -> gen_server:call(SrvRef, {fold, Fun, Acc0}, infinity).

%% Ignore callbacks for other backends so multi backend works
callback(_State, _Ref, _Msg) ->
    ok.

%% @private
handle_info(_Msg, State) -> {noreply, State}.

%% @private
terminate(_Reason, _State) -> ok.

%% @private
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%
%% Test
%%
-ifdef(TEST).

% @private
simple_test() ->
    riak_kv_backend:standard_test(?MODULE, []).

-ifdef(EQC).
%% @private
eqc_test() ->
    ?assertEqual(true, backend_eqc:test(?MODULE, true)).

-endif. % EQC
-endif. % TEST
