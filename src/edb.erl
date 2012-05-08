-module(edb).
-behaviour(gen_server).
-include_lib("epgsql/include/pgsql.hrl").

-export([start/0]).

-export([types/1, squery/1,equery/2,connection/1]).

-export([start_link/1, stop/0, init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-record(state, {conn}).

start() ->
    start_app(edb).

start_app(App) ->
    case application:start(App) of
        {error, {not_started, Dep}} ->
            start_app(Dep),
            start_app(App);
        Other ->
            Other
    end.

start_link(Args) -> gen_server:start_link(?MODULE, Args, []).
stop() -> gen_server:cast(?MODULE, stop).

init(Args) ->
    process_flag(trap_exit, true),

    [Hostname, Database, Username, Password, Port ] =
        [ proplists:get_value(N, Args) ||
            N <- [hostname, database, username, password, port ] ],

    CArgs = [Hostname] ++
        case Username of 
            undefined ->
                [];
            _ ->
                [Username, Password]
        end ++ [[case Port of undefined -> []; _ -> [{port, Port}] end|[{database, Database}]]],

    {ok, Conn} = apply(pgsql, connect, CArgs),

    {ok, #state{conn=Conn}}.

-define(WithWorker(Code), 
        Worker = poolboy:checkout(edb),
        Reply = Code,
        poolboy:checkin(edb, Worker),
        Reply).
        
        
types(Sql) ->
    ?WithWorker(gen_server:call(Worker, {types, Sql})).

squery(Sql) ->
    ?WithWorker(gen_server:call(Worker, {squery, Sql})).

equery(Stmt, Params) ->
    ?WithWorker(gen_server:call(Worker, {equery, Stmt, Params})).

connection(Worker) ->
    gen_server:call(Worker, connection).


handle_call({types, Sql}, _From, #state{conn=Conn}=State) ->
    case pgsql_connection:parse(Conn, "", Sql, []) of
        {ok, #statement{ types = Types }} ->
            {reply, {ok, Types}, State};
        Other ->
            {reply, Other, State}
    end;
handle_call({squery, Sql}, _From, #state{conn=Conn}=State) ->
    {reply, pgsql:squery(Conn, Sql), State};
handle_call({equery, Stmt, Params}, _From, #state{conn=Conn}=State) ->
    case pgsql_connection:parse(Conn, "", Stmt, []) of
        {ok, #statement{ types = Types }} ->
            CoercedParams = coerce(lists:zip(Params, Types)),
            {reply, pgsql:equery(Conn, Stmt, CoercedParams), State};
        Error ->
            {reply, Error, State}
    end;
handle_call(connection, _From, #state{ conn = Conn } = State) ->
    {reply, Conn, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(stop, State) ->
    {stop, shutdown, State};
handle_info({'EXIT', _, _}, State) ->
    {stop, shutdown, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{conn=Conn}) ->
    pgsql:close(Conn),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.    
    
%%% internal
coerce([]) ->
    [];
coerce([{Param,Type}|T]) ->
    [coerce_1(Param, Type)|coerce(T)].

coerce_1(<<>>, Num) when
                      (Num == int2 orelse
                       Num == int4 orelse
                       Num == int8) ->
    null;
coerce_1(B, Num) when is_binary(B) andalso
                      (Num == int2 orelse
                       Num == int4 orelse
                       Num == int8) ->
    {I,_} = string:to_integer(binary_to_list(B)),
    I;

coerce_1(<<>>, Num) when 
                      (Num == float4 orelse
                       Num == float8) ->
    null;
coerce_1(B, Num) when is_binary(B) andalso
                      (Num == float4 orelse
                       Num == float8) ->
    {F,_} = string:to_float(binary_to_list(B)),
    F;
coerce_1(undefined, _) ->
    null;
coerce_1(Other, _) ->
    Other.
