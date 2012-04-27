-module(edb).
-behaviour(gen_server).

-export([start/0]).

-export([squery/1,equery/2,connection/1]).

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
        
        
squery(Sql) ->
    ?WithWorker(gen_server:call(Worker, {squery, Sql})).

equery(Stmt, Params) ->
    ?WithWorker(gen_server:call(Worker, {equery, Stmt, Params})).

connection(Worker) ->
    gen_server:call(Worker, connection).


handle_call({squery, Sql}, _From, #state{conn=Conn}=State) ->
    {reply, pgsql:squery(Conn, Sql), State};
handle_call({equery, Stmt, Params}, _From, #state{conn=Conn}=State) ->
    {reply, pgsql:equery(Conn, Stmt, Params), State};
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
    

