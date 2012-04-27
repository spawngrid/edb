-module(example_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    {ok, Pid} = example_sup:start_link(),
    init_database(),
    {ok, Pid}.

stop(_State) ->
    ok.

%% Internal
init_database() ->
    case application:get_env(skip_migrations, exampledb) of
        {ok, true} ->
            ok;
        _ ->
            Worker = poolboy:checkout(edb),
            Conn = edb:connection(Worker),
            Migs = sql_migration:migrations(example),
            sql_migration:migrate(Conn, hd(lists:reverse(Migs)), Migs),
            poolboy:checkin(edb, Worker)
    end.