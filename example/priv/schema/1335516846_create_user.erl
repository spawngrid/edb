-module('1335516846_create_user').
-export([upgrade/1, downgrade/1]).
-behaviour(sql_migration).

upgrade(Conn) ->
        {ok, _, _} = pgsql:squery(Conn,
                "CREATE TABLE users ("
                "id SERIAL PRIMARY KEY,"
                "email VARCHAR(255) NOT NULL,"
                "password VARCHAR(255) NOT NULL,"
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
                ")").

downgrade(Conn) ->
        {ok, _, _} = pgsql:squery(Conn, "DROP TABLE users").
