-module(edb_sup).
-behaviour(esupervisor).
-include_lib("esupervisor/include/esupervisor.hrl").


%% API
-export([start_link/0, start_link/1]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    {ok, PgSQL} = application:get_env(edb, postgresql),
    start_link(PgSQL).
  
start_link(PgSQL) ->
    esupervisor:start_link({local, ?MODULE}, ?MODULE, [PgSQL]).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================


init([PgSQL]) ->
    PoolArgs = [{name, {local, edb}}, {worker_module, edb}, {size, 10}, {max_overflow, 20}] ++ PgSQL,
    #one_for_one{
      children = [
                  #worker{
                           id = edb,
                           restart = permanent,
                           start_func = {poolboy, start_link, [PoolArgs]}
                         }
                 ]
     }.

