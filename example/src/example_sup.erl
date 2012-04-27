-module(example_sup).
-behaviour(esupervisor).
-include_lib("esupervisor/include/esupervisor.hrl").


%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================
  
start_link() ->
    esupervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================


init([]) ->
    {ok, PgSQL} = application:get_env(example, postgresql),
    #one_for_one{
      children = [
                  #worker{
                           id = edb,
                           restart = permanent,
                           start_func = {edb_sup, start_link, [PgSQL]},
                           shutdown = infinity
                         }
                 ]
     }.

