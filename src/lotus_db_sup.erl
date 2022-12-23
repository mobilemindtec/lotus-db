%%%-------------------------------------------------------------------
%%% @author ricardo
%%% @copyright (C) 2022, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(lotus_db_sup).

-behaviour(supervisor).

-export([start_link/1, init/1]).

start_link(DbConfigs) ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, DbConfigs).

init(DbConfigs) ->
  %% Starting simple, and we'll be dynamically adding pools
  SupFlags = #{
    strategy=>one_for_one,
    intensity=>2000,
    period=>1
  },
  ChildSpecs = [
    #{id => lotus_db,
      start => {lotus_db, start_link, DbConfigs},
      restart => permanent,
      shutdown => brutal_kill,
      type => worker,
      modules => [lotus_db]}
    ],
  {ok, {SupFlags, ChildSpecs}}.
