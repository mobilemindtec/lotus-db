%%%-------------------------------------------------------------------
%%% @author ricardo
%%% @copyright (C) 2022, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(lotus_db_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    lotus_db_sup:start_link().

stop(_State) ->
    ok.
