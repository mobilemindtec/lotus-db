-module(lotus_db_test).

-include_lib("eunit/include/eunit.hrl").


-compile(export_all).

% rebar3 eunit --module=lotus_db_test --sys_config=test/test.config 

get_db_configs() ->
	[{tables, [
      {chat_messages, [
        {columns, [ id
                  , {created_at, [{auto, created}]}
                  , {updated_at, [{auto, updated}]}
                  , {uuid, [{auto, uuid}]}
                  , from_user_id
                  , to_user_id
                  , owner_id
                  , message
                  , read_at
                  , received_at]}
      ]},
      {chat_users, [
        {columns, [{id, [{key,auto}]}
                  , user_id
                  , admin
                  , provider_id
                  , last_login_at
                  , last_message_at
                  , {created_at, [{auto, created}]}
                  , {updated_at, [{auto, updated}]}]}
      ]},
      {users, [
        {columns, [{id, [{key,auto}]}
                  , name
                  , user_name
                  , password
                  , token
                  , tenant_id]}
      ]}
    ]}].

orm_read_test() ->
	ok = sql_bridge:start(),
	Configs = get_db_configs(),
	{ok, Pid} = lotus_db:start(Configs),
	{ok, Result} = lotus_db:list(Pid, users, [{debug, true}, {limit, 1}, {offset,0}, {sort, [id]}, {where, [id, 1]}]),
	%?debugFmt("ORM result = ~p", [Result]),
	{ok, Result2} = lotus_db:first(Pid, users, [{debug, true}, {limit, 1}, {offset,1}, {sort, [[id, desc]]}, {where, [[id, 1], [name, ne, "x"]]}]),
	%?debugFmt("ORM result = ~p", [Result2]),
	lotus_db:first(Pid, users, [{debug, true}, {limit, 1}, {offset,1}, {sort, [[id, desc]]}, {where, [[id, in, [1,2]], [name, ne, "x"]]}]),
	{ok, Count} = lotus_db:count(Pid, users, [{debug, true}, {limit, 1}, {offset,1}, {sort, [[id, desc]]}, {where, [[id, in, [1,2]], [name, ne, "x"]]}]),
	%?debugFmt("Count = ~p", [Count]),
	{ok, Page} = lotus_db:page(Pid, users, [{debug, true}, {limit, 1}, {offset,1}, {sort, [[id, desc]]}, {where, [[id, in, [1,2]], [name, ne, "x"]]}]),
	%?debugFmt("Page = ~p", [Page]),
	{ok, Exists} = lotus_db:exists(Pid, users, [{debug, true}, {limit, 1}, {offset,1}, {sort, [[id, desc]]}, {where, [[id, in, [1,2]], [name, ne, "x"]]}]).
	%?debugFmt("Exists = ~p", [Exists]).

orm_save_test() ->	
	ok = sql_bridge:start(),
	Configs = get_db_configs(),
	%{ok, Pid} = lotus_db:start(Configs),
	{ok, Saved} = lotus_db:save(chat_users, [
		  {user_id, 1}
		, {admin, true}
		, {provider_id, 1}
		, {last_login_at, undefined}
		, {last_message_at, undefined}
		, {created_at, calendar:local_time()}
		, {updated_at, calendar:local_time()}
	]),
	?debugFmt("Saved = ~p", [Saved]).


orm_merge_test() ->	
	ok = sql_bridge:start(),
	Configs = get_db_configs(),
	%{ok, Pid} = lotus_db:start(Configs),
	{ok, Merged} = lotus_db:merge(chat_users, [
		  {id, 1}
		, {user_id, 1}
		, {admin, true}
		, {provider_id, 1}
		, {last_login_at, undefined}
		, {last_message_at, undefined}
	]),
	?debugFmt("Merged = ~p", [Merged]).

orm_update_test() ->	
	ok = sql_bridge:start(),
	Configs = get_db_configs(),
	%{ok, Pid} = lotus_db:start(Configs),
	Data = [
		{admin, false}
	],
	Options = [{debug, true}, {where, [id, 1]}],
	{ok, Updated} = lotus_db:update(chat_users, Options, Data),
	?debugFmt("Updated = ~p", [Updated]).

orm_persist_test() ->	
	ok = sql_bridge:start(),
	Configs = get_db_configs(),
	%{ok, Pid} = lotus_db:start(Configs),
	Data = [
		  {id, 1}
		, {user_id, 1}
		, {admin, true}
		, {provider_id, 1}
		, {last_login_at, undefined}
		, {last_message_at, undefined}
	],
	{ok, Persisted} = lotus_db:persist(chat_users, Data),
	?debugFmt("Persisted = ~p", [Persisted]),
	Data2 = [
		  {user_id, 1}
		, {admin, true}
		, {provider_id, 1}
		, {last_login_at, undefined}
		, {last_message_at, undefined}
	],
	Persisted2 = lotus_db:persist(chat_users, Data2),
	?debugFmt("Persisted2 = ~p", [Persisted2]).

orm_remove_test() ->	
	ok = sql_bridge:start(),
	Configs = get_db_configs(),
	%{ok, Pid} = lotus_db:start(Configs),
	Data = [
		{id, 1}
	],
	{ok, Removed} = lotus_db:remove(chat_users, Data),
	?debugFmt("Removed = ~p", [Removed]).

orm_delete_test() ->	
	ok = sql_bridge:start(),
	Configs = get_db_configs(),
	%{ok, Pid} = lotus_db:start(Configs),
	Options = [{debug, true}, {where, [[admin, true], [id, 1]]}],
	{ok, Deleted} = lotus_db:delete(chat_users, Options),
	?debugFmt("Deleted = ~p", [Deleted]).	

orm_projection_test() ->		
	Projection = lotus_db:list(users, [
		{debug, true}, 
		{projections, [id]}, 
		{sort, [id]}, 
		{where, [id, 1]}
	]),
	?debugFmt("Projection = ~p", [Projection]).

orm_group_by_test() ->		
	{ok, Group} = lotus_db:list(users, [
		{debug, true}, 
		{projections, ["distinct(tenant_id)"]}, 
		{group, [tenant_id]},
		{limit,1}
	]),
	?debugFmt("Group = ~p", [Group]).	