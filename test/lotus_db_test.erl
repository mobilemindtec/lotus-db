-module(lotus_db_test).

-include_lib("eunit/include/eunit.hrl").


-compile(export_all).

% rebar3 eunit --module=lotus_db_test --sys_config=test/test.config 


test_setup() ->
  ?debugMsg("** setup"),
	{ok, DbCfg} = application:get_env(lotus_db, configs),
	lotus_db:start_link(DbCfg),
	case file:read_file("test/database.sql") of
		{ok, Contents} ->
			0 = db:qu(Contents);
		{error, Reason} ->
			?debugFmt("ERROR: read file ./database.sql: ~p", [Reason]),
			throw(file_read_error)
	end.

test_teardown(_) ->
    ?debugMsg("** teardown").

t_test_() ->
  {setup,
   fun test_setup/0,
   fun test_teardown/1,
   [
     fun test_insert_employee/0,
     fun test_join/0,
     fun test_native_select/0
   ]}.

test_insert_employee() ->

	SqlInsert = "insert into employees ( name, created_at, updated_at ) values ( ?, ?, ? )",
	SqlSelect = "select id, name, created_at, updated_at from employees where id = ?",
	SqlUpdate = "update employees set name = ? where id = ?",
	SqDelete = "delete from employees where id = ?",
	SqCount = "select count(*) from employees",
	Name = <<"Will Smith">>,
	{ok, #{ id := Id }, {SqlInsert, _}} = lotus_db:save(employees, #{name => Name}, #{ return => sql }),	
	?assert(Id > 0),
	{ok, #{id := Id, name := Name }, {SqlSelect, _}} = lotus_db:first(employees, #{where => [id, Id], return => sql}),
	{ok, 1, {SqlUpdate, _}} = lotus_db:update(employees, #{name => <<"Tom">>}, #{where => [id, Id], return => sql}),
  {ok, 1, {SqCount, _}} = lotus_db:count(employees, #{return => sql}),
  {ok, 1, {SqDelete, _}} = lotus_db:delete(employees, #{where => [id, Id], return => sql}),
  {ok, [], {"select name e_name from employees", _}}  = lotus_db:list(employees, #{ select => #{name => e_name}, return => sql}),
	ok.

test_join() ->
  Joins = #{
    alias => emp,
    select => ['emp.id', 'dept_emps.id'],
    join => [
      #{table => dept_emps,
        on => [id, employee_id],
        join => #{table => departments,
                  alias => deps,
                  on => [department_id, id]
        }
      }
    ],
    where => [['emp.id', 1], ['dept_emps.id', 1]],
    return => sql
  },
  {ok, [], {"select emp.id, dept_emps.id from employees emp left join dept_emps on emp.id = dept_emps.employee_id left join departments deps on dept_emps.department_id = deps.id where emp.id = ? and dept_emps.id = ?", _}}  = lotus_db:list(employees, Joins).

test_native_select() ->
  NativeSelect = #{
    where => #{
      native => "exists (select id from employees where id = ?)",
      args => [5]
    },
    return => sql
  },
  {ok, [], {"select id, name, created_at, updated_at from employees where exists (select id from employees where id = ?)", _}}  = lotus_db:list(employees, NativeSelect).


%orm_read_test() ->
%	ok = sql_bridge:start(),
%	Configs = get_db_configs(),
%	{ok, Pid} = lotus_db:start(Configs),
%	{ok, Result} = lotus_db:list(Pid, users, [{debug, true}, {limit, 1}, {offset,0}, {sort, [id]}, {where, [id, 1]}]),
%	%?debugFmt("ORM result = ~p", [Result]),
%	{ok, Result2} = lotus_db:first(Pid, users, [{debug, true}, {limit, 1}, {offset,1}, {sort, [[id, desc]]}, {where, [[id, 1], [name, ne, "x"]]}]),
%	%?debugFmt("ORM result = ~p", [Result2]),
%	lotus_db:first(Pid, users, [{debug, true}, {limit, 1}, {offset,1}, {sort, [[id, desc]]}, {where, [[id, in, [1,2]], [name, ne, "x"]]}]),
%	{ok, Count} = lotus_db:count(Pid, users, [{debug, true}, {limit, 1}, {offset,1}, {sort, [[id, desc]]}, {where, [[id, in, [1,2]], [name, ne, "x"]]}]),
%	%?debugFmt("Count = ~p", [Count]),
%	{ok, Page} = lotus_db:page(Pid, users, [{debug, true}, {limit, 1}, {offset,1}, {sort, [[id, desc]]}, {where, [[id, in, [1,2]], [name, ne, "x"]]}]),
%	%?debugFmt("Page = ~p", [Page]),
%	{ok, Exists} = lotus_db:exists(Pid, users, [{debug, true}, {limit, 1}, {offset,1}, {sort, [[id, desc]]}, {where, [[id, in, [1,2]], [name, ne, "x"]]}]).
%	%?debugFmt("Exists = ~p", [Exists]).
%
%orm_save_test() ->	
%	ok = sql_bridge:start(),
%	Configs = get_db_configs(),
%	%{ok, Pid} = lotus_db:start(Configs),
%	{ok, Saved} = lotus_db:save(chat_users, [
%		  {user_id, 1}
%		, {admin, true}
%		, {provider_id, 1}
%		, {last_login_at, undefined}
%		, {last_message_at, undefined}
%		, {created_at, calendar:local_time()}
%		, {updated_at, calendar:local_time()}
%	]),
%	?debugFmt("Saved = ~p", [Saved]).
%
%
%orm_merge_test() ->	
%	ok = sql_bridge:start(),
%	Configs = get_db_configs(),
%	%{ok, Pid} = lotus_db:start(Configs),
%	{ok, Merged} = lotus_db:merge(chat_users, [
%		  {id, 1}
%		, {user_id, 1}
%		, {admin, true}
%		, {provider_id, 1}
%		, {last_login_at, undefined}
%		, {last_message_at, undefined}
%	]),
%	?debugFmt("Merged = ~p", [Merged]).
%
%orm_update_test() ->	
%	ok = sql_bridge:start(),
%	Configs = get_db_configs(),
%	%{ok, Pid} = lotus_db:start(Configs),
%	Data = [
%		{admin, false}
%	],
%	Options = [{debug, true}, {where, [id, 1]}],
%	{ok, Updated} = lotus_db:update(chat_users, Options, Data),
%	?debugFmt("Updated = ~p", [Updated]).
%
%orm_persist_test() ->	
%	ok = sql_bridge:start(),
%	Configs = get_db_configs(),
%	%{ok, Pid} = lotus_db:start(Configs),
%	Data = [
%		  {id, 1}
%		, {user_id, 1}
%		, {admin, true}
%		, {provider_id, 1}
%		, {last_login_at, undefined}
%		, {last_message_at, undefined}
%	],
%	{ok, Persisted} = lotus_db:persist(chat_users, Data),
%	?debugFmt("Persisted = ~p", [Persisted]),
%	Data2 = [
%		  {user_id, 1}
%		, {admin, true}
%		, {provider_id, 1}
%		, {last_login_at, undefined}
%		, {last_message_at, undefined}
%	],
%	Persisted2 = lotus_db:persist(chat_users, Data2),
%	?debugFmt("Persisted2 = ~p", [Persisted2]).
%
%orm_remove_test() ->	
%	ok = sql_bridge:start(),
%	Configs = get_db_configs(),
%	%{ok, Pid} = lotus_db:start(Configs),
%	Data = [
%		{id, 1}
%	],
%	{ok, Removed} = lotus_db:remove(chat_users, Data),
%	?debugFmt("Removed = ~p", [Removed]).
%
%orm_delete_test() ->	
%	ok = sql_bridge:start(),
%	Configs = get_db_configs(),
%	%{ok, Pid} = lotus_db:start(Configs),
%	Options = [{debug, true}, {where, [[admin, true], [id, 1]]}],
%	{ok, Deleted} = lotus_db:delete(chat_users, Options),
%	?debugFmt("Deleted = ~p", [Deleted]).	
%
%orm_projection_test() ->		
%	Projection = lotus_db:list(users, [
%		{debug, true}, 
%		{projections, [id]}, 
%		{sort, [id]}, 
%		{where, [id, 1]}
%	]),
%	?debugFmt("Projection = ~p", [Projection]).
%
%orm_group_by_test() ->		
%	{ok, Group} = lotus_db:list(users, [
%		{debug, true}, 
%		{projections, ["distinct(tenant_id)"]}, 
%		{group, [tenant_id]},
%		{limit,1}
%	]),
%	?debugFmt("Group = ~p", [Group]).	