-module(lotus_db).

-behaviour(gen_server).

-include("include/lotus_db.hrl").
-include_lib("eunit/include/eunit.hrl").

%-export_type([options/0, criteria/0, sort/0, tuple_opts/0]).

%% server events
-export([
	start/1,
	start_link/1,
	init/1,
	handle_call/3
]).

%% server API
-export([
	list/2,
	list/3,
	first/2,
	first/3,
	count/2,
	count/3,
	page/2,
	page/3,
	exists/2,
	exists/3,
	save/2,
	save/3,
	update/3,
	update/4,
	delete/2,
	delete/3,
	persist/2,
	persist/3,
	merge/2,
	merge/3,
	remove/2,
	remove/3
]).


start(DbConfigs) ->
	ok = sql_bridge:start(),
	gen_server:start({global, ?MODULE}, ?MODULE, [DbConfigs], []).

start_link(DbConfigs) ->
	ok = sql_bridge:start(),
	gen_server:start_link({global, ?MODULE}, ?MODULE, [DbConfigs], []).

init([DbConfigs]) ->
	{ok, #state{ configs = DbConfigs }}.

 %% api
list(TableName, Options) -> list({global, ?MODULE}, TableName, Options).
list(Pid, TableName, Options) ->
	gen_server:call(Pid, {list, TableName, Options}).

first(TableName, Options) -> first({global, ?MODULE}, TableName, Options).
first(Pid, TableName, Options) ->
	gen_server:call(Pid, {first, TableName, Options}).

count(TableName, Options) -> count({global, ?MODULE}, TableName, Options).
count(Pid, TableName, Options) ->
	gen_server:call(Pid, {count, TableName, Options}).

exists(TableName, Options) -> exists({global, ?MODULE}, TableName, Options).
exists(Pid, TableName, Options) ->
	gen_server:call(Pid, {exists, TableName, Options}).

page(TableName, Options) -> page({global, ?MODULE}, TableName, Options).
page(Pid, TableName, Options) ->
	gen_server:call(Pid, {page, TableName, Options}).

save(TableName, Data) -> save({global, ?MODULE}, TableName, Data).
save(Pid, TableName, Data) ->
	gen_server:call(Pid, {save, TableName, Data}).

merge(TableName, Data) -> merge({global, ?MODULE}, TableName, Data).
merge(Pid, TableName, Data) ->
	gen_server:call(Pid, {merge, TableName, Data}).

update(TableName, Options, Data) -> update({global, ?MODULE}, TableName, Options, Data).
update(Pid, TableName, Options, Data) ->
	gen_server:call(Pid, {update, TableName, Options, Data}).

remove(TableName, Data) -> remove({global, ?MODULE}, TableName, Data).
remove(Pid, TableName, Data) ->
	gen_server:call(Pid, {remove, TableName, Data}).

delete(TableName, Options) -> delete({global, ?MODULE}, TableName, Options).
delete(Pid, TableName, Options) ->
	gen_server:call(Pid, {delete, TableName, Options}).

persist(TableName, Data) -> persist({global, ?MODULE}, TableName, Data).
persist(Pid, TableName, Data) ->
	gen_server:call(Pid, {persist, TableName, Data}).

%% api records

new_sort([]) -> [];
new_sort(List) -> new_sort(List, []).

new_sort([], Result) -> Result;
new_sort([[Field, Order]|T], Result) -> new_sort(T, Result++[#sort{ field = Field, order = Order }]);
new_sort([Field|T], Result) -> new_sort(T, Result++[#sort{ field = Field }]).

new_criteria([]) -> [];
new_criteria(List) -> new_criteria(List, []).

new_criteria([], Result) -> Result;
new_criteria([[Field, in, Value]|T], Result) -> new_criteria(T, Result++[#criteria{ field = Field, test = in, values = Value }]);
new_criteria([[Field, between, Value]|T], Result) -> new_criteria(T, Result++[#criteria{ field = Field, test = between, values = Value }]);
new_criteria([[Field, Test, Value]|T], Result) -> new_criteria(T, Result++[#criteria{ field = Field, test = Test, value = Value }]);
new_criteria([[Field, Value]|T], Result) -> new_criteria(T, Result++[#criteria{ field = Field, value = Value }]);
new_criteria([_, _, _]=H, Result) -> new_criteria([H], Result);
new_criteria([_, _]=H, Result) -> new_criteria([H], Result).

new_options(Val) ->
	Sort = proplists:get_value(sort, Val, []),
	Limit = proplists:get_value(limit, Val, 0),
	Offset = proplists:get_value(offset, Val, 0),
	Criterias = proplists:get_value(where, Val, []),
	Group = proplists:get_value(group, Val, []),
	Projections = proplists:get_value(projections, Val, []),
	Debug = proplists:get_value(debug, Val, false),
	Options = #options{ limit = Limit
										, offset = Offset
										, sort = new_sort(Sort)
										, where = new_criteria(Criterias)
										, projections = Projections
										, group = Group
										, debug = Debug },
	%?debugFmt("Options = ~p", [Options]),
	Options.

%% events

%% first
handle_call({first, Table, Options=#options{}}, _From, State) ->
	{Sql, Args} = lotus_db_util:prepare_sql(select, Table, Options, [], State),
	ResultSet = db:plfr(Sql, Args),
	{reply, {ok, ResultSet}, State};

handle_call({first, Table, Options}, From, State) ->
	handle_call({first, Table, new_options(Options)}, From, State);

%% list
handle_call({list, Table, Options=#options{}}, _From, State) ->
	{Sql, Args} = lotus_db_util:prepare_sql(select, Table, Options, [], State),
	ResultSet = db:plq(Sql, Args),
	{reply, {ok, ResultSet}, State};

handle_call({list, Table, Options}, From, State) ->	
	handle_call({list, Table, new_options(Options)}, From, State);

%% count
handle_call({count, Table, Options=#options{}}, _From, State) ->
	{Sql, Args} = lotus_db_util:prepare_sql(count, Table, Options, [], State),
	ResultSet = db:fffr(Sql, Args),
	{reply, {ok, ResultSet}, State};

handle_call({count, Table, Options}, From, State) ->	
	handle_call({count, Table, new_options(Options)}, From, State);

%% exists
handle_call({exists, Table, Options=#options{}}, _From, State) ->
	{Sql, Args} = lotus_db_util:prepare_sql(count, Table, Options, [], State),
	ResultSet = db:qexists(Sql, Args),
	{reply, {ok, ResultSet}, State};

handle_call({exists, Table, Options}, From, State) ->	
	handle_call({exists, Table, new_options(Options)}, From, State);

%% page
handle_call({page, Table, Options=#options{}}, _From, State) ->
	{SqlSelect, ArgsSelect} = lotus_db_util:prepare_sql(select, Table, Options, [], State),
	ResultSetSelect = db:plq(SqlSelect, ArgsSelect),
	{SqlCount, ArgsCount} = lotus_db_util:prepare_sql(count, Table, Options, [], State),
	ResultSetCount = db:fffr(SqlCount, ArgsCount),
	{reply, {ok, [{data, ResultSetSelect}, {total_count, ResultSetCount}]}, State};

handle_call({page, Table, Options}, From, State) ->	
	handle_call({page, Table, new_options(Options)}, From, State);

%% save
handle_call({save, Table, Data}, _From, State) ->
	{Sql, Args} = lotus_db_util:prepare_sql(save, Table, #options{debug=true} ,Data, State),
	Id = db:qi(Sql, Args),
	NewData = [{id, Id}|proplists:delete(id, Data)],
	{reply, {ok, NewData}, State};

%% update
handle_call({update, Table, Options=#options{}, Data}, _From, State) ->
	{Sql, Args} = lotus_db_util:prepare_sql(update, Table, Options, Data, State),
	ResultSet = db:qu(Sql, Args),
	{reply, {ok, ResultSet}, State};

handle_call({update, Table, Options, Data}, From, State) ->	
	handle_call({update, Table, new_options(Options), Data}, From, State);

%% merge
handle_call({merge, Table, Data}, _From, State) ->
	Result = case proplists:get_value(id, Data, undefined) of
		undefined -> {error, "can't find column id"};
		Id when Id > 0 -> 
			Options = #options{ where = new_criteria([id, Id]), debug=true },
			{Sql, Args} = lotus_db_util:prepare_sql(merge, Table, Options, Data, State),
			ResultSet = db:qu(Sql, Args),
			{ok, ResultSet};
		_ -> {error, "id value can't be zero"}
	end,
	{reply, Result, State};

%% delete
handle_call({delete, Table, Options=#options{}}, _From, State) ->
	{Sql, Args} = lotus_db_util:prepare_sql(delete, Table, Options, [], State),
	ResultSet = db:qu(Sql, Args),
	{reply, {ok, ResultSet}, State};

handle_call({delete, Table, Options}, From, State) ->	
	handle_call({delete, Table, new_options(Options)}, From, State);

%% remove
handle_call({remove, Table, Data}, _From, State) ->
	Result = case proplists:get_value(id, Data, undefined) of
		undefined -> {error, "can't find column id"};
		Id when Id > 0 -> 
			Options = #options{ where = new_criteria([id, Id]), debug=true },
			{Sql, Args} = lotus_db_util:prepare_sql(delete, Table, Options, [], State),
			ResultSet = db:qu(Sql, Args),
			{ok, ResultSet};
		_ -> {error, "id value can't be zero"}
	end,
	{reply, Result, State};

%% persist
handle_call({persist, Table, Data}, _From, State) ->
	Result = case lotus_db_util:prepare_sql(persist, Table, #options{}, Data, State) of
		{error, Reason} -> {error, Reason};
		{merge, {Sql, Args}} ->
			db:qu(Sql, Args),
			{ok, Data};
		{save, {Sql, Args}} ->
			Id = db:qi(Sql, Args),
			NewData = [{id, Id}|proplists:delete(id, Data)], 
			{ok, NewData}
	end,
	{reply, Result, State};

handle_call(Event, _From, State) ->
	{reply, {error, "event not found"}, State}.


%% privates

