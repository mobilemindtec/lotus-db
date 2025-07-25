-module(lotus_db).

-behaviour(gen_server).

-include("../include/lotus_db.hrl").
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
	 save/3,
	 save/4,
	 update/3,
	 update/4,
	 delete/2,
	 delete/3,
	 persist/3,
	 persist/4,
	 merge/2,
	 merge/3,
	 remove/2,
	 remove/3
	]).


start(DbConfigs) ->
    gen_server:start({local, ?MODULE}, ?MODULE, DbConfigs, []).

start_link(DbConfigs) ->
    ok = sql_bridge:start(),
    gen_server:start_link({local, ?MODULE}, ?MODULE, DbConfigs, []).

init(DbConfigs) ->
    lager:info("lotus db init"),
    {ok, #state{ configs = DbConfigs }}.

%% api

call(Args) ->
    call(undefined, Args).
call(Pid, Args) ->
    PidRef = case Pid of
		 undefined ->
		     case whereis(?MODULE) of
			 P when is_pid(P) -> P;
			 _ ->
			     lager:info("[lotus_db] pid not found"),
			     throw("pid not found")
		     end;
		 _ ->
		     Pid
	     end,
    gen_server:call(PidRef, Args).

list(TableName, Options) -> list(undefined, TableName, Options).
list(Pid, TableName, Options) ->
    call(Pid, {list, TableName, Options}).

first(TableName, Options) -> first(undefined, TableName, Options).
first(Pid, TableName, Options) ->
    call(Pid, {first, TableName, Options}).

count(TableName, Options) -> count(undefined, TableName, Options).
count(Pid, TableName, Options) ->
    call(Pid, {count, TableName, Options}).

exists(TableName, Options) -> exists(undefined, TableName, Options).
exists(Pid, TableName, Options) ->
    call(Pid, {exists, TableName, Options}).

page(TableName, Options) -> page(undefined, TableName, Options).
page(Pid, TableName, Options) ->
    call(Pid, {page, TableName, Options}).

save(TableName, Data, Options) -> save(undefined, TableName, Data, Options).
save(Pid, TableName, Data, Options) ->
    call(Pid, {save, TableName,  Data, Options}).

merge(TableName, Data) -> merge(undefined, TableName, Data).
merge(Pid, TableName, Data) ->
    call(Pid, {merge, TableName, Data}).

update(TableName, Data, Options) -> update(undefined, TableName, Data, Options).
update(Pid, TableName, Data, Options) ->
    call(Pid, {update, TableName, Data, Options}).

remove(TableName, Data) -> remove(undefined, TableName, Data).
remove(Pid, TableName, Data) ->
    call(Pid, {remove, TableName, Data}).

delete(TableName, Options) -> delete(undefined, TableName, Options).
delete(Pid, TableName, Options) ->
    call(Pid, {delete, TableName, Options}).

persist(TableName, Data, Options) -> persist(undefined, TableName, Data, Options).
persist(Pid, TableName, Data, Options) ->
    call(Pid, {persist, TableName, Data, Options}).

%% api records

new_order_by([]) -> [];
new_order_by(List) -> new_order_by(List, []).

new_order_by([], Result) -> Result;
new_order_by(OrderBy, Result) when is_atom(OrderBy) -> new_order_by([OrderBy], Result);
new_order_by(OrderBy=[_, _], Result) -> new_order_by([OrderBy], Result);
new_order_by([[Field, Order]|T], Result) -> new_order_by(T, Result++[#order_by{ field = Field, order = Order }]);
new_order_by([Field|T], Result) -> new_order_by(T, Result++[#order_by{ field = Field }]).

new_criteria([]) -> [];
new_criteria(List) when is_list(List) -> new_criteria(List, []);
new_criteria(Map) when is_map(Map) ->
    Props = case maps:get(native, Map, undefined) of
		undefined ->
		    lists:map(fun(Key) ->
				      Val = maps:get(Key, Map),
				      case Val of
					  X when is_list(X) -> [Key|X];
					  _ -> [Key, Val]
				      end
			      end, maps:keys(Map));
		_ ->
		    Map
	    end,
    new_criteria(Props, []).

new_criteria([], Result) -> Result;
new_criteria(M, Result) when is_map(M) -> new_criteria([M], Result);
new_criteria([M|T], Result) when is_map(M) ->
    new_criteria(T, Result++[#criteria{ native = maps:get(native, M), values = maps:get(args, M, []) }]);
new_criteria([[Field, in, Value]|T], Result) ->
    new_criteria(T, Result++[#criteria{ field = Field, test = in, values = Value }]);
new_criteria([[Field, between, Value]|T], Result) ->
    new_criteria(T, Result++[#criteria{ field = Field, test = between, values = Value }]);
new_criteria([[Field, Test, Value]|T], Result) ->
    new_criteria(T, Result++[#criteria{ field = Field, test = Test, value = Value }]);
new_criteria([[Field, Value]|T], Result) ->
    new_criteria(T, Result++[#criteria{ field = Field, value = Value }]);
new_criteria([_, _, _]=H, Result) -> new_criteria([H], Result);
new_criteria([_, _]=H, Result) -> new_criteria([H], Result).

new_join(MainTableAlias, Joins) when is_map(Joins)-> new_join(MainTableAlias, [Joins]);
new_join(MainTableAlias, Joins) -> new_join(MainTableAlias, Joins, []).
new_join(_, [], Results) -> Results;
new_join(MainTableAlias, [Join|T], Results) ->
    JoinTable = maps:get(table, Join),
    Alias = maps:get(alias, Join, undefined),
    ChildJoin = maps:get(join, Join, []),
    Type = maps:get(type, Join, left),
    [Left, Right] = maps:get(on, Join, undefined),
    Where = maps:get(where, Join, []),
    JoinRec = #join{ alias =  Alias
		   , type = Type
		   , table = JoinTable
		   , where = new_criteria(Where)
		   , join = new_join(JoinTable, ChildJoin)
		   , on = #on { left = Left, right = Right } },
    new_join(MainTableAlias, T, [JoinRec]++Results).

new_options(TableName, Val) when is_list(Val) ->
    Sort = proplists:get_value(order_by, Val, []),
    Limit = proplists:get_value(limit, Val, 0),
    Offset = proplists:get_value(offset, Val, 0),
    Criterias = proplists:get_value(where, Val, []),
    Group = proplists:get_value(group_by, Val, []),
    Projections = proplists:get_value(select, Val, []),
    Return = proplists:get_value(return, Val, map),
    Mapper = proplists:get_value(mapper, Val, undefined),
    Debug = proplists:get_value(debug, Val, false),
    Alias = proplists:get_value(alias, Val, TableName),
    Options = #options{ alias = Alias
		      , limit = Limit
		      , offset = Offset
		      , order_by = new_order_by(Sort)
		      , where = new_criteria(Criterias)
		      , select = Projections
		      , group_by = Group
		      , mapper = Mapper
		      , return = Return
		      , debug = Debug },
						%?debugFmt("Options = ~p", [Options]),
    Options;

new_options(TableName, Val) when is_map(Val) ->
    Sort = maps:get(order_by, Val, []),
    Limit = maps:get(limit, Val, 0),
    Offset = maps:get(offset, Val, 0),
    Criterias = maps:get(where, Val, []),
    Group = maps:get(group_by, Val, []),
    Projections = maps:get(select, Val, []),
    Return = maps:get(return, Val, map),
    Mapper = maps:get(mapper, Val, undefined),	
    Debug = maps:get(debug, Val, false),
    Alias = maps:get(alias, Val, undefined),
    Join = maps:get(join, Val, []),
    JoinAlias = case Alias of
		    undefined -> TableName;
		    _ -> Alias
		end,
    Options = #options{ limit = Limit
		      , alias = Alias
		      , join = new_join(JoinAlias, Join)
		      , offset = Offset
		      , order_by = new_order_by(Sort)
		      , where = new_criteria(Criterias)
		      , select = Projections
		      , group_by = Group
		      , mapper = Mapper
		      , return = Return										
		      , debug = Debug },
						%?debugFmt("Options = ~p", [Options]),
    Options.

%% events


%% first
handle_call({first, Table, Options=#options{}}, _From, State) ->
    {Sql, Args} = lotus_db_util:prepare_sql(select, Table, Options, [], State),
    ResultSet = db:plfr(Sql, Args),
    {reply, create_result(Options, ResultSet, {Sql, Args}), State};

handle_call({first, Table, Options}, From, State) ->
    handle_call({first, Table, new_options(Table, Options)}, From, State);

%% list
handle_call({list, Table, Options=#options{}}, _From, State) ->
    {Sql, Args} = lotus_db_util:prepare_sql(select, Table, Options, [], State),
    ResultSet = db:plq(Sql, Args),
    {reply, create_result(Options, ResultSet, {Sql, Args}), State};

handle_call({list, Table, Options}, From, State) ->	
    handle_call({list, Table, new_options(Table, Options)}, From, State);

%% count
handle_call({count, Table, Options=#options{}}, _From, State) ->
    {Sql, Args} = lotus_db_util:prepare_sql(count, Table, Options, [], State),
    ResultSet = db:fffr(Sql, Args),
    {reply, create_result(Options, ResultSet, {Sql, Args}), State};

handle_call({count, Table, Options}, From, State) ->	
    handle_call({count, Table, new_options(Table, Options)}, From, State);

%% exists
handle_call({exists, Table, Options=#options{}}, _From, State) ->
    {Sql, Args} = lotus_db_util:prepare_sql(count, Table, Options, [], State),
    ResultSet = db:qexists(Sql, Args),
    {reply, create_result(Options, ResultSet, {Sql, Args}), State};

handle_call({exists, Table, Options}, From, State) ->	
    handle_call({exists, Table, new_options(Table, Options)}, From, State);

%% page
handle_call({page, Table, Options=#options{}}, _From, State) ->
    {SqlSelect, ArgsSelect} = lotus_db_util:prepare_sql(select, Table, Options, [], State),
    ResultSetSelect = db:plq(SqlSelect, ArgsSelect),
    {SqlCount, ArgsCount} = lotus_db_util:prepare_sql(count, Table, Options, [], State),
    ResultSetCount = db:fffr(SqlCount, ArgsCount),
    Result = case lotus_db_util:is_return(Options, props) of
		 true -> [{data, ResultSetSelect}, {total_count, ResultSetCount}];
		 _ -> #{data => ResultSetSelect, total_count => ResultSetCount}
	     end,
    {reply, create_result(Options, Result, [{SqlSelect, ArgsSelect}, {SqlCount, ArgsCount}]), State};

handle_call({page, Table, Options}, From, State) ->	
    handle_call({page, Table, new_options(Table, Options)}, From, State);

%% save
handle_call({save, Table, Data, Options=#options{}}, _From, State) ->
    {Sql, Args} = lotus_db_util:prepare_sql(save, Table, #options{debug=true} ,Data, State),
    Id = db:qi(Sql, Args),
    NewData = case lotus_db_util:is_return(Options, props) of
		  true -> [{id, Id}|proplists:delete(id, Data)];
		  _ -> maps:merge(Data, #{ id => Id})
	      end,
    {reply, create_result(Options, NewData, {Sql, Args}), State};

handle_call({save, Table, Data, Options}, From, State) ->	
    handle_call({save, Table, Data, new_options(Table, Options)}, From, State);

%% update
handle_call({update, Table, Data, Options=#options{}}, _From, State) ->
    {Sql, Args} = lotus_db_util:prepare_sql(update, Table, Options, Data, State),
    RowsCount = db:qu(Sql, Args),
    {reply, create_result(Options, RowsCount, {Sql, Args}), State};

handle_call({update, Table, Data, Options}, From, State) ->	
    handle_call({update, Table, Data, new_options(Table, Options)}, From, State);

%% merge
handle_call({merge, Table, Data}, _From, State) ->
    Result = case proplists:get_value(id, Data, not_found) of
		 not_found -> {error, "can't find column id"};
		 Id when Id > 0 -> 
		     Options = #options{ where = new_criteria([id, Id]), debug=true },
		     {Sql, Args} = lotus_db_util:prepare_sql(merge, Table, Options, Data, State),
		     RowsCount = db:qu(Sql, Args),
		     create_result(Options, affected_rows(RowsCount, Data), {Sql, Args});
		 _ -> {error, "id value is required"}
	     end,
    {reply, Result, State};

%% delete
handle_call({delete, Table, Options=#options{}}, _From, State) ->
    {Sql, Args} = lotus_db_util:prepare_sql(delete, Table, Options, [], State),
    ResultSet = db:qu(Sql, Args),
    {reply, create_result(Options, ResultSet, {Sql, Args}), State};

handle_call({delete, Table, Options}, From, State) ->	
    handle_call({delete, Table, new_options(Table, Options)}, From, State);

%% remove
handle_call({remove, Table, Data}, _From, State) ->
    Result = case proplists:get_value(id, Data, not_found) of
		 not_found -> {error, "can't find column id"};
		 Id when Id > 0 -> 
		     Options = #options{ where = new_criteria([id, Id]) },
		     {Sql, Args} = lotus_db_util:prepare_sql(delete, Table, Options, [], State),
		     RowsCount = db:qu(Sql, Args),
		     create_result(Options, affected_rows(RowsCount, Data), {Sql, Args});
		 _ -> {error, "id value can't be zero"}
	     end,
    {reply, Result, State};

%% persist
handle_call({persist, Table, Data, Options=#options{}}, _From, State) ->
    Result = case lotus_db_util:prepare_sql(persist, Table, #options{}, Data, State) of
		 {error, Reason} -> {error, Reason};
		 {merge, {Sql, Args}} ->
		     RowsCount = db:qu(Sql, Args),
		     create_result(Options, affected_rows(RowsCount, Data), {Sql, Args});
		 {save, {Sql, Args}} ->
		     Id = db:qi(Sql, Args),
		     ResultSet = case lotus_db_util:is_return(Options, props) of
				     true -> [{id, Id}|proplists:delete(id, Data)];
				     _ -> maps:merge(Data, #{ id => Id})
				 end,
		     create_result(Options, ResultSet, {Sql, Args})
	     end,
    {reply, Result, State};

handle_call({persist, Table, Data, Options}, From, State) ->	
    handle_call({persist, Table, Data, new_options(Table, Options)}, From, State);

handle_call(_Event, _From, State) ->
    {reply, {error, "event not found"}, State}.


%% privates

affected_rows(1, Data)-> Data;
affected_rows(0, _)-> not_found;
affected_rows(RowsCount, _)-> {error, integer_to_list(RowsCount)++"rows affected"}.

create_result(Options, ResultSet, Return) ->
    case lotus_db_util:is_return(Options, sql) of
	true -> to_result(Options, ResultSet, Return);
	_ -> to_result(Options, ResultSet) 
    end.

to_result(_, {error, Reason}, _) -> {error, Reason};
to_result(_, not_found, _) -> not_found;
to_result(_, unknown_error, _) -> {error, unknown_error};
to_result(Options, Result, Return) -> {ok, convert_result(Options, Result), Return}.

to_result(_, {error, Reason}) -> {error, Reason};
to_result(_, not_found) -> not_found;
to_result(_, unknown_error) -> {error, unknown_error};
to_result(Options, Result) -> {ok, convert_result(Options, Result)}.

convert_result(Options, Result) ->
    case lotus_db_util:is_return(Options, props) of
	true -> Result;
	_ -> result_to_map(Result)
    end.

result_to_map(Data) when is_map(Data) -> Data;
result_to_map([{_,_}|_]=Data) -> props_to_map(Data);
result_to_map([[{_,_}|_]|_]=Data) -> [props_to_map(X) || X <- Data];
result_to_map(Data) -> Data.

props_to_map(Data) -> props_to_map(Data, #{}).
props_to_map([], Map) -> Map;
props_to_map([{K,V}|T], Map) -> props_to_map(T, maps:merge(#{ K => V}, Map)).
