-module(lotus_db_util).

-include("include/lotus_db.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
	prepare_sql/5
]).

prepare_sql(Type, Table, Options, Data, State) ->
	Configs = State#state.configs,
	TableName = get_table_name(Table, Configs),
	Columns = get_table_columns(Table, Configs),
	{Sql, Args} = create_sql(Type, TableName, Columns, Options, Data),
	sql_debug(Options, Sql, Args).

create_sql(select, TableName, Columns, Options=#options{}, _) ->
	#options{ where = Criterias, sort = Sort, projections = Projections, group = Group} = Options,
	ColumnsStr = case Projections of
		[] -> columns_to_select_str(Columns);
		_ -> string:join(projections_to_str(Projections), ", ")
	end,
	Select = string:join(["select", ColumnsStr, "from", TableName], " "),
	{Conditions, ConditionsArgs} = compile_sql_criteria(Criterias),
	{LimitSql, LimitArgs} = add_limit(Options),
	{OffsetSql, OffsetArgs} = add_offset(Options),
	GroupStr = add_group_by(Group),
	SortStr = add_sort(Sort),
	{string:join([Select, Conditions, SortStr, GroupStr, LimitSql, OffsetSql], " "), ConditionsArgs++LimitArgs++OffsetArgs};

create_sql(count, TableName, _, #options{ where = Criterias } = Options, _) ->
	Select = string:join(["select count(*) from", TableName], " "),
	{Conditions, ConditionsArgs} = compile_sql_criteria(Criterias),
	{string:join([Select, Conditions], " "), ConditionsArgs};

create_sql(save, TableName, Columns, _, Data) ->
	{Count, ColumnsStr} = columns_to_insert_str(Columns, Data),
	ColumnsRepaces = string:join(lists:map(fun(_) -> "?" end, lists:seq(1, Count)), ", "),
	Select = string:join(["insert into", TableName, "(", ColumnsStr, ")", "values", "(", ColumnsRepaces, ")"], " "),
	Args = create_val_args(save, Columns, Data),
	{Select, Args};

create_sql(Type, TableName, Columns, #options{ where = Criterias}, Data) when Type =:= update orelse Type =:= merge ->
	{_, ColumnsStr} = columns_to_update_str(Type, Columns, Data),
	{Conditions, ConditionsArgs} = compile_sql_criteria(Criterias),
	Select = string:join(["update", TableName, "set", ColumnsStr, Conditions], " "),
	Args = create_val_args(Type, Columns, Data),
	{Select, Args++ConditionsArgs};

create_sql(delete, TableName, Columns, #options{ where = Criterias}, Data) ->
	{Conditions, ConditionsArgs} = compile_sql_criteria(Criterias),
	Select = string:join(["delete from", TableName, Conditions], " "),
	{Select, ConditionsArgs};

create_sql(persist, TableName, Columns, Options, Data) ->	
	case proplists:get_value(id, Data, 0) of
		Id when Id > 0 -> {merge, create_sql(merge, TableName, Columns, Options, Data)};
		_  -> {save, create_sql(save, TableName, Columns, Options, Data)}
	end.

projections_to_str(Projections) -> projections_to_str(Projections, []).
projections_to_str([], Result) -> Result;
projections_to_str([H|T], Result) when is_atom(H) -> projections_to_str(T, Result++[atom_to_list(H)]);
projections_to_str([H|T], Result) -> projections_to_str(T, Result++[H]).

criteria_cond(eq) -> "=";
criteria_cond(ne) -> "<>";
criteria_cond(gt) -> ">";
criteria_cond(ge) -> ">=";
criteria_cond(lt) -> "<";
criteria_cond(le) -> "<=";
criteria_cond(in) -> "in";
criteria_cond(between) -> "between".

compile_criteria_replace_args(in, _, Values) -> "(" ++ string:join(lists:map(fun(_) -> "?" end, Values), ", ") ++ ")";
compile_criteria_replace_args(between, _, Values) -> "? and ?";
compile_criteria_replace_args(_, _, _) -> "?".

compile_criteria_condition(#criteria{ field = Field, test = Cond, value = Value, values = Values }) ->
	[	atom_to_list(Field)
	, criteria_cond(Cond)
	, compile_criteria_replace_args(Cond, Value, Values)].


criteria_compile([]) -> [];
criteria_compile(Criterias) -> criteria_compile(Criterias, []).
criteria_compile([], Compiled) -> Compiled;
criteria_compile([H|T], Compiled) ->
	criteria_compile(T, Compiled++[string:join(compile_criteria_condition(H), " ")]).


compile_criteria_args(#criteria{ test = in, values = Values}) -> Values;
compile_criteria_args(#criteria{ test = between, values = Values}) -> Values;
compile_criteria_args(#criteria{value = Value}) -> [Value].


criteria_compile_args(Criterias) -> criteria_compile_args(Criterias, []).
criteria_compile_args([], Args) -> Args;
criteria_compile_args([H|T], Args) ->
	criteria_compile_args(T, Args ++ compile_criteria_args(H)).

criteria_where([]) -> [];
criteria_where([H|_]) -> ["where", H].

criteria_and([]) -> [];
criteria_and([_]) -> [];
criteria_and([_|T]) -> ["and" | lists:join("and", T)].

compile_sql_criteria(Criterias) ->
	Compiled = criteria_compile(Criterias),
	Where = string:join(criteria_where(Compiled), " "),
	And = string:join(criteria_and(Compiled), " "),
	%?debugFmt("Compiled = ~p, Where = ~p, And = ~p", [Compiled, Where, And]),
	{string:join([Where, And], " "), criteria_compile_args(Criterias)}.

add_limit(#options{ limit = 0 }) -> {"", []};
add_limit(#options{ limit = Limit }) -> {"limit ?", [Limit]}.

add_offset(#options{ offset = 0 }) -> {"", []};
add_offset(#options{ offset = Offset }) -> {"offset ?", [Offset]}.

add_sort([]) -> [];
add_sort(List) -> add_sort(List, []).

add_sort([], []) -> [];
add_sort([], [H|[]]) -> string:join(["order by"| H], " ");
add_sort([], [H|T]) -> string:join([["order by"| H] | list:join(", ", T)], " ");
add_sort([#sort{ field = Field, order = Order }|T], Result) ->
	add_sort(T, Result ++ [[atom_to_list(Field), atom_to_list(Order)]]).

add_group_by([]) -> [];
add_group_by(List) -> add_group_by(List, []).

add_group_by([], []) -> [];
add_group_by([], Result) -> string:join(["group by" | Result], " ");
add_group_by([Field|T], Result) ->
	add_group_by(T, Result ++ [atom_to_list(Field)]).

columns_to_select_str(Columns) -> columns_to_select_str(Columns, []).
columns_to_select_str([], Result) -> string:join(Result, ", ");
columns_to_select_str([{FieldName, _}|T], Result) -> columns_to_select_str([FieldName|T], Result);
columns_to_select_str([FieldName|T], Result) -> columns_to_select_str(T, Result++[atom_to_list(FieldName)]).

get_table_columns(TableName, Configs) ->
	Tables = proplists:get_value(tables, Configs),
	Table = proplists:get_value(TableName, Tables),
	proplists:get_value(columns, Table).

get_table_name(TableName, Configs) ->
	Tables = proplists:get_value(tables, Configs),
	Table = proplists:get_value(TableName, Tables),
	Name = proplists:get_value(name, Table, TableName),
	get_table_name(Name).

get_table_name(Name) when is_atom(Name) -> atom_to_list(Name);
get_table_name(Name) -> Name.


create_val_args(Type, Columns, Data) -> create_val_args(Type, Columns, Data, []).
create_val_args(Type, [], _, Args) -> Args;
create_val_args(Type, [{FieldName, Opts}|T], Data, Args) -> 
	case get_column_data_val(Type, FieldName, Opts, Data) of
		skip -> create_val_args(Type, T, Data, Args);
		Val -> create_val_args(Type, T, Data, Args++[Val])
	end;
create_val_args(Type, [FieldName|T], Data, Args) ->
	create_val_args(Type, [{FieldName, []}|T], Data, Args).

columns_to_insert_str(Columns, Data) -> columns_to_insert_str(Columns, Data, []).
columns_to_insert_str([], _, Result) -> {length(Result), string:join(Result, ", ")};
columns_to_insert_str([{FieldName, Opts}|T]=Columns, Data, Result) -> 
	case proplists:get_value(key, Opts, undefined) of
		auto -> columns_to_insert_str(T, Data, Result);
		undefined -> columns_to_insert_str([FieldName|T], Data, Result)
	end;
columns_to_insert_str([{FieldName, _}|T], Data, Result) -> columns_to_insert_str([FieldName|T], Data, Result);
columns_to_insert_str([FieldName|T], Data, Result) -> columns_to_insert_str(T, Data, Result++[atom_to_list(FieldName)]).

columns_to_update_str(Type, Columns, Data) -> columns_to_update_str(Type, Columns, Data, []).
columns_to_update_str(Type, [], _, Result) -> {length(Result), string:join(Result, ", ")};
columns_to_update_str(Type, [{FieldName, Opts}|T], Data, Result) -> 
	case get_column_data_val(Type, FieldName, Opts, Data) of
		skip -> columns_to_update_str(Type, T, Data, Result);
		_ -> columns_to_update_str(Type, T, Data, Result++[atom_to_list(FieldName)++" = ?"])
	end;
columns_to_update_str(Type, [FieldName|T], Data, Result) ->
	columns_to_update_str(Type, [{FieldName, []}|T], Data, Result).

get_column_data_val(Type, FieldName, Opts, Data) ->
	Def = proplists:get_value(FieldName, Data, skip),
	Id  = proplists:get_value(id, Data, 0),
	Auto = proplists:get_value(auto, Opts, false),
	Key = proplists:get_value(key, Opts, false),
	ForceAuto = Type =:= save orelse Type =:= merge,
	if
		Def =:= skip andalso not ForceAuto -> skip;
		Auto =:= uuid -> lotus_db_uuid:v4();
		Auto =:= updated -> calendar:local_time();
		Auto =:= created andalso Id =:= 0 -> calendar:local_time(); 
		Key =:= auto -> skip;
		true ->  Def
	end.

%% debug

sql_debug_arg_str(Arg) when is_list(Arg) -> Arg;
sql_debug_arg_str(Arg)  -> lists:flatten(io_lib:format("~p", [Arg])).
sql_debug(#options{ debug = true }, Sql, Args) ->
	ArgsStrList = lists:map(fun sql_debug_arg_str/1, Args),
	?debugFmt("SQL: ~p, ARGS: ~p", [Sql, string:join(ArgsStrList, ", ")]), 	
	{Sql, Args};
sql_debug(_, Sql, Args) -> {Sql, Args}.
