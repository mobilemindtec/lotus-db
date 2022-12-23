-module(lotus_db_util).

-include("../include/lotus_db.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
	prepare_sql/5,
	is_return/2,
  conv_col_name/1
]).

prepare_sql(Type, Table, Options, Data, State) ->
	Configs = State#state.configs,
	TableName = get_table_name(Table, Configs),
	Columns = get_table_columns(Table, Configs),
	TableNameAlias = case Options of
		#options{ alias = undefined } -> TableName;
		#options{ alias = Alias } -> string:join([TableName, atom_to_list(Alias)], " ")
	end,
	{Sql, Args} = create_sql(Type, TableNameAlias, Columns, Options, Data),
	Opts = case get_is_debug(Configs) of 
		true -> Options#options { debug = true };
		_ -> Options
	end,
		
	sql_debug(Opts, string:trim(Sql), Args).

create_sql(select, TableName, Columns, Options=#options{}, _) ->
	#options{ where 		= Criterias
					, order_by 	= Sort
					, select 		= Projections
					, group_by 	= Group
          , join = Join} = Options,
  MainJoin = if
               length(Join) == 0 -> #join{};
               true ->
                 options_to_join(Options#options{table = list_to_atom(TableName)})
             end,
	ColumnsStr = case Projections of
		[] -> columns_to_select_str(MainJoin, Columns);
		_ -> string:join(projections_to_str(Projections), ", ")
	end,
	Select = string:join(["select", ColumnsStr, "from",  TableName], " "),
  Joins = join_compile(MainJoin, Join),
	{Conditions, ConditionsArgs} = compile_sql_criteria(Criterias),
	{LimitSql, LimitArgs} = add_limit(Options),
	{OffsetSql, OffsetArgs} = add_offset(Options),
	GroupStr = add_group_by(Group),
	SortStr = add_sort(Sort),

  SelectList = [Select, Joins, Conditions, SortStr, GroupStr, LimitSql, OffsetSql],
  SelectConstruct = lists:filter(fun(X) -> length(X) > 0 end, SelectList),
	{string:join(SelectConstruct, " "), ConditionsArgs++LimitArgs++OffsetArgs};

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

projections_to_str(Projections) when is_map(Projections) ->
  NewProjections = lists:map(fun(Key) ->
                              [conv_col_name(Key) ++ " "
                                ++ conv_col_name(maps:get(Key, Projections))]
                             end, maps:keys(Projections)),
  projections_to_str(NewProjections);
projections_to_str(Projections) -> projections_to_str(Projections, []).
projections_to_str([], Result) -> Result;
projections_to_str([H|T], Result) when is_atom(H) -> projections_to_str(T, Result++[conv_col_name(H)]);
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

compile_criteria_condition(#criteria{ field = Field, test = eq, value = null}) ->
  [atom_to_list(Field), "is null"];
compile_criteria_condition(#criteria{ field = Field, test = ne, value = null}) ->
  [atom_to_list(Field), "is not null"];
compile_criteria_condition(#criteria{ field = Field, test = Cond, value = Value, values = Values, native = Native }) ->
  case Native of
    undefined ->
      [	atom_to_list(Field)
        , criteria_cond(Cond)
        , compile_criteria_replace_args(Cond, Value, Values)];
    _ -> [Native]
  end.

criteria_compile([]) -> [];
criteria_compile(Criterias) -> criteria_compile(Criterias, []).
criteria_compile([], Compiled) -> Compiled;
criteria_compile([H|T], Compiled) ->
	criteria_compile(T, Compiled++[string:join(compile_criteria_condition(H), " ")]).

options_to_join(#options{ table = Table, alias = Alias}) -> #join{table = Table, alias = Alias}.

join_compile(MainJoin, Joins) -> join_compile(MainJoin, Joins, []).
join_compile(_, [], Results) -> string:join(Results, " ");
join_compile(MainJoin, [J|T], Results) ->
  On = J#join.on,
  JoinAliasTable = get_join_alias(J),
  JoinAliasCol = get_join_alias_col_join(J),
  TableAliasCol = get_join_alias_col_join(MainJoin),
  JoinList = [get_join_type(J#join.type)
            , "join"
            , JoinAliasTable
            , "on"
            , TableAliasCol++atom_to_list(On#on.left)
            , "="
            , JoinAliasCol++atom_to_list(On#on.right)],
  JoinStr = string:join(JoinList, " "),
  ChildJoins = case J#join.join of
                  [] -> [JoinStr];
                  ChildJoin -> [JoinStr|[join_compile(J, ChildJoin)]]
               end,
  join_compile(MainJoin, T, ChildJoins++Results).


get_join_alias_col_join(#join{alias = Alias, table = JoinTable}) ->
  case Alias of
    undefined -> case JoinTable of
                   undefined -> "";
                   _ -> atom_to_list(JoinTable) ++ "."
                 end;
    _ -> atom_to_list(Alias) ++ "."
  end.

get_join_alias_col_sel(#join{alias = Alias, table = JoinTable}, Col) ->
  case Alias of
    undefined -> case JoinTable of
                   undefined -> Col;
                   _ ->
                     Al = atom_to_list(JoinTable),
                     Al ++ "." ++ Col ++ " " ++ Al++"_"++Col
                 end;
    _ ->
      Al = atom_to_list(Alias),
      Al ++ "." ++ Col ++ " " ++ Al++"_"++Col
  end.

get_join_alias(#join{alias = Alias, table = JoinTable}) ->
  case Alias of
    undefined -> atom_to_list(JoinTable);
    _ -> atom_to_list(JoinTable) ++ " " ++ atom_to_list(Alias)
  end.

get_join_type(left) -> "left";
get_join_type(right) -> "right";
get_join_type(full) -> "full";
get_join_type(outer) -> "outer";
get_join_type(inner) -> "inner".

compile_criteria_args(#criteria{ value = null }) -> [];
compile_criteria_args(#criteria{ test = in, values = Values}) -> Values;
compile_criteria_args(#criteria{ test = between, values = Values}) -> Values;
compile_criteria_args(#criteria{value = Value, values = Values, native = Native}) ->
  case Native of
    undefined -> [Value];
    _ -> Values
  end.

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
add_sort([#order_by{ field = Field, order = Order }|T], Result) ->
	add_sort(T, Result ++ [[atom_to_list(Field), atom_to_list(Order)]]).

add_group_by([]) -> [];
add_group_by(List) -> add_group_by(List, []).

add_group_by([], []) -> [];
add_group_by([], Result) -> string:join(["group by" | Result], " ");
add_group_by([Field|T], Result) ->
	add_group_by(T, Result ++ [atom_to_list(Field)]).

columns_to_select_str(Join, Columns) -> columns_to_select_str(Join, Columns, []).
columns_to_select_str(_, [], Result) -> string:join(Result, ", ");
columns_to_select_str(Join, [{FieldName, _}|T], Result) ->
  columns_to_select_str(Join, [FieldName|T], Result);
columns_to_select_str(Join, [FieldName|T], Result) ->
  columns_to_select_str(Join, T, Result++[get_join_alias_col_sel(Join, conv_col_name(FieldName))]).

conv_col_name(C) when is_atom(C) -> atom_to_list(C);
conv_col_name(C) -> C.

get_table_columns(TableName, Configs) ->
	Tables = proplists:get_value(tables, Configs, []),
	Table = proplists:get_value(TableName, Tables, []),
	proplists:get_value(columns, Table, []).

get_table_name(TableName, Configs) ->
	Tables = proplists:get_value(tables, Configs),
	Table = proplists:get_value(TableName, Tables),
	Name = proplists:get_value(name, Table, TableName),
	get_table_name(Name).

get_table_name(Name) when is_atom(Name) -> atom_to_list(Name);
get_table_name(Name) -> Name.

get_default_return_type(Configs) ->
	proplists:get_value(return, Configs, undefined).

get_is_debug(Configs) ->
	Tables = proplists:get_value(debug, Configs, false).

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

	Def = get_data_val(FieldName, Data, skip),
	Id  = get_data_val(id, Data, 0),
	Auto = proplists:get_value(auto, Opts, false),
	Key = proplists:get_value(key, Opts, false),
	ForceAuto = Type =:= save orelse Type =:= merge,
	%?debugFmt("FieldName = ~p, Auto = ~p, Id = ~p, Def = ~p", [FieldName, Auto, Id, Def]),
	EmptyId = Id =:= 0 orelse Id =:= undefined, 
	if
		Def =:= skip andalso not ForceAuto -> skip;
		Auto =:= uuid -> lotus_db_uuid:v4();
		Auto =:= updated -> calendar:local_time();
		Auto =:= created andalso EmptyId -> calendar:local_time();  
		Key =:= auto -> skip;
    Def =:= null -> skip;
		true ->  Def
	end.

get_data_val(FieldName, Data, Default) when is_list(Data) -> proplists:get_value(FieldName, Data, Default);
get_data_val(FieldName, Data, Default) when is_map(Data) -> maps:get(FieldName, Data, Default). 

%% debug

sql_debug_arg_str(Arg) when is_list(Arg) -> Arg;
sql_debug_arg_str(Arg)  -> lists:flatten(io_lib:format("~p", [Arg])).
sql_debug(#options{ debug = true }, Sql, Args) ->
	ArgsStrList = lists:map(fun sql_debug_arg_str/1, Args),
	?debugFmt("SQL: ~p, ARGS: ~p", [Sql, string:join(ArgsStrList, ", ")]), 	
	{Sql, Args};
sql_debug(_, Sql, Args) -> {Sql, Args}.

is_return(#options{ return = Return }, Val) -> is_return(Return, Val);
is_return(R, Val) when not is_tuple(R) -> is_return({R}, Val);
is_return(_, []) -> true; 
is_return(Return, [H|T]) ->
	case lists:member(H, tuple_to_list(Return)) of
		true -> is_return(T, Return);
		_ -> false
	end;
is_return(Return, Val) -> lists:member(Val, tuple_to_list(Return)). 