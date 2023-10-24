# lotus-db
ER lang SQL maker and executor build on top sql_bridge

## Configure

```erlang
{lotus_db,[
    {configs, [
	  {debug, true},
	  {tables, [
	    {employees, [
	      {columns, [{id, [{key,auto}]}
			, name
			, {created_at, [{auto, created}]}
			, {updated_at, [{auto, updated}]}]}
	    ]},
	    {departments, [
	      {columns, [{id, [{key,auto}]}
			, name
			, {created_at, [{auto, created}]}
			, {updated_at, [{auto, updated}]}]}
	    ]},
	    {dept_emps, [
	      {columns, [{id, [{key,auto}]}
			, department_id
			, employee_id
			, {created_at, [{auto, created}]}
			, {updated_at, [{auto, updated}]}]}
	    ]}
	  ]}       
	]}     
  ]}
```

## Start

```erlang
lotus_db:start_link:([DbConfig])
```

## Usage

```erlang
{ok, #{ id := EmpId }} = lotus_db:save(employees, #{name => "Name"})

{ok, #{ id := DepId }} = lotus_db:save(departments, #{name => "Name"})

{ok, #{ id := Id }} = lotus_db:save(dept_emps, #{department_id => DepId, employee_id => EmpId})

{ok, #{id := Id, name := Name }} = lotus_db:first(employees, #{where => [id, Id]})

{ok, 1} = lotus_db:update(employees, #{name => <<"Tom">>}, #{where => [id, Id]})

{ok, 1} = lotus_db:count(employees)

{ok, 1} = lotus_db:delete(employees, #{where => [id, Id]})

% is null
lotus_db:first(employees, #{where => [id, is_null]})
% is not null
lotus_db:first(employees, #{where => [id, is_not_null]})
% equals
lotus_db:first(employees, #{where => [id, 1]})
% not equals
lotus_db:first(employees, #{where => [id, ne, 1]})

% use eq, ne, gt, ge, lt, le, in, between
```

### JOIN

```erlang
Joins = #{
  alias => emp,
  select => ['emp.id', 'dept_emps.id', 'deps.id'],
  join => [
    #{table => dept_emps,
      on => [id, employee_id],
      join => #{table => departments,
		alias => deps,
		on => [department_id, id]
      }
    }
  ],
  where => [['emp.id', 1], ['dept_emps.id', 1]]	  
},
% select employees join dept_emps join departments
lotus_db:first(employees, Joins)
```

### Native Where

```erlang
NativeSelect = #{
	select => ['emp.id'],
	alias => emp,
  where => #{
    native => "exists (select id from dept_emps where id = ? and employee_id = emp.id)",
    args => [5]
  },
},  	
lotus_db:first(employees, NativeSelect)
```
