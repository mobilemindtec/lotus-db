

-record(state, {configs 												:: tuple()}).

-record(criteria, { field 											:: atom()
									, test        = eq						:: eq | ne | lt | le | gt | ge | between | in | like | ilike
									, match                       :: contains | starts_with | ends_with | anyware
									, value 											:: any()
									, values 											:: list(any())}).

-type criteria() 																:: #criteria{}.

-record(order_by, { field                       :: atom()
							    , order           = asc 			:: atom()}).

-type order_by()																:: #order_by{}.

-record(on, { l                            :: atom(),
									 r                            :: atom(),
									 test        = eq             :: atom()}).

-type on() 																::#on{}.

-record(join, { table                           :: atom()
		          , alias                           :: atom()
		          , on                              :: list(on())}). %['tb1.id' eq 'tb2.id']

-type join() 																		:: #join{}.

-record(options, { table                        :: atom()
								 , alias        = undefined     :: atom()  
								 , limit 				= 0 						:: integer()
								 , offset 			= 0 						:: integer()
								 , where        = []				    :: list(criteria())
								 , order_by     = [] 						:: list(order_by())
								 , select       = []            :: list(atom())
								 , group_by     = []            :: list(atom())
								 , debug        = false         :: boolean()
								 , return       = map           :: tuple() % props | map | sql
								 , mapper       = undefined     :: atom() }).

-type options() 																:: #options{}.

-type tuple_opts()  														:: tuple().