

-record(state, {configs 												:: tuple()}).

-record(criteria, { field 											:: atom()
									, test        = eq						:: atom()
									, value 											:: any()
									, values 											:: list(any())}).

-type criteria() 																:: #criteria{}.

-record(sort, { field                           :: atom()
							, order           = asc 					:: atom()}).

-type sort()																		:: #sort{}.

-record(options, { limit 				= 0 						:: integer()
								 , offset 			= 0 						:: integer()
								 , where        = []				    :: list(criteria())
								 , sort         = [] 						:: list(sort())
								 , projections  = []            :: list(atom())
								 , group        = []            :: list(atom())
								 , debug        = false         :: boolean()}).

-type options() 																:: #options{}.

-type tuple_opts()  														:: tuple().