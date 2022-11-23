-module(lotus_db_uuid).
-export([v4/0]).
-define(VARIANT10, 2#10).
-define(UUIDv4, 4).


v4() ->
    <<U0:32, U1:16, _:4, U2:12, _:2, U3:30, U4:32>> = crypto:strong_rand_bytes(16),
    lists:flatten(io_lib:format("~8.16.0b-~4.16.0b-~4.16.0b-~2.16.0b~2.16.0b-~12.16.0b",get_binary_uuid(<<U0:32, U1:16, ?UUIDv4:4, U2:12, ?VARIANT10:2, U3:30, U4:32>>))).

get_binary_uuid(<<TL:32, TM:16, THV:16, CSR:8, CSL:8, N:48>>) ->
    [TL, TM, THV, CSR, CSL, N].