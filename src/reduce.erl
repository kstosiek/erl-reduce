%% Author: Karol Stosiek (karol.stosiek@gmail.com)
%% Created: 28-11-2010
%% Description: Module with stub reduce/1 function.
-module(reduce).

%%
%% Exported Functions
%%
-export([reduce/1]).

%% @doc Reduces intermediate data to final data. In current implementation
%%     it is an identity function.
%% @spec ([{K2,V2}]) -> [{K3, V3}]
%% @throws not_implemented
reduce (IntermediateData) -> 
    sum_of_values(IntermediateData).
    %% throw(not_implemented).

%% Takes list [{k1, v1}, {k2, v2}, ...]
%% Returns sum of [v1, v2, ...]
sum_of_values(L) -> lists:sum(lists:map(fun ({_,B}) -> B end, L)).
