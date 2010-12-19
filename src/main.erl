%% Author: Karol Stosiek (karol.stosiek@gmail.com)
%% Created: 07-11-2010
%% Description: MapReduce program entry.
-module(main).

%%
%% Exported Functions
%%
-export([eval/1]).

%% @doc Runs MapReduce on given input.
%% @spec ([{K1,V1}]) -> [{K3,V3}].
eval(Input) ->
    Result = reduce:reduce(map:map(Input)),
    io:format("Final result: ~w~n", [Result]),
    ok.
