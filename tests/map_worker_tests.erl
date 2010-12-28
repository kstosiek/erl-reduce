%% Author: Karol Stosiek (karol.stosiek@gmail.com)
%% Created: 25-12-2010
-module(map_worker_tests).

%%
%% Include files
%%
-include_lib("eunit/include/eunit.hrl").

%%
%% API Functions
%%

map_phase_successful_computation_test() ->
    MapData = [{1,"a"}],
    MapWorkerPid = spawn_link(map_worker, run, [fun(Data) -> Data end]),
    MapWorkerPid ! {self(), {map_data, MapData}},
    receive
        {_, {map_result, ActualResult}} ->
            ExpectedResult = [{1, "a"}],
            ?assertEqual(ExpectedResult, ActualResult)
    end,
    exit(MapWorkerPid, kill).

