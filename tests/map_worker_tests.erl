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

map_worker_successful_computation_test() ->
    MapData = [{1,"a"}],
    MasterPid = self(),
    Recipe = fun (_) -> MasterPid end, 
    MapWorkerPid = spawn_link(map_worker, run, [fun(Data) -> Data end]),
    MapWorkerPid ! {self(), {map_data, MapData}},
    receive
        {_, map_finished} ->
            MapWorkerPid ! {self(), {recipe, Recipe}}
    end,
    receive
        {_, {reduce_data, _ }} ->
            MapWorkerPid ! {self(), reduce_data_acknowledged}
    end,
    receive
        {_, map_send_finished} ->
            success
    end,
    exit(MapWorkerPid, kill).

