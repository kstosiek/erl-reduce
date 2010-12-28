%% Author: Karol Stosiek (karol.stosiek@gmail.com)
%% Created: 25-12-2010
-module(master_tests).

%%
%% Include files
%%
-include_lib("eunit/include/eunit.hrl").

%%
%% API Functions
%%

% TODO: this test should be moved to a separate module.
map_data_partition_divides_into_evenly_sized_chunks_test() ->
    DataToPartition = [{1, 1}, {2, 2}, {3, 3}, {4, 4}],
    Chunks = 3,
    ActualResult = master:partition_map_data(DataToPartition, Chunks),
    
    % Check that no element is missing (count-based) and that chunk sizes
    % differ by at most one element.
    ?assertEqual(length(DataToPartition), length(lists:flatten(ActualResult))),
    lists:foreach(fun(DataChunk) ->
                          ?assert(1 =< length(DataChunk)),
                          ?assert(length(DataChunk) =< 2)
                  end,
                  ActualResult).

% This is a large test.
master_protocol_test() ->
    MapWorkerPids = [spawn(map_worker, run,
                           [fun(MapData) -> MapData end])],
    ReduceWorkerPids = [spawn(reduce_worker, run,
                              [fun(ReduceData) -> ReduceData end])],

    ActualResult = master:run(MapWorkerPids, ReduceWorkerPids, [{1, "a"}]),
    ExpectedResult = [[{1, "a"}]],
    ?assertEqual(ExpectedResult, ActualResult),

    lists:foreach(fun(Pid) -> exit(Pid, kill) end, MapWorkerPids),
    lists:foreach(fun(Pid) -> exit(Pid, kill) end, ReduceWorkerPids).


% This is a large test.
map_phase_successful_computation_test() ->
    MapData = [{1,"a"}],
    MapWorkerPids = [spawn(map_worker, run,
                           [fun(Data) -> Data end])],
    
    ActualResult = master:execute_map_phase(MapData, MapWorkerPids),
    ExpectedResult = [[{1, "a"}]],
    ?assertEqual(ExpectedResult, ActualResult),

    lists:foreach(fun(Pid) -> exit(Pid, kill) end, MapWorkerPids).


% This is a large test.
reduce_phase_successful_computation_test() ->
    ReduceData = [{1,"a"}],
    ReduceWorkerPids = [spawn(reduce_worker, run,
                           [fun(Data) -> Data end])],

    ActualResult = master:execute_reduce_phase(ReduceData, ReduceWorkerPids),
    ExpectedResult = [{1, "a"}],
    ?assertEqual(ExpectedResult, ActualResult),

    lists:foreach(fun(Pid) -> exit(Pid, kill) end, ReduceWorkerPids).





