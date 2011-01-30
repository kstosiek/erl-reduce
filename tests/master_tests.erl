%% Author: Karol Stosiek (karol.stosiek@gmail.com)
%%         Marcin Milewski (mmilewski@gmail.com)
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
    MainWorkerPid = spawn(fun() -> ok end),

    MapWorkerPids = [spawn(map_worker, run,
                           [fun(MapData) -> MapData end])],
    ReduceWorkerPids = [spawn(reduce_worker, run,
                              [fun(ReduceData) -> ReduceData end])],
    InputData = [{1, "a"}],
    Recipe = fun (_) -> lists:nth(1, ReduceWorkerPids) end,
    ActualResult = master:run(MainWorkerPid,
                              MapWorkerPids,
                              ReduceWorkerPids,
                              InputData,
                              Recipe),
    ExpectedResult = [{1, "a"}],
    ?assertEqual(ExpectedResult, ActualResult),

    lists:foreach(fun(Pid) -> exit(Pid, kill) end, MapWorkerPids),
    lists:foreach(fun(Pid) -> exit(Pid, kill) end, ReduceWorkerPids),
    receive
        _ -> ok
    end.


%% tests situation when one map worker crashes.
master_mapper_fault_tolerance_one_mapper_missing_test() ->
    MainWorkerPid = spawn(fun() -> ok end),
    MapWorkerPid1 = spawn(map_worker, run, [fun(MapData) -> MapData end]),
    MapWorkerPid2 = spawn(map_worker, run, [fun(MapData) -> MapData end]),
    MapWorkerPids = [MapWorkerPid1, MapWorkerPid2],
    ReduceWorkerPids = [spawn(reduce_worker, 
                              run,
                              [fun(ReduceData) -> ReduceData end])
                       ],
    InputData = [{1, "a"}, {2, "b"}],
    Recipe = fun (_) -> lists:nth(1, ReduceWorkerPids) end,
    exit(MapWorkerPid1, kill),
    ActualResult = master:run(MainWorkerPid,
                              MapWorkerPids,
                              ReduceWorkerPids,
                              InputData,
                              Recipe),
    ExpectedResult = [{1, "a"}, {2, "b"}],
    ?assertEqual(lists:sort(ExpectedResult), lists:sort(ActualResult)),

    exit(MapWorkerPid2, kill),
    lists:foreach(fun(Pid) -> exit(Pid, kill) end, ReduceWorkerPids),
    receive
        _ -> ok
    end.


%% Tests situation when all map worker crashes.
master_mapper_fault_tolerance_all_mappers_crashed_test() ->
    MainWorkerPid = spawn(fun() -> ok end),
    MapWorkerPid1 = spawn(map_worker, run, [fun(MapData) -> MapData end]),
    MapWorkerPid2 = spawn(map_worker, run, [fun(MapData) -> MapData end]),
    MapWorkerPids = [MapWorkerPid1, MapWorkerPid2],
    ReduceWorkerPids = [spawn(reduce_worker, 
                              run,
                              [fun(ReduceData) -> ReduceData end])
                       ],
    InputData = [{1, "a"}, {2, "b"}],
    Recipe = fun (_) -> lists:nth(1, ReduceWorkerPids) end,
    exit(MapWorkerPid1, kill),
    exit(MapWorkerPid2, kill),
    
    ActualResult = master:run(MainWorkerPid,
                              MapWorkerPids,
                              ReduceWorkerPids,
                              InputData,
                              Recipe),
    ExpectedResult = [],
    ?assertEqual(lists:sort(ExpectedResult), lists:sort(ActualResult)),

    lists:foreach(fun(Pid) -> exit(Pid, kill) end, ReduceWorkerPids).


%% % This is a large test.
%% map_phase_successful_computation_test() ->
%%     MapData = [{1,"a"}],
%%     MapWorkerPids = [spawn(map_worker, run,
%%                            [fun(Data) -> Data end])],
%%
%%     ActualResult = master:execute_map_phase(MapData, MapWorkerPids),
%%     ExpectedResult = [[{1, "a"}]],
%%     ?assertEqual(ExpectedResult, ActualResult),
%%
%%     lists:foreach(fun(Pid) -> exit(Pid, kill) end, MapWorkerPids).
%%
%%
%% % This is a large test.
%% reduce_phase_successful_computation_test() ->
%%     ReduceData = [{1,"a"}],
%%     ReduceWorkerPids = [spawn(reduce_worker, run,
%%                            [fun(Data) -> Data end])],
%%
%%     ActualResult = master:execute_reduce_phase(ReduceData, ReduceWorkerPids),
%%     ExpectedResult = [{1, "a"}],
%%     ?assertEqual(ExpectedResult, ActualResult),
%%
%%     lists:foreach(fun(Pid) -> exit(Pid, kill) end, ReduceWorkerPids).
