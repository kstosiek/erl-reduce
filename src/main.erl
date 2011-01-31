%% Author: Karol Stosiek (karol.stosiek@gmail.com)
%% Created: 07-11-2010
%% Description: MapReduce program entry.
-module(main).

%%
%% Exported Functions
%%
-export([start/0]).


%% @doc Runs MapReduce on given input. Sets up master and worker units
%%     and performs the computation.
%% @spec () -> FinalData where
%%    MapData = [{K1,V1}],
%%    FinalData = [{K3,V3}]
start() ->
    MapWorkerPids = spawn_map_workers(conf:map_function(),
                                      conf:map_worker_nodes()),
    ReduceWorkerPids = spawn_reduce_workers(conf:reduce_function(),
                                            conf:reduce_worker_nodes()),
    Recipe = conf:recipe(ReduceWorkerPids),
    InputData = conf:input_data(),

    error_logger:info_msg("Starting map/reduce calculation with~n"
                              "map workers: ~p~n"
                                  "reduce workers: ~p~n",
                          [MapWorkerPids, ReduceWorkerPids]),
    MasterPid = spawn(master, run, [self(),
                                    MapWorkerPids,
                                    ReduceWorkerPids,
                                    InputData,
                                    Recipe]),

    error_logger:info_msg("Waiting for map/reduce result."),
    receive
        {MasterPid, {map_reduce_result, MapReduceResult}} ->
            MapReduceResult
    end,

    error_logger:info_msg("Map/reduce finished."),
    MapReduceResult.


%% @doc Spawns map workers with given map function (MapFunction) on given
%%     nodes (MapWorkerNames). Returns list of pids of the spawned processes.
%% @spec (MapFunction,MapWorkerNames) -> SpawnedProcesses where
%%     MapFunction = (MapData) -> IntermediateData
%%     MapData = [{K1,V1}]
%%     IntermediateData = [{K2,V2}]
%%     MapWorkerNames = [string()]
%%     SpawnedProcesses = [pid()]
spawn_map_workers(MapFunction, MapWorkerNames) ->
    lists:map(fun (MapWorkerName) ->
                       spawn(MapWorkerName, map_worker,
                             run, [MapFunction])
              end,
              MapWorkerNames).


%% @doc Spawns reduce workers with given map function (ReduceFunction) on given
%%     nodes (ReduceWorkerNames). Returns list of pids of the spawned processes.
%% @spec (ReduceFunction,ReduceWorkerNames) -> SpawnedProcesses where
%%     ReduceFunction = (IntermediateData) -> FinalData
%%     IntermediateData = [{K2,V2}]
%%     FinalData = [{K3,V3}]
%%     ReduceWorkerNames = [string()]
%%     SpawnedProcesses = [pid()]
spawn_reduce_workers(ReduceFunction, ReduceWorkerNames) ->
    lists:map(fun (ReduceWorkerName) ->
                       spawn(ReduceWorkerName, reduce_worker,
                             run, [ReduceFunction])
              end,
              ReduceWorkerNames).

