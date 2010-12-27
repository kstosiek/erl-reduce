%% Author: Marcin Milewski (mmilewski@gmail.com)
%% Created: 21-11-2010
%% Description: Coordinates the map/reduce computation: feeds workers with data,
%%     takes care of dying workers and returns collected data to the user.
-module(master).

%%
%% Exported Functions.
%%
-export([run/3]).


%%
%% API Functions.
%%

%% @doc Main master function. Executes map/reduce operation on given input
%%     with given map and reduce workers. Returns overall result.
%% @spec (MapWorkerPids, ReduceWorkerPids, InputData) -> FinalResult where
%%    MapWorderPids = [pid()],
%%    ReduceWorkerPids = [pid()],
%%    InputData = [{K1,V1}],
%%    FinalResult = [{K3,V3}]
run(MapWorkerPids, ReduceWorkerPids, InputData) ->   
    MapResult = execute_map_phase(InputData, MapWorkerPids),
    ReduceResult = execute_reduce_phase(MapResult, ReduceWorkerPids),
    ReduceResult.

%%
%% Local Functions.
%%

%% @doc Partitions given Data into chunks of at most SliceSize and appends
%%     resulting chunk to the Accumulator. If there is at least ChunkSize
%%     elements in the given list, then chunk size is ChunkSize; otherwise
%%     there is at most as many elements in the chunk, as in the given list.
%%     TODO: make the difference in chunk sizes be at most 1 element.
%% @spec (Data,SliceSize,Accumulator) -> PartitionedMapData where
%%     Data = [{K1,V1}],
%%     SliceSize = int(),
%%     Accumulator = PartitionedMapData = [Data]
partition_data_with_accumulator([], _, Accumulator) ->
    Accumulator;

partition_data_with_accumulator(Data, ChunkSize, Accumulator) ->
    {Chunk, OtherData} = lists:split(erlang:min(ChunkSize, length(Data)), Data),
    partition_data_with_accumulator(OtherData, ChunkSize, [Chunk|Accumulator]).


%% @doc Parititions data into almost evenly-sized data chunks. Last chunk may
%%     not be as big as other chunks.
%%     TODO: make the difference in chunk sizes be at most 1 element.
%% @spec (Data,Slices) -> [Data] where
%%     Data = [{K1,V1}],
%%     Slices = int()
partition_map_data(Data, Slices) ->
    partition_data_with_accumulator(Data, round(length(Data) / Slices), []).

%% @doc Collects results from given map workers (MapWorkerPids)
%%     into CollectedResults (accumulator); RemainingWorkers contains
%%     information on how many workers left need to return results.
%%     TODO: use pid set instead of count (RemainingWorkers).
%% @spec (MapWorkerPids, CollectedResults, RemainingWorkers) ->
%%     IntermediateData where
%%     MapWorkerPids = [pid()],
%%     CollectedResults = [{K2,V2}],
%%     RemainingWorkers = int()
collect_map_results_loop(_, CollectedResults, 0) ->
    CollectedResults;

collect_map_results_loop(MapWorkerPids,
                         CollectedResults,
                         RemainingWorkers) ->
    receive 
        {MapWorkerPid, {map_result, Result}} ->
            collect_map_results_loop(MapWorkerPids, [Result|CollectedResults],
                                     RemainingWorkers - 1)
    end.


%% @doc Collects mapped data from map workers until all workers return their
%%    results.
%% @spec (MapWorkerPids) -> IntermediateData where
%%     MapWorkerPids = [pid()],
%%     IntermediateData = [{K2,V2}]
collect_map_results(MapWorkerPids) ->
    collect_map_results_loop(MapWorkerPids, [], length(MapWorkerPids)).


%% @doc Sends partitioned data to map workers and collects results.
%% @spec (MapData, MapWorkerPids) -> IntermediateData where
%%     MapData = [{K1,V1}],
%%     MapWorkerPids = [pid()],
%%     IntermediateData = [{K2,V2}]
execute_map_phase(MapData, MapWorkerPids) ->
    MapDataParts = partition_map_data(MapData, length(MapWorkerPids)),
    
    %% Spread data among the map workers.
    case lists:foreach(lists:zip(MapWorkerPids, MapDataParts)) of
        {MapWorkerPid, MapDataPart} ->
            MapWorkerPid ! {self(), {map_data, MapDataPart}}
    end,
    
    MapResult = collect_map_results(MapWorkerPids),
    MapResult.


%% @doc Executes reduction phase of the map/reduce operation.
%% @spec (ReduceData, ReduceWorkerPids) -> FinalResult where
%%     ReduceData = [{K2,V2}],
%%     ReduceWorkerPids = [pid()],
%%     FinalResult = [{K3,V3}]
execute_reduce_phase(ReduceData, ReduceWorkerPids) ->
    ReduceResult = ReduceData,
    %% TODO: implement reduce phase.
    ReduceResult.

