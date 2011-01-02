%% Author: Marcin Milewski (mmilewski@gmail.com),
%%         Karol Stosiek (karol.stosiek@gmail.com)
%% Created: 21-11-2010
%% Description: Coordinates the map/reduce computation: feeds workers with data,
%%     takes care of dying workers and returns collected data to the user.
-module(master).

%%
%% Exported Functions.
%% TODO: move partition_map_data/2 to a separate module.
-export([run/4,
         partition_map_data/2,
         execute_map_phase/3, % exported for testing purposes.
         execute_reduce_phase/1 % exported for testing purporses.
        ]).


%%
%% API Functions.
%%

%% @doc Main master function. Executes map/reduce operation on given input
%%     with given map and reduce workers. Returns overall result. We require
%%     to have at least one map and one reduce worker pid available.
%% @spec (MapWorkerPids, ReduceWorkerPids, InputData) -> FinalResult where
%%    MapWorderPids = [pid()],
%%    ReduceWorkerPids = [pid()],
%%    InputData = [{K1,V1}],
%%    FinalResult = [{K3,V3}]
run(MapWorkerPids, ReduceWorkerPids, InputData, Recipe)
  when length(MapWorkerPids) > 0,
       length(ReduceWorkerPids) > 0 ->   
    execute_map_phase(InputData, MapWorkerPids, Recipe),
    execute_reduce_phase(ReduceWorkerPids).


%%
%% Local Functions.
%%


%% @doc Partitions given Data into chunks of at most ChunkSize and appends
%%     resulting chunk to the Accumulator. If there is at least ChunkSize
%%     elements in the given list, then chunk size is ChunkSize; otherwise
%%     there is at most as many elements in the chunk, as in the given list.
%%     TODO: make the difference in chunk sizes be at most 1 element.
%% @spec (Data,ChunkSize,Accumulator) -> PartitionedMapData where
%%     Data = [{K1,V1}],
%%     ChunkSize = int(),
%%     Accumulator = PartitionedMapData = [Data]
partition_data_with_accumulator([], _, Accumulator) ->
    Accumulator;

partition_data_with_accumulator(Data, ChunkSize, Accumulator)
  when length(Data) < ChunkSize ->
    Accumulator;

partition_data_with_accumulator(Data, ChunkSize, Accumulator) ->
    {Chunk, OtherData} = lists:split(ChunkSize, Data),
    partition_data_with_accumulator(OtherData, ChunkSize, [Chunk|Accumulator]).


%% @doc Parititions data into almost evenly-sized data chunks. Last chunk may
%%     not be as big as other chunks.
%%     TODO: make the difference in chunk sizes be at most 1 element.
%% @spec (Data,Chunks) -> [Data] where
%%     Data = [{K1,V1}],
%%     Chunks = int()
partition_map_data(Data, Chunks) ->
    partition_data_with_accumulator(Data, round(length(Data) / Chunks), []).


%% @doc Sends partitioned data to map workers and collects results.
%% @spec (MapData, MapWorkerPids, Recipe) -> IntermediateData where
%%     MapData = [{K1,V1}],
%%     MapWorkerPids = [pid()],
%%     IntermediateData = [{K2,V2}],
%%     Recipe = (K2) -> ReducerPid,
%%     ReducerPid = pid()
%% @private
execute_map_phase(MapData, MapWorkerPids, Recipe) ->
    MapDataParts = partition_map_data(MapData, length(MapWorkerPids)),
    
    % Spread data among the map workers.
    lists:foreach(fun({MapWorkerPid, MapDataPart}) ->
                          MapWorkerPid ! {self(), {map_data, MapDataPart}}
                  end,
                  lists:zip(MapWorkerPids, MapDataParts)),
    
    % Collect map_finished messages and send the recipe.
    lists:foreach(fun (_) ->
                           receive
                               {MapperPid, map_finished} ->
                                   MapperPid ! {self(), {recipe, Recipe}}
                           end
                  end, MapWorkerPids),

    % Collect map_send_finished messages.
    lists:foreach(fun (_) ->
                           receive
                               {_, map_send_finished} ->
                                   ok
                           end
                  end, MapWorkerPids).


%% @doc Executes reduction phase of the map/reduce operation.
%% @spec (ReduceData, ReduceWorkerPids) -> FinalResult where
%%     ReduceData = [{K2,V2}],
%%     ReduceWorkerPids = [pid()],
%%     FinalResult = [{K3,V3}]
%% @private
execute_reduce_phase(ReduceWorkerPids) ->
    % Initiate reduction.
    lists:foreach(fun (ReducerPid) ->
                           ReducerPid ! {self(), start_reducing}
                  end, ReduceWorkerPids),

    % Collect and return final results.
    lists:foldl(fun (_, ReduceResults) ->
                         receive
                             {_, {reduce_finished, ReduceResult}} ->
                                 ReduceResult ++ ReduceResults
                         end
                end, [], ReduceWorkerPids).
    
    

