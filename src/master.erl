%% Author: Marcin Milewski (mmilewski@gmail.com),
%%         Karol Stosiek (karol.stosiek@gmail.com)
%%         Piotr Polesiuk (bassists@o2.pl)
%% Created: 21-11-2010
%% Description: Coordinates the map/reduce computation: feeds workers with data,
%%     takes care of dying workers and returns collected data to the user.
-module(master).

%%
%% Exported Functions.
%% TODO: move partition_map_data/2 to a separate module.
-export([run/4,
         partition_map_data/2,
         execute_map_phase/4, % exported for testing purposes.
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
    error_logger:info_msg("Starting master (~p).", [self()]),
    
    RemainingReducerPids = 
		execute_map_phase(InputData, MapWorkerPids, ReduceWorkerPids, Recipe),
    execute_reduce_phase(RemainingReducerPids).


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
%%    TODO: fault tolerance extention protocol implementation.
%% @spec (MapData, MapWorkerPids, ReduceWorkerPids, Recipe) -> RemainingReducerPids where
%%     MapData = [{K1,V1}],
%%     MapWorkerPids = [pid()],
%%     ReduceWorkerPids = [pid()],
%%     Recipe = (K2) -> ReducerPid,
%%     RemainingReducerPids = [pid()]
%% @private
execute_map_phase(MapData, MapWorkerPids, ReduceWorkerPids, Recipe) ->
    error_logger:info_msg("Starting map phase with map workers ~p",
                          [MapWorkerPids]),
    
    MapDataParts = partition_map_data(MapData, length(MapWorkerPids)),
    
    
    % Spread data among the map workers.
    error_logger:info_msg("Spreading map data among map workers ~p",
                          [MapWorkerPids]),
    lists:foreach(fun({MapWorkerPid, MapDataPart}) ->
                          MapWorkerPid ! {self(), {map_data, MapDataPart}}
                  end,
                  lists:zip(MapWorkerPids, MapDataParts)),
    
    % Collect map_finished messages and send the recipe.
    error_logger:info_msg("Collecting map_finished messages and sending "
                              "the recipes..."),
    lists:foreach(fun (_) ->
                           receive
                               {MapperPid, map_finished} ->
                                   error_logger:info_msg(
                                     "Received {map_finished} message from ~p; "
                                         "sending the recipe...",
                                         [MapperPid]),
                                   
                                   MapperPid ! {self(), {recipe, Recipe}}
                           end
                  end, MapWorkerPids),
    
	% Create monitors for reducres
	error_logger:info_msg(
		"Creating monitors for reduce workers ~p", 
		[ReduceWorkerPids]),
	
	spawn(monitors, monitor_reduce_workers, [self(), ReduceWorkerPids]),
    
    % Collect map_send_finished messages.
    error_logger:info_msg("Collecting map_send_finished messages..."),
    RemainingReducerPids = collect_map_send_finished(MapWorkerPids, ReduceWorkerPids),
    
    error_logger:info_msg("Map phase finished. Remaining reduce workers : ~p",
						  [RemainingReducerPids]),
	RemainingReducerPids.

%% @doc Collects map_send_finished messages. When reducer is down, sends 
%%    reduce_worker_down message to map workers.
%% @spec (MapWorkerPids, ReduceWorkerPids) -> RemainingReducerPids where
%%    MapWorkerPids = [pid()],
%%    ReduceWorkerPids = [pid()],
%%    RemainingReducerPids = [pid()]
%% @private
collect_map_send_finished([], ReduceWorkerPids) ->
	ReduceWorkerPids;
collect_map_send_finished(MapWorkerPids, ReduceWorkerPids) ->
	receive
		{MapperPid, map_send_finished} ->
			NewMapWorkerPids = lists:delete(MapperPid, MapWorkerPids),
			collect_map_send_finished(NewMapWorkerPids, ReduceWorkerPids);
		{ReducerPid, reduce_worker_down} ->
			% inform mappers about dead reducer.
			lists:foreach(fun(MapWorkerPid) ->
								  MapWorkerPid ! {ReducerPid, reduce_worker_down}
						  end, MapWorkerPids),
			
			NewReduceWorkerPids = lists:delete(ReducerPid, ReduceWorkerPids),
			collect_map_send_finished(MapWorkerPids, NewReduceWorkerPids)
	end.


%% @doc Executes reduction phase of the map/reduce operation.
%% @spec (ReduceData, ReduceWorkerPids) -> FinalResult where
%%     ReduceData = [{K2,V2}],
%%     ReduceWorkerPids = [pid()],
%%     FinalResult = [{K3,V3}]
%% @private
execute_reduce_phase(ReduceWorkerPids) ->
    error_logger:info_msg("Starting reduce phase with reduce workers ~p",
                          [ReduceWorkerPids]),
	
    % Initiate reduction.
    error_logger:info_msg("Sending start signal to reduce workers ~p", 
                          [ReduceWorkerPids]),
    lists:foreach(fun (ReducerPid) ->
                           error_logger:info_msg(
                             "Sending start signal to reduce worker ~p",
                             [ReducerPid]),
                           
                           ReducerPid ! {self(), start_reducing}
                  end, ReduceWorkerPids),
    
    % Collect and return final results.
    error_logger:info_msg("Collecting final results from reduce workers ~p",
                          [ReduceWorkerPids]),
	
    collect_reduce_phase_results(ReduceWorkerPids, []).

%% @doc Collects all results of reducing phase. When one of reduce wokres fails,
%%    function splits his job between other reducers (fault tolerance).
%% @spec (ReduceWorkerPids, Accumulator) ->
%%    ReduceWorkerPids = [pid()],
%%    Accumulator = [{K3,V3}]
%% @private
collect_reduce_phase_results([], Accumulator) ->
	Accumulator;
collect_reduce_phase_results(ReduceWorkerPids, Accumulator) ->
	receive
		{ReducerPid, {reduce_finished, ReduceResult}} ->
			error_logger:info_msg(
			  "Received final data from reducer ~p.", 
			  [ReducerPid]),
			
			RemainingReducerPids = lists:delete(ReducerPid, ReduceWorkerPids),
			collect_reduce_phase_results(RemainingReducerPids, ReduceResult ++ Accumulator);
		
		{ReducerPid, reduce_worker_down} ->
			error_logger:info_msg(
			  "Reduce worker ~p is down",
			  [ReducerPid]),
			
			% TODO: fault tolerance
			
			RemainingReducerPids = lists:filter(fun (Pid) -> 
														 Pid =/= ReducerPid end, 
												Accumulator),
			collect_reduce_phase_results(RemainingReducerPids, Accumulator)
	end.
