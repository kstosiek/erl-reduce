%% Author: Karol Stosiek (karol.stosiek@gmail.com)
%% Created: 25-12-2010
%% Description: Implementation of map worker, performing the map operation
%%    on given input.
-module(map_worker).

%%
%% Exported Functions.
%%
-export([run/1]).

%%
%% API Functions.
%%

%% @doc Represents a single map worker node. See the documentation for protocol
%%     definition.
%% @spec ((MapData)->IntermediateData) -> () where
%%     MapData = [{K1,V1}],
%%     IntermediateData = [{K2,V2}]
run(MapFunction) ->
    error_logger:info_msg("Map worker ~p started; waiting for map data...",
                          [self()]),
    receive
        {MasterPid, {map_data, MapData}} ->
            error_logger:info_msg("Received map data; mapping..."),
            
            MapResult = MapFunction(MapData),
            MasterPid ! {self(), map_finished},
            
            error_logger:info_msg("Mapping finished; waiting for recipe..."),
            
            receive
                {_, {recipe, Recipe}} ->
                    error_logger:info_msg("Received recipe; splitting data..."),
                    
                    ReducerPidsWithData = split_data_among_reducers(MapResult,
                                                                    Recipe),
                    ReducerPids = dict:fetch_keys(ReducerPidsWithData),
                    
                    error_logger:info_msg("Sending data to reducers ~p...",
                                          [ReducerPids]),
                    send_data_to_reducers(ReducerPids, ReducerPidsWithData),
                    
                    error_logger:info_msg("Collecting acknowledgements from "
                                              "reducers ~p...", [ReducerPids]),
                    collect_acknowledgements(ReducerPids),
                    
                    error_logger:info_msg("Notifying master mapper ~p is done "
                                              "and quitting.", [self()]),
                    MasterPid ! {self(), map_send_finished}
            end
    end.


%%
%% Local Functions.
%%

%% @doc Sends all data to reducers.
%% @spec (ReducerPids, ReducerPidsWithData) -> void() where
%%     ReducerPids = [pid()],
%%     ReducerPidsWithData = dictionary()
%% @private
send_data_to_reducers(ReducerPids, ReducerPidsWithData) ->
    lists:foreach(fun (ReducerPid) ->
                           error_logger:info_msg("Sending data to reducer ~p.",
                                                 [ReducerPid]),
                           
                           ReduceData = dict:fetch(ReducerPid,
                                                   ReducerPidsWithData),
                           ReducerPid ! {self(), {reduce_data, ReduceData}}
                  end, ReducerPids).


%% @doc Collects reduce data receival acknowledgements from the given set
%%     of reducers.
%%     TODO: this will loop forever in case of a reducer failing to send
%%     acknowledgements. Implement a fix.
%% @spec (RemainingReducerPids) -> void() where
%%     RemainingReducerPids = set()
%% @private
collect_acknowledgements_loop(RemainingReducerPids) ->
    case sets:size(RemainingReducerPids) of
        0 ->
            error_logger:info_msg("Acknowledgements from reducers collected!"),
            void;
        
        _ -> 
            receive
                {ReducerPid, reduce_data_acknowledged} ->
                    NewRemainingReducerPids = sets:del_element(
                                                ReducerPid,
                                                RemainingReducerPids),
                    
                    error_logger:info_msg(
                      "Received acknowledgement from reducer ~p; "
                          "waiting for ~p.",
                          [ReducerPid, sets:to_list(NewRemainingReducerPids)]),
                    
                    collect_acknowledgements_loop(NewRemainingReducerPids)
            end 
    end.


%% @doc Collects acknowledgements from each of given reducers.
%% @spec (ReducerPids) -> void() where
%%     ReducerPids = [pid()]
%% @private
collect_acknowledgements(ReducerPids) ->
    collect_acknowledgements_loop(sets:from_list(ReducerPids)).


%% @doc Given a recipe, creates a mapping from reducer pid to a list with data
%%    to be sent to it.
%% @spec (Data, Recipe) -> ReducerPidsWithData where
%%     Data = [{K2,V2}],
%%     Recipe = K2 -> ReducerPid,
%%     ReducerPid = pid(),
%%     ReducerPidsWithData = dict()
%% @private
split_data_among_reducers(Data, Recipe) ->
    lists:foldl(fun ({Key, Value}, ReducerPidWithData) ->
                         DestinationReducerPid = Recipe(Key),
                         dict:update(DestinationReducerPid,
                                     fun (OldData) ->
                                              [{Key, [Value]} | OldData]
                                     end,
                                     [{Key, Value}],
                                     ReducerPidWithData)
                end, dict:new(), Data).
