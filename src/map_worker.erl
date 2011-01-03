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
    receive
        {MasterPid, {map_data, MapData}} ->            
            MapResult = MapFunction(MapData),
            MasterPid ! {self(), map_finished},
            receive
                {_, {recipe, Recipe}} ->
                    ReducerPidsWithData = split_data_among_reducers(MapResult,
                                                                     Recipe),
                    ReducerPids = dict:fetch_keys(ReducerPidsWithData),
                    
                    send_data_to_reducers(ReducerPids, ReducerPidsWithData),
                    collect_acknowledgements(ReducerPids),
                    
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
        0 -> void;

        _ -> 
            receive
                {ReducerPid, reduce_data_acknowledged} ->
                    collect_acknowledgements_loop(
                      sets:del_element(ReducerPid, RemainingReducerPids))
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
                         dict:update(Recipe(Key),
                                     fun (OldData) ->
                                              [{Key, Value} | OldData]
                                     end,
                                     [{Key, Value}],
                                     ReducerPidWithData)
                end, dict:new(), Data).
