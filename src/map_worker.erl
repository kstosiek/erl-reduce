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
            MasterPid ! {map_result, MapResult}
    end.

