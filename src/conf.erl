%% Author: Marcin Milewski (mmilewski@gmail.com)
%% Created: 17-12-2010
%% Description: Configuration values for map/reduce computation instance.
-module(conf).
-compile(export_all).

%% @doc Reference to the map function used in given computation. This is where
%%     the problem-related map function may be plugged. It is strongly advised
%%     to define the custom map function in a separate module.
%% @spec (MapData) -> IntermediateData where
%%     MapData = [{K1,V1}],
%%     IntermediateData = [{K2,V2}]
map_function() ->
    fun (MapData) -> map:map(MapData) end.

%% @doc Reference to the reduce function used in given computation. This is
%%     where the problem-related reduce function may be plugged. It is
%%     strongly advised to define the custom map function in a separate module.
%% @spec (IntermediateData) -> FinalData where
%%     IntermediateData = [{K2,V2}],
%%     FinalData = [{K3,V3}]
reduce_function() ->
    fun (IntermediateData) -> reduce:reduce(IntermediateData) end.


%% @doc List of nodes participating in the map/reduce computation
%%     instance as hosts for map workers. You can provide your own names here
%%     depending on the network you are configuring. Number of names returned
%%     depends on implementation, obviously.
%% @spec () -> [string()].
map_worker_nodes() ->
    ['map1@localhost', 'map2@localhost', 'map3@localhost'].



%% @doc List of nodes participating in the map/reduce computation
%%     instance as hosts for reduce workers. You can provide your own names
%%     here depending on the network you are configuring. Number of names
%%     returned depends on implementation, obviously.
%% @spec () -> [string()].
reduce_worker_nodes() ->
    ['reduce1@localhost', 'reduce2@localhost', 'reduce3@localhost'].

