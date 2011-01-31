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
    fun (MapData) -> indexing_map:map(MapData) end.


%% @doc Reference to the reduce function used in given computation. This is
%%     where the problem-related reduce function may be plugged. It is
%%     strongly advised to define the custom reduce function in a separate
%%     module.
%% @spec (IntermediateData) -> FinalData where
%%     IntermediateData = [{K2,V2}],
%%     FinalData = [{K3,V3}]
reduce_function() ->
    fun (IntermediateData) -> indexing_reduce:reduce(IntermediateData) end.


%% @doc Reference to the recipe function used in given computation. This is
%%     where the problem-related recipe function may be plugged. It is
%%     strongly advised to define the custom recipe in a separate module.
%% @spec (IntermediateData) -> FinalData where
%%     IntermediateData = [{K2,V2}],
%%     FinalData = [{K3,V3}]
recipe(ReduceNodes) ->
    indexing_recipe:create_recipe(ReduceNodes).


%% @doc Reference to the data function used in given computation. This is
%%     where the problem-related data function may be plugged. It is
%%     strongly advised to define the custom data function in a separate module.
%% @spec () -> MapData where
%%     MapData = [{K1,V1}]
input_data() ->
    indexing_input:data().


%% @doc List of nodes participating in the map/reduce computation
%%     instance as hosts for map workers. You can provide your own names here
%%     depending on the network you are configuring. Number of names returned
%%     depends on implementation, obviously.
%% @spec () -> [string()].
map_worker_nodes() ->
    [map1@yennefer, map2@yennefer].


%% @doc List of nodes participating in the map/reduce computation
%%     instance as hosts for reduce workers. You can provide your own names
%%     here depending on the network you are configuring. Number of names
%%     returned depends on implementation, obviously.
%% @spec () -> [string()].
reduce_worker_nodes() ->
    [reduce1@yennefer].

