%% Author: Karol Stosiek (karol.stosiek@gmail.com)
%% Created: 03-01-2011
-module(indexing_recipe_tests).

%%
%% Include files
%%
-include_lib("eunit/include/eunit.hrl").

%%
%% API Functions
%%

recipe_associates_buckets_properly_test() ->
    ReducerPids = ["pid1", "pid2", "pid3"],
    Recipe = indexing_recipe:create_recipe(ReducerPids),
    ?assertEqual("pid1", Recipe("actor")),
    ?assertEqual("pid2", Recipe("mother")),
    ?assertEqual("pid3", Recipe("white")).


recipe_ignores_case_test() ->
    ReducerPids = ["pid1", "pid2", "pid3"],
    Recipe = indexing_recipe:create_recipe(ReducerPids),
    ?assertEqual(Recipe("Actor"), Recipe("actor")).


