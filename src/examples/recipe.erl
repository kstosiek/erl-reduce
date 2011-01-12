%% Author: Karol Stosiek (karol.stosiek@gmail.com)
%% Created: 28-11-2010
%% Description: Module containing stub recipe/1 implementation.
-module(recipe).

%%
%% Exported Functions
%%
-export([create_recipe/1]).

%%
%% API Functions.
%%

%% @doc Given a list of reducer pids, creates a recipe function. 
%% @spec (ReducerPids) -> (IntermediateKey) -> ReducerPid where
%%     IntermediateKey = K2,
%%     ReducerPids = [pid()],
%%     ReducerPid = pid()
%% @throws not_implemented
create_recipe(_) ->
    throw(not_implemented).
