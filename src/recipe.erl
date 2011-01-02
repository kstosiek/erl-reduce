%% Author: Karol Stosiek (karol.stosiek@gmail.com)
%% Created: 28-11-2010
%% Description: Module containing stub recipe/1 implementation.
-module(recipe).

%%
%% Exported Functions
%%
-export([recipe/1]).

%% @doc Maps given input into intermediate data.
%% @spec (IntermediateKey) -> ReducerPid where
%%     IntermediateKey = K2,
%%     ReducerPid = pid()
%% @throws not_implemented
recipe(_) ->
    throw(not_implemented).
