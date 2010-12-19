%% Author: Karol Stosiek (karol.stosiek@gmail.com)
%% Created: 28-11-2010
%% Description: Module containing stub map/1 implementation.
-module(map).

%%
%% Exported Functions
%%
-export([map/1]).

%% @doc Maps given input into intermediate data.
%% @spec ([{K1,V1}]) -> [{K2, V2}]
%% @throws not_implemented

map(Input) ->
    master:main(Input).
    %% throw(not_implemented).
