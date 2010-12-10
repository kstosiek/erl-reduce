%% Author: Karol Stosiek (karol.stosiek@gmail.com)
%% Created: 04-11-2010
%% Description: Module with factorial function. Example code for developers
%%     to get familiar with basic Erlang code and project structure.
-module(factorial).

%%
%% Include files
%% Here we put all necessary include's.

%%
%% Exported Functions
%% Here we export functions.
-export([fact/1]).

fact(N) when N>0 ->
  N * fact (N-1);
fact(0) ->
  1.

%%
%% API Functions
%% Here we put API functions.

%%
%% Local Functions
%% Local functions are located here.

