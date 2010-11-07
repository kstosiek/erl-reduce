%% Author: karol
%% Created: 04-11-2010
%% Description: TODO: Add description to example
-module(factorial).

%%
%% Include files
%%

%%
%% Exported Functions
%%
-export([fact/1]).

fact(N) when N>0 ->
  N * fact (N-1);
fact(0) ->
  1.

%%
%% API Functions
%%



%%
%% Local Functions
%%

