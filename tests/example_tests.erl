%% Author: Karol Stosiek (karol.stosiek@gmail.com)
%% Created: 04-11-2010
%% Description: TODO: Add description to example_tests
-module(example_tests).

%%
%% Include files
%%

-include_lib("eunit/include/eunit.hrl").

%%
%% Exported Functions
%%

factorial_for_zero_test() -> ?assert(example:fact(0) =:= 1).
factorial_for_one_test() -> ?assert(example:fact(1) =:= 1).
factorial_for_n_test() -> ?assert(example:fact(4) =:= 24).

%%
%% API Functions
%%


%%
%% Local Functions
%%

