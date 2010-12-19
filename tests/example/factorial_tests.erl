%% Author: Karol Stosiek (karol.stosiek@gmail.com)
%% Created: 04-11-2010
%% Description: Tests for factorial module. Example code for developers to get
%%     familiar with basic Erlang code and project structure.
-module(factorial_tests).

%%
%% Include files
%% Necessary includes, along with eunit library, are included here.
-include_lib("eunit/include/eunit.hrl").

%%
%% Exported Functions
%% All eunit functions which match *_test format are exported by default.
factorial_for_zero_test() -> ?assert(factorial:fact(0) =:= 1).
factorial_for_one_test() -> ?assert(factorial:fact(1) =:= 1).
factorial_for_n_test() -> ?assert(factorial:fact(4) =:= 24).