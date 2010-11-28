%% Author: Karol Stosiek (karol.stosiek@gmail.com)
%% Created: 04-11-2010
%% Description: A set of high-level tests for the MapReduce implementation.
-module(main_tests).

%%
%% Include files
%%

-include_lib("eunit/include/eunit.hrl").

%%
%% Exported Functions
%%

%% @doc Sanity test for checking if evaluation performs as expected. Implements
%%    a Distributed Grep: The map function emits a line if it matches a given
%%    pattern. The reduce function is an identity function that just copies
%%    the supplied intermediate data to the output.
main_test() ->
	Input = [{"Soft kitty\n"
              "Warm kitty\n"
              "Little ball of fur\n"
              "Happy kitty\n"
              "Sleepy kitty\n"
              "purr, purr, purr", 0},
			 {"Soft kitty\n"
              "Warm kitty\n"
              "Little ball of fur\n"
              "Happy kitty\n"
              "Sleepy kitty\n"
              "Purr, purr, purr", 0}],
	ExpectedOutput = [
			   {"Soft kitty", 0},
               {"Warm kitty", 0},
               {"Happy kitty", 0},
               {"Sleepy kitty", 0},
			   {"Soft kitty", 0},
               {"Warm kitty", 0},
               {"Happy kitty", 0},
               {"Sleepy kitty", 0}],
	?assertEqual(ExpectedOutput, main:eval(Input)).