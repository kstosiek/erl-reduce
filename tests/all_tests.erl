%% Author: Karol Stosiek (karol.stosiek@gmail.com)
%% Created: 07-11-2010
%% Description: TODO: Add description to all_tests
-module(all_tests).

%%
%% Exported Functions
%%
-export([run/0]).

run() -> eunit:test({dir, "erl-reduce/ebin"}).
