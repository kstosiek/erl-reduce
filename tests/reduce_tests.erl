%% Author: Karol Stosiek (karol.stosiek@gmail.com)
%% Created: 28-11-2010
-module(reduce_tests).

%%
%% Include files
%%
-include_lib("eunit/include/eunit.hrl").

%%
%% Exported Functions
%%
reduce_test() -> ?assert(reduce:reduce([{"x", 1}]) =:= [{"x", 1}]).