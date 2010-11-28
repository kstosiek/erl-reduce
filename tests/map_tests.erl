%% Author: Karol Stosiek (karol.stosiek@gmail.com)
%% Created: 28-11-2010
-module(map_tests).

%%
%% Include files
%%
-include_lib("eunit/include/eunit.hrl").

%%
%% Exported Functions
%%
map_splits_string_by_newlines_test() ->
	?assertEqual([{"kitty", 0}],
				 map:map([{"Soft" "\n" "kitty", 0}])).


