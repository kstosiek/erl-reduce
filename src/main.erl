%% Author: Karol Stosiek (karol.stosiek@gmail.com)
%% Created: 07-11-2010
%% Description: MapReduce program entry.
-module(main).

%%
%% Exported Functions
%%
-export([eval/1]).

%% @doc Runs MapReduce on given input. Definitions of map/1 and reduce/1
%%     operations are available in map and reduce modules, respectively.
%% @spec ([{K1,V1}]) -> [{K3,V3}].
eval(Input) ->
	reduce:reduce(map:map(Input)).

	
