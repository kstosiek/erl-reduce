%% Author: Karol Stosiek (karol.stosiek@gmail.com)
%% Created: 28-11-2010
%% Description: Module containing example implementation of reduce operation.
%%     Distributed Grep: The map function emits a line if it matches a given
%%     pattern. The reduce function is an identity function that just copies
%%     the supplied intermediate data to the output.
-module(reduce).

%%
%% Exported Functions
%%
-export([reduce/1]).

%% @doc Reduces intermediate data to final data. In current implementation
%%     it is an identity function.
%% @spec ([{K2,V2}]) -> [{K3, V3}]
reduce (IntermediateData) -> 
	IntermediateData.
