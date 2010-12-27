%% Author: Karol Stosiek (karol.stosiek@gmail.com)
%% Created: 25-12-2010
%% Description: Implementation of reduce worker, performing the reduce operation
%%    on given input.
-module(reduce_worker).

%%
%% Exported Functions.
%%
-export([run/1]).

%%
%% API Functions.
%%

%% @doc Represents a single map worker node. See the documentation for protocol
%%     definition.
%% @spec ((IntermediateData) -> FinalData) -> () where
%%     IntermediateData = [{K2,V2}],
%%     FinalData = [{K3,V3}]
run(ReduceFunction) ->
    receive
        {MasterPid, {reduce_data, ReduceData}} ->
            ReduceResult = ReduceFunction(ReduceData),
            MasterPid ! {reduce_result, ReduceResult}
    end.

