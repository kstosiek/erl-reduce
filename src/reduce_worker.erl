%% Author: Piotr Polesiuk (bassists@o2.pl),
%%         Karol Stosiek (karol.stosiek@gmail.com)
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

%% @doc Represents a single reduce worker node. See the documentation for
%%     protocol definition.
%% @spec ((IntermediateData) -> FinalData) -> () where
%%     IntermediateData = {K2,[V2]},
%%     FinalData = [{K3,V3}]
run(ReduceFunction) ->
    {MasterPid, ReduceData} = collect_reduce_data(),
    ReduceResult = lists:map(ReduceFunction, ReduceData),
    MasterPid ! {self(), {reduce_finished, ReduceResult}}.


%%
%% Local Functions.
%%

%% @doc Receives map results from mappers and collects them into accumulator 
%%     (CollectedResultsDict) until receives start_reducing message from master.
%%     Function returns master PID and collected data.
%% @spec Accumulator -> {MasterPid, CollectedData} where
%%     Accumulator = dictionary(),
%%     MasterPid = pid(),
%%     CollectedData = [{K2,[V2]}]
%% @private
collect_reduce_data_loop(CollectedResultsDict) ->
    receive
        {MapperPid, {reduce_data, ReduceData}} ->
            NewCollectedResults = 
                lists:foldl(fun({Key, Value}, Dict) ->
                                    dict:append_list(Key, Value, Dict)
                            end, 
                            CollectedResultsDict, ReduceData),
            MapperPid ! {self(), reduce_data_acknowledged},
            collect_reduce_data_loop(NewCollectedResults);
        {MasterPid, start_reducing} ->
            {MasterPid, dict:to_list(CollectedResultsDict)}
    end.


%% @doc Collects map results from mappers until receives 
%%     start_reducing message from master. Function returns master PID
%%     and collected data.
%% @spec () -> {MasterPid, CollectedData} where
%%     MasterPid = pid(),
%%     CollectedData = [{K2,[V2]}]
%% @private
collect_reduce_data() ->
    collect_reduce_data_loop(dict:new()).
