%% Author: Karol Stosiek (karol.stosiek@gmail.com)
%% Created: 25-12-2010
-module(reduce_worker_tests).

%%
%% Include files
%%
-include_lib("eunit/include/eunit.hrl").

%%
%% API Functions
%%

reduce_phase_successful_computation_test() ->
    ReduceData = [{1,"a"}],
    ActualResult = master:execute_reduce_phase(ReduceData, []),
    ExpectedResult = ReduceData,
    ?assertEqual(ExpectedResult, ActualResult).

% Simple reduce_worker test
reduce_worker_test() ->
	ReduceData = [{1,"a"}],
	ExpectedResult = ["a"],
	ReduceWorkerPid = spawn(reduce_worker, run, 
							[fun({_,V}) -> V end]),
	ReduceWorkerPid ! {self(), {reduce_data, ReduceData}},
	receive
		{_, reduce_data_acknowledged} ->
			ReduceWorkerPid ! {self(), start_reducing},
			receive
				{_, {reduce_finished, ActualResult}} ->
					?assertEqual(ExpectedResult, ActualResult)
			end
	end.
