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
