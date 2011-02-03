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

% collect_reduce_data test
collect_reduce_data_test() ->
	ReduceData = [{1,"a"}],
    ExpectedResult = [{1, ["a"]}],
	MyPid = self(),
	spawn(fun () ->
				   MyPid ! {self(), {reduce_data, ReduceData}},
				   receive
					   {_, reduce_data_acknowledged} ->
						   MyPid ! {self(), start_reducing}
				   end
		  end),
	{data_collected, _, ActualResult} = reduce_worker:collect_reduce_data(),
	?assertEqual(ExpectedResult, ActualResult).

% Simple reduce_worker protocol test
reduce_worker_protocol_test() ->
    ReduceData = [{1,"a"}],
    ExpectedResult = [{1, "a"}],
    ReduceWorkerPid = spawn(reduce_worker, run, 
                            [fun([{K,V}]) -> [{K, lists:concat(V)}] end]),
    ReduceWorkerPid ! {self(), {reduce_data, ReduceData}},
    receive
        {_, reduce_data_acknowledged} ->
            ReduceWorkerPid ! {self(), start_reducing},
            receive
                {_, {reduce_finished, ActualResult}} ->
                    ?assertEqual(ExpectedResult, ActualResult),
					ReduceWorkerPid ! {self(), map_reducing_complete}
            end
    end.


% Simple reduce_worker test
reduce_worker_successful_computation_test() ->
    ReduceData =
        [
         [{"a", 1}, {"a", 2}, {"b", 4}, {"c", 5}],
         [{"b", 3}, {"b", 2}, {"c", 3}, {"d", 12}, {"d", 12}],
         [{"c", 2}, {"d", 2}, {"d", 4}]
        ],
    ExpectedResult = [{"a", 3}, {"b", 9}, {"c", 10}, {"d", 30}],
    ReduceWorkerPid =
        spawn(reduce_worker, run, 
              [fun(DataToReduce) ->
                       lists:map(fun ({K,V}) ->  {K, lists:sum(V)}  end,
                                 DataToReduce)
               end]),
    % send data to reducer.
    lists:foreach(fun(Data) -> 
                          ReduceWorkerPid ! {self(), {reduce_data, Data}}
                  end, 
                  ReduceData),
    % receive ack messages.
    lists:foreach(fun(_) -> 
                          receive
                              {_, reduce_data_acknowledged} -> void
                          end
                  end,
                  ReduceData),
    % start reducing
    ReduceWorkerPid ! {self(), start_reducing},
    receive
        {_, {reduce_finished, ActualResult}} ->
            SortedResult = lists:sort(fun({K1,_}, {K2,_}) ->
                                              K1 < K2 end,
                                      ActualResult),
            ?assertEqual(ExpectedResult, SortedResult),
			ReduceWorkerPid ! {self(), map_reducing_complete}
    end.