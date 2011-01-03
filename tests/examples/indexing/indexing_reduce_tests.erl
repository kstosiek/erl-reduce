%% Author: Karol Stosiek (karol.stosiek@gmail.com)
%% Created: 09-12-2010
-module(indexing_reduce_tests).

%%
%% Include files
%%
-include_lib("eunit/include/eunit.hrl").

%%
%% API Functions
%%

reduce_joins_the_same_words_test() ->
    TestData = [{"Hello", 1}, {"Hello", 2}],
    ExpectedResult = [{"Hello", [2,1]}],
    ActualResult = indexing_reduce:reduce(TestData),
    ?assertEqual(ExpectedResult, ActualResult).

reduce_does_not_join_different_words_test() ->
    TestData = [{"Hello", 1}, {"kitty", 2}],
    ExpectedResult = [{"Hello", [1]}, {"kitty", [2]}],
    ActualResult = indexing_reduce:reduce(TestData),
    ?assertEqual(ExpectedResult, ActualResult).

