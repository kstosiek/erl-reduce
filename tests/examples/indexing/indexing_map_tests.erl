%% Author: Karol Stosiek (karol.stosiek@gmail.com)
%% Created: 04-11-2010
-module(indexing_map_tests).

%%
%% Include files
%%
-include_lib("eunit/include/eunit.hrl").

%%
%% API Functions
%%

map_splitting_words_by_space_test() ->
	TestData = [{"hello kitty", 1}],
	ExpectedResult = [{"hello", 1}, {"kitty",1}],
	ActualResult = indexing_map:map(TestData),
	?assertEqual(ExpectedResult, ActualResult).

map_splitting_words_by_comma_test() ->
	TestData = [{"hello,kitty", 1}],
	ExpectedResult = [{"hello", 1}, {"kitty",1}],
	ActualResult = indexing_map:map(TestData),
	?assertEqual(ExpectedResult, ActualResult).

map_splitting_words_by_colon_test() ->
	TestData = [{"hello:kitty", 1}],
	ExpectedResult = [{"hello", 1}, {"kitty",1}],
	ActualResult = indexing_map:map(TestData),
	?assertEqual(ExpectedResult, ActualResult).

map_splitting_words_by_semicolon_test() ->
	TestData = [{"hello;kitty", 1}],
	ExpectedResult = [{"hello", 1}, {"kitty",1}],
	ActualResult = indexing_map:map(TestData),
	?assertEqual(ExpectedResult, ActualResult).

map_splitting_words_by_exclamation_mark_test() ->
	TestData = [{"hello!kitty", 1}],
	ExpectedResult = [{"hello", 1}, {"kitty",1}],
	ActualResult = indexing_map:map(TestData),
	?assertEqual(ExpectedResult, ActualResult).

map_splitting_words_by_question_mark_test() ->
	TestData = [{"hello?kitty", 1}],
	ExpectedResult = [{"hello", 1}, {"kitty",1}],
	ActualResult = indexing_map:map(TestData),
	?assertEqual(ExpectedResult, ActualResult).

map_not_splitting_words_by_apostrophe_test() ->
	TestData = [{"hello'kitty", 1}],
	ExpectedResult = [{"hello'kitty", 1}],
	ActualResult = indexing_map:map(TestData),
	?assertEqual(ExpectedResult, ActualResult).

map_does_not_produce_empty_words_test() ->
	TestData = [{"hello!? kitty?", 1}],
	ExpectedResult = [{"hello", 1}, {"kitty",1}],
	ActualResult = indexing_map:map(TestData),
	?assertEqual(ExpectedResult, ActualResult).

map_converts_words_to_lower_case_test() ->
	TestData = [{"Hello kitty?", 1}],
	ExpectedResult = [{"hello", 1}, {"kitty",1}],
	ActualResult = indexing_map:map(TestData),
	?assertEqual(ExpectedResult, ActualResult).
