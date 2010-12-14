%% Author: Karol Stosiek (karol.stosiek@gmail.com)
%% Created: 28-11-2010
%% Description: Implementation of reduce operation for Word Indexing problem.
-module(indexing_reduce).

%%
%% Exported Functions
%%
-export([reduce/1]).

%% @doc Reduces intermediate data to final data. In current implementation
%%     it is an identity function.
%% @spec ([{K2,V2}]) -> [{K3, V3}]
reduce (WordsWithLineNumbers) ->
	AddWordToIndex =
		fun({Word, LineNumber}, WordIndex) ->
				dict:update(Word,
							fun (WordLineNumbers) ->
									 [LineNumber|WordLineNumbers]
							end, [LineNumber], WordIndex)
		end,
	WordIndex = lists:foldl(AddWordToIndex, dict:new(), WordsWithLineNumbers),
	lists:sort(dict:to_list(WordIndex)).

