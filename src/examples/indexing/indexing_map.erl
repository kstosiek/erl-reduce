%% Author: Karol Stosiek (karol.stosiek@gmail.com)
%% Created: 28-11-2010
%% Description: Implementation of map operation for Word Indexing problem.
-module(indexing_map).

%%
%% Exported Functions
%%
-export([map/1]).

%%
%% API Functions
%%

%%  @doc Maps given list of lines with their line numbers into list of words
%%      and line numbers of which these words occur on.
%%  @spec ([{K1,V1}]) -> [{K2, V2}]
map(Input) ->
    SplitLineIntoWordsWithLineNumber =
        fun ({Line, LineNumber}) ->
                 WordsInLine = re:split(Line, "[^A-Za-z'-]+",
                                        [{return, list}, trim]),
                 SingleWordToLowerWordAndLineNumber =
                     fun (Word) -> {string:to_lower(Word), LineNumber} end,
                 lists:map(SingleWordToLowerWordAndLineNumber, WordsInLine)
        end,
    lists:flatmap(SplitLineIntoWordsWithLineNumber, Input).
