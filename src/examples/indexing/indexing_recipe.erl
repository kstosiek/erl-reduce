%% Author: karol
%% Created: 03-01-2011
%% Description: TODO: Add description to indexing_recipe
-module(indexing_recipe).

%%
%% Include files
%%

%%
%% Exported Functions
%%
-export([create_recipe/1]).

%%
%% API Functions
%%


%% @doc Given a list of reducer pids, creates a recipe function. The recipe
%%     function divides the English alphabet into exactly length(ReducerPids)
%%     "buckets" and then, given a word, finds the "bucket" the word's first
%%     letter falls into and returns the pid of a reducer that is "associated"
%%     with this bucket. Buckets are chunks of consecutive letters in the
%%     alphabet.
%% @spec (ReducerPids) -> (IntermediateKey) -> ReducerPid where
%%     IntermediateKey = K2 = Word = string(),
%%     ReducerPids = [pid()],
%%     ReducerPid = pid()
create_recipe(ReducerPids) ->
    BucketSize = 26 div max(length(ReducerPids) - 1, 1),
    fun (Word) ->
             FirstLetterOfTheWord = string:left(Word, 1),
             PositionInEnglishAlphabet = string:str(
                                           "abcdefghijklmnopqrstuvwxyz",
                                           string:to_lower(
                                             FirstLetterOfTheWord)),
             lists:nth(min((PositionInEnglishAlphabet div BucketSize) + 1,
                           length(ReducerPids)),
                       ReducerPids)
             end. 
