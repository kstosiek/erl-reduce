%% Author: Karol Stosiek (karol.stosiek@gmail.com)
%% Created: 09-12-2010
%% Description: Implementation of Map-Reduce solution for Word Indexing problem:
%%     create an "index" of a body of text -- given a text file, output a list
%%     of words annotated with the line-number at which each word appears. 
-module(indexing_input).

%%
%% Exported Functions
%%
-export([data/0]).

%%
%% API Functions
%%

%% @doc Returns map data for the indexing problem. Currently returns
%%     the contents of "Tale of Two Cities" novel.
%% @spec () -> MapData where
%%     MapData = [{K1,V1}],
%%     K1 = LineContents = string(),
%%     V1 = LineNo = int().
%% @throws {error, atom()} Thrown on I/O errors.
data() ->
    get_file_contents("../src/examples/indexing/data/tale_of_two_cities.txt").

%%
%% Private Functions.
%%

%% @doc Runs the Map-Reduce solution for Word Indexing problem.
%%     Reads file contents to the accumulator per line, decorating each line
%%     with it's number in the file. After reading the given file, it runs
%%     map & reduce operations on the resulting annotated file lines, returning
%%     and index of form [{string(), [int()]}], where the first pair element
%%     is a word and the second element is a list of lines, on which the word
%%     in question occurs.
%% @spec (string()) -> [{string(), [int()]}]
%% @throws {error, atom()} Thrown on I/O errors.
get_file_contents(FileName) ->
    case file:open(FileName, read) of
        {ok, IoDevice} -> 
            get_file_contents_as_lines_with_line_no(IoDevice);
        {error, Reason} ->
            throw({error, Reason})
    end.

%%
%% Local Functions
%%

%% @doc Reads file contents to the accumulator per line, decorating each line
%%     with it's number in the file.
%% @spec (io_device(), int(), [{string(), int()}]) -> [{string(), int()}]
%% @throws {error, atom()} Thrown on I/O errors.
append_file_contents_with_line_no(IoDevice, CurrentLineNumber, Accumulator) ->
    case io:get_line(IoDevice, "") of
        eof  ->
            file:close(IoDevice),
            Accumulator;
        
        {error, Reason} ->
            file:close(IoDevice),
            throw({error, Reason});
        
        Line ->
            NewLineNumber = CurrentLineNumber + 1,
            NewAccumulator = [{Line, NewLineNumber}|Accumulator],
            append_file_contents_with_line_no(IoDevice, NewLineNumber,
                                              NewAccumulator)
    end.


%% @doc Gets contents of given (opened) file and annotates each line with it's
%%     number in the file. Results in a list of consecutive lines, annotated
%%     with line number.
%% @spec (io_device() -> [{string(), int()}]
%% @throws {error, atom()} thrown on I/O errors.
get_file_contents_as_lines_with_line_no(IoDevice) ->
    CurrentLineNumber = 0,
    Accumulator = [],
    append_file_contents_with_line_no(IoDevice, CurrentLineNumber, Accumulator).

