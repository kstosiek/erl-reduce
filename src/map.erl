%% Author: Karol Stosiek (karol.stosiek@gmail.com)
%% Created: 28-11-2010
%% Description: Module containing example implementation of map operation.
%%    Distributed Grep: The map function emits a line if it matches a given
%%    pattern. The reduce function is an identity function that just copies
%%    the supplied intermediate data to the output.
-module(map).

%%
%% Include files
%%

%%
%% Exported Functions
%%
-export([map/1, map/3, main/1, 
         test_slice_by_length/0]).


%%
slice_aux([],_) -> 
    [];

slice_aux(XS, N) when length(XS) < N -> 
    [XS];

slice_aux(XS, N) ->
    {A, B} = lists:split(N, XS),
    lists:append([A], slice_aux(B, N)).

%% Slices list of elements into list of N-lists of elements. N-list is a list of length N.
slice_by_length([], _) -> [[]];

slice_by_length(XS, N) ->
    slice_aux(XS, N).


%%  @doc TODO
%%  @ spec Input - List of input elements, like [1, 2, 3, 4, 5]
%%         Mapfun - TODO
%%         M - well-known constant. Number of mapping processes.
map([], _, _) -> [];

map(Input, _, M) ->
    Parts = slice_by_length(Input, length(Input) div M),
    dist:start(Parts),
    ok.


%% Start point (required when using escript command).
main(_) ->
    test(fun map:test_slice_by_length/0, "test_slice_by_length"),
    io:format("~n"),
    %
    M = 2,
    Input = lists:seq(1,10), 
    MapFunction = fun() -> ok end,    % not used now
    map(Input, MapFunction, M),
    ok.

test_slice_by_length() ->
    [
     slice_by_length([], 3) == [[]],
     slice_by_length([1,2], 3) == [[1,2]],
     slice_by_length([1,2,3,4,5,6], 3) == [[1,2,3], [4,5,6]],
     slice_by_length([1,2,3,4,5,6,7], 3) == [[1,2,3], [4,5,6], [7]]
    ].


%%  @ doc Testing function. 
%%  @ spec TestFunction -- function to be tested
%%         TestName -- name of the test (displayed while testing)
test(TestFunction, TestName) ->
    io:fwrite("Testing ~s ", [TestName]),
    Result = TestFunction(),
    case lists:all(fun(P)->P end, Result) of
        true -> io:fwrite("\t\t\t\t OK~n");
        _    -> io:fwrite("\t\t\t\t FAILURE ~w~n", [Result])
    end,
    ok.


%%  @doc Maps given input into intermediate data.
%%  @spec ([{K1,V1}]) -> [{K2, V2}]
%% depracated?
map(Input) ->
	lists:flatten(local_map(Input)).

%%
%% Local Functions
%%

%%  @doc Takes a list of {key, value} pairs and produces a list
%%      of {key', value'} lists. The inner lists represent mapping result
%%      for a single input {key, value} pair. 
%%  @spec ({K1,V1}) -> [[{K2, V2}]]
%%  @private
local_map([]) ->
	[];
local_map([Head|Tail]) ->
	[process(Head)| map(Tail)].

%%  @doc Takes a single {key, value} pair and produces a list of {key', value'}
%%      pairs to process by the reduce operation. In current implementation
%%      it splits the Key string into separate words and assigns 1 to each
%%      of them.
%%  @spec ({K1,V1}) -> [{K2, V2}]
%%  @private
process({Key, _}) ->
	  lists:filter(fun({Token, _})-> string:str(Token, "kitty") /= 0 end,
		lists:map(fun(Token)-> {Token, 0} end,
				string:tokens(Key, "\n"))).
