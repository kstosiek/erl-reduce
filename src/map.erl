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

%%  @doc Context passed to map function
%%  @spec m - num of mapping processors
%%        mapfun - mapping function
%% -record(map_context, {m, mapfun}).

%%
%% Exported Functions
%%
-export([map/1, map/3, main/1]).


%%
slice_aux([],_) -> 
    [];

slice_aux(XS, N) when length(XS) < N -> 
    [XS];

slice_aux(XS, N) ->
    {A, B} = lists:split(N, XS),
    lists:append([A], slice_aux(B, N)).

%%
slice_by_length([], _) -> [[]];

slice_by_length(XS, N) ->
    slice_aux(XS, N).


%%  @doc TODO
%%  @ spec Input - TODO
%%         Mapfun - TODO
%%         M - well-known constant. Number of mapping processors.
map(Input, Mapfun, M) ->
    [].


run_tests() ->
    [
     slice_by_length([], 3) == [[]],
     slice_by_length([1,2], 3) == [[1,2]],
     slice_by_length([1,2,3,4,5,6], 3) == [[1,2,3], [4,5,6]],
     slice_by_length([1,2,3,4,5,6,7], 3) == [[1,2,3], [4,5,6], [7]]
    ].

main(_) ->
    X = run_tests(),
    case lists:all(fun(B)->B==true end, X) of
        true -> io:fwrite("tests ok~n");
        _    -> io:fwrite("FAILURE: tests ~w~n", [X])
    end,
    io:format("~n").


%%  @doc Maps given input into intermediate data.
%%  @spec ([{K1,V1}]) -> [{K2, V2}]
%% depracated
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
