-module(master).
-export([main/0, main/1,
         test_slice_by_length/0]).


%% Aux function for slice_by_length
slice_aux([], _) -> [];
slice_aux(XS, N) when length(XS) < N -> [XS];
slice_aux(XS, N) ->
    {A, B} = lists:split(N, XS),
    lists:append([A], slice_aux(B, N)).

%% Slices list of elements into list of N-lists of elements. N-list is a list of length N.
%% At most one element of result can be shorter than N.
slice_by_length([], _) -> [[]];
slice_by_length(XS, N) ->
    slice_aux(XS, N).


%% Spawns a slave node
spawn_slave(SlaveName, Data) ->
    io:format("Spawning slave: ~s~n", [SlaveName]),
    Pid = spawn(SlaveName, slavenode, run, []),
    Pid ! {self(), {'mapdata', Data}},
    ok.


%% Event loop.
%% Argument means how many messages received. It is used to stop loop after a number 
%% of events.
%% Magic constant describes how many messages we will wait for. Used just not to kill
%% master every single time.
handle_events(2) -> ok;
handle_events(N) ->
    receive
        Msg ->
            io:format('~w~n',[Msg]),
            handle_events(N + 1)
    end,
    ok.


%% Spawns slaves.
spawn_slaves(SlavesWithData) ->
    Spawner = fun({SlaveName, DataForSlave}) -> spawn_slave(SlaveName, DataForSlave) end,
    io:format('~w~n', [SlavesWithData]),
    lists:foreach(Spawner, SlavesWithData),
    ok.


%%  @doc TODO
%%  @ spec Input - List of pairs to be mapped.
%%         M - Number of mapping processes to be used.
master([], _) -> [];

master(Input, M) ->
    Data = slice_by_length(Input, length(Input) div M),
    % We need to assure that lists are the same length (zip's requirement)
    Slaves = conf:slaves_names(),
    io:format("START Data: ~w~n", [Data]),
    io:format("Lengths (will take smaller): ~B ~B~n", [length(Slaves), length(Data)]),
    CommonLength = erlang:min(length(Slaves), length(lists:nth(1, Data))),
    CutSlaves = lists:sublist(Slaves, CommonLength),
    CutData = lists:sublist(Data, CommonLength),
    %
    SlavesWithData = lists:zip(CutSlaves, CutData),
    spawn_slaves(SlavesWithData),
    handle_events(0),
    ok.


%% Start point. Use this to run supervisor (aka master).
%% 1-arg-version is used when running with escript.
%% 0-arg-version is present in order not to confuse user (argument is ignored).
main() -> main([]).
main(_) ->
    test(fun master:test_slice_by_length/0, "test_slice_by_length"),
    io:format("~n"),
    % Prepare data in order to run master
    M = conf:max_M(),
    Input = [{X, X} || X <- lists:seq(1, 10)],
    master(Input, M),
    ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%  TESTING  %%%%%%%%%%%%%%%%%%%%%%%%%

% Should be moved to test dir, but I don't know how to do it correctly now
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
