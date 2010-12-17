-module(supervisor).
-export([start/1, client/0, mapping/1, supervisor/3, main/1, 
         test_slice_by_length/0]).


%% Returns list of slave-nodes (mapping processes?)
slaves() ->
    lists:sublist(['bb@localhost', 'cc@localhost'], conf:max_M()).


%% Spawns a slave
spawn_slave(SlaveName, Data) ->
    io:format("Spawning slave: ~s~n", [SlaveName]),
    %% Pid = spawn(SlaveName,dist, client, []),
    %% Pid ! {self(), {'taskdata', Data}},
    ok.


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


%%  @doc TODO
%%  @ spec Input - List of input elements, like [1, 2, 3, 4, 5]
%%         Mapfun - TODO
%%         M - well-known constant. Number of mapping processes.
supervisor([], _, _) -> [];

supervisor(Input, _, M) ->
    Parts = slice_by_length(Input, length(Input) div M),
    dist:start(Parts),
    ok.


%% Start point (required when using escript command).
main(_) ->
    test(fun map:test_slice_by_length/0, "test_slice_by_length"),
    io:format("~n"),
    %
    M = conf:max_M(),
    Input = lists:seq(1, 10), 
    MapFunction = fun() -> ok end,    % not used now
    supervisor(Input, MapFunction, M),
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


%% Argument means how many messages received.
receive_messages(2) ->
    ok;

receive_messages(N) ->
    receive
        Msg ->
            io:format('~w~n',[Msg]),
            receive_messages(N + 1)
    end,
    ok.


%% Spawns slaves and waits for message.
spawn_slaves(SlavesWithData) ->
    Spawner = fun({S,D}) -> spawn_slave(S,D) end,
    io:format('~w~n', [SlavesWithData]),
    lists:foreach(Spawner, SlavesWithData),
    ok.


%% Supervisor starts here
start(Data) -> 
    % We need to assure that lists are the same length (zip's requirement)
    Slaves = slaves(),
    io:format("START Data: ~w~n", [Data]),
    io:format("Lengths (will take smaller): ~B ~B~n", [length(Slaves), length(Data)]),
    CommonLength = erlang:min(length(Slaves), length(lists:nth(1, Data))),
    CutSlaves = lists:sublist(Slaves, CommonLength),
    CutData = lists:sublist(Data, CommonLength),
    %
    SlavesWithData = lists:zip(CutSlaves, CutData),
    spawn_slaves(SlavesWithData),
    receive_messages(0),
    ok.


%% Transforms list of elements info list of pairs.
mapping(XS) ->
    SleepTime = erlang:max(conf:min_map_wait(), random:uniform(conf:max_map_wait())),
    timer:sleep(SleepTime),
    lists:map(fun(X) -> {X, X*X} end, XS).


%% Code executed by client (slave node).
client() ->
    receive
        {Pid, {'taskdata', XS}} ->
            First = lists:last(lists:reverse(XS)),
            Last = lists:last(XS),
            io:format(user, "Received data: ~w -- ~w~n", [First, Last]),
            Pid ! mapping(XS),
            io:format(user, "Result was sent~n", [])
    end.
