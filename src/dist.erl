-module(dist).
-export([start/1, client/0, mapping/1]).


%% Returns list of slave-nodes (mapping processes?)
slaves() ->
    lists:sublist(['bb@localhost', 'cc@localhost'], conf:max_M()).


%% Spawns a slave
spawn_slave(SlaveName, Data) ->
    io:format("Spawning slave: ~s~n", [SlaveName]),
    %% Pid = spawn(SlaveName,dist, client, []),
    %% Pid ! {self(), {'taskdata', Data}},
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
