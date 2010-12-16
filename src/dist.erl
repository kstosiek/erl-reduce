-module(dist).
-export([start/1, client/0, mapping/1]).

%% Returns list of slave-nodes (mapping processes?)
slaves() ->
    ['bb@localhost', 'cc@localhost'].
    %% ['bb@localhost'].


%% Runs one slave
spawn_slave(SlaveName, Data) ->
    io:format("Spawning slave: ~s~n", [SlaveName]),
    Pid = spawn(SlaveName,dist, client, []),
    Pid ! {self(), {'taskdata', Data}},
    receive
        Msg ->
            io:format('~w~n',[Msg])
    end.


%% Supervisor starts here
start(Data) -> 
    % We need to assure lists are the same length (zip's requirement)
    Slaves = slaves(),
    io:format("START Data: ~w~n",[Data]),
    io:format("~B ~B~n", [length(Slaves), length(lists:nth(1, Data))]),
    MinLength = erlang:min(length(Slaves), length(lists:nth(1, Data))),
    CutSlaves = lists:sublist(Slaves, MinLength),
    CutData = lists:sublist(Data, MinLength),
    %
    SlavesWithData = lists:zip(CutSlaves, CutData),
    lists:foreach(fun ({A,B}) -> spawn_slave(A,B) end, SlavesWithData),
    ok.


%% Transforms list of elements info list of pairs.
mapping(XS) ->
    lists:map(fun(X) -> {X, X*X} end, XS).


%% Code executed by client (slave node).
client() ->
    receive
        {Pid, {'taskdata', XS}} ->
            First = lists:last(lists:reverse(XS)),
            Last = lists:last(XS),
            io:format(user, "Received data: ~w -- ~w~n", [First, Last]),
            Pid ! mapping(XS)
    end.
