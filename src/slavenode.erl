-module(slavenode).
-export([run/0]).


%% Transforms list of elements info list of pairs.
%% XS is a list of pairs.
%% Result is list of pairs.
mapping(XS) ->
    SleepTime = erlang:max(conf:min_map_wait(), random:uniform(conf:max_map_wait())),
    timer:sleep(SleepTime),                    % Z, z, z, ...
    lists:map(fun({X,_}) -> {X, X*X} end, XS).


%% Code executed by slave node.
run() ->
    receive
        {Pid, {'mapdata', XS}} ->
            %% First = lists:last(lists:reverse(XS)),
            %% Last = lists:last(XS),
            %% io:format(user, "Received data: ~w -- ~w~n", [First, Last]),
            Pid ! mapping(XS),        % Send back to master
            io:format(user, "Result was sent back~n", [])
    end.

