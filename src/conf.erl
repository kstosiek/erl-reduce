%% Author: Marcin Milewski (mmilewski@gmail.com)
%% Created: 17-12-2010
%% Description: Contains configuration values.
-module(conf).
-compile(export_all).


%% Work time overhead. Used to make mapper work longer.
%% Values are given in miliseconds.
min_map_wait() -> 500.
max_map_wait() -> 1500.

%% Maximum number of mapping and reducing processes.
max_M() -> 2.
max_R() -> 2.

%% List of all possible node names. In future we would probably like
%% to achieve this list at runtime -- autodiscover.
slaves_names() ->
    Names = ['bb@localhost', 'cc@localhost', 'dd@localhost'],
    lists:sublist(Names, conf:max_M()).
