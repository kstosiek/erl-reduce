-module(conf).
-compile(export_all).

%% Work time overhead. Used to make mapper work longer.
min_map_wait() -> 500.
max_map_wait() -> 1500.

%% Maximum number of mapping and reducing processes.
max_M() -> 2.
max_R() -> 2.
