%% Author: Piotr Polesiuk (bassists@o2.pl),
%% Created: 16-01-2011
%% Description: Monitors map workers and reduce workers. Informs master
%%     when some of them fails.
-module(monitors).

%%
%% Exported Functions.
-export([monitor_map_workers/2,
		 monitor_reduce_workers/2
        ]).


%%
%% API Functions.
%%

%% @doc Monitors map workers. When some of workers fails, sends 
%%     {WorkerPid, map_worker_down} to master.
%%     to have at least one map and one reduce worker pid available.
%% @spec (MasterPid, MapWorkerPids) -> () where
%%    MasterPid = pid(),
%%    MapWorderPids = [pid()]
monitor_map_workers(MasterPid, MapWorkerPids) ->
	create_monitors(MapWorkerPids),	
	monitor_workers(MasterPid, map_worker_down, MapWorkerPids).

%% @doc Monitors reduce workers. When some of workers fails, sends 
%%     {WorkerPid, reduce_worker_down} to master.
%%     to have at least one map and one reduce worker pid available.
%% @spec (MasterPid, ReduceWorkerPids) -> () where
%%    MasterPid = pid(),
%%    ReduceWorderPids = [pid()]
monitor_reduce_workers(MasterPid, ReduceWorkerPids) ->
	create_monitors(ReduceWorkerPids),	
	monitor_workers(MasterPid, reduce_worker_down, ReduceWorkerPids).

%%
%% Local Functions.
%%

%% @doc Create monitors to specified workers.
%% @spec (WorkerPids) -> () where
%%    WorkerPids = [pid()]
create_monitors(WorkerPids) ->
	lists:foreach(fun(Pid) ->
						  erlang:monitor(process, Pid)
				  end,
				  WorkerPids).

%% @doc Receives monitor messages, and informs master when some of
%%    workers fails, by sendind {WorkerPid, Message} message.
%% @spec (MasterPid, Message, WorkerPids) where
%%    MasterPid = pid(),
%%    Message = atom(),
%%    WorkerPids = [pid()]
monitor_workers(_, _, []) ->
	ok;
monitor_workers(MasterPid, Message, WorkerPids) ->
	receive
		{'DOWN', _, process, WorkerPid, normal} ->
			NewWorkerPids = lists:delete(WorkerPid, WorkerPids),
			monitor_workers(MasterPid, Message, NewWorkerPids);
		
		{'DOWN', _, process, WorkerPid, _} ->
			MasterPid ! {WorkerPid, Message},
			NewWorkerPids = lists:delete(WorkerPid, WorkerPids),
			monitor_workers(MasterPid, Message, NewWorkerPids)
	end.
