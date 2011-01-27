%% Author: Marcin Milewski (mmilewski@gmail.com),
%% Created: 25-01-2011
%% Description: 
-module(logger).

%%
%% Exported Functions.
-export([ info_msg/1,
          info_msg/2,
          warning_msg/1,
          warning_msg/2,
          error_msg/1,
          error_msg/2
         ]).

info_msg(Message) ->
    error_logger:info_msg(Message),
    wait_a_bit().

info_msg(Message, Args) ->
    error_logger:info_msg(Message, Args),
    wait_a_bit().

warning_msg(Message) ->
    error_logger:warning_msg(Message),
    wait_a_bit().

warning_msg(Message, Args) ->
    error_logger:warning_msg(Message, Args),
    wait_a_bit().

error_msg(Message) ->
    error_logger:error_msg(Message),
    wait_a_bit().

error_msg(Message, Args) ->
    error_logger:error_msg(Message, Args),
    wait_a_bit().

wait_a_bit() ->
    timer:sleep(30).

