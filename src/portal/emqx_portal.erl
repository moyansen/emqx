%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

%% @doc This module implements the worker of message batches for portal
%% between EMQX nodes/clusters.
%%
%% A portal worker is a gen_statem process which collects local node/cluster
%% messages into batches and sends over to portal receivers working in the
%% receiver node.
%%
%% A portal worker may subscribe to multiple topics (including wildcard topics)
%% in local node/cluster. However, portal workers are not designed to support
%% automatic load-balancing, i.e. in case it can not keep up with the amount of
%% messages comming in, administrator should split and balance topics between
%% workers.
%%
%% State Diagram
%%
%% [connecting] --(2)--> [connected]
%%  |        ^                 |
%%  |        |                 |
%%  '--(1)---'--------(3)------'
%%
%% (1): timeout
%% (2): successfuly connected to remote node/cluster
%% (3): received {disconnected, conn_ref()} OR
%%      failed to publish to remote node/cluster.

-module(emqx_portal).
-behaviour(gen_statem).

%% APIs
-export([start_link/2]).

%% gen_statem callbacks
-export([terminate/3, code_change/4, init/1, callback_mode/0]).

%% state functions
-export([connecting/3, connected/3]).

-export_type([batch/0,
              batch_ref/0
             ]).

-type batch() :: [emqx_types:message()].
-type batch_ref() :: replayq:ack_ref().

-include("logger.hrl").
-include("emqx_mqtt.hrl").

-define(DEFAULT_BATCH_COUNT, 100).
-define(DEFAULT_BATCH_BYTES, 1 bsl 20).
-define(DEFAULT_SEND_AHEAD, 8).
-define(DEFAULT_RECONNECT_DELAY_MS, timer:seconds(5)).
-define(DEFAULT_SEG_BYTES, (1 bsl 20)).
-define(keep_sending, {next_event, internal, try_publish}).

%% @doc Start a portal worker. Supported options:
%% connect_module: The module which implements emqx_portal_connect behaviour
%%      and work as message batch transport layer
%% connect_config: Config for eqmx_portal_connect, transparent to this module
%% reconnect_delay_ms: Delay in milli-seconds for the portal worker to retry
%%      in case of transportation failure.
%% batch_bytes_limit: Max number of bytes to collect in a batch for each
%%      publish call towards emqx_portal_connect
%% batch_count_limit: Max number of messages to collect in a batch for
%%      each publish call towards eqmx_portal_connect
%% max_inflight_batches: Max number of batches allowed to send-ahead before
%%      receiving confirmation from remote node/cluster
%% mountpoint: The topic mount point for messages sent to remote node/cluster
%%      'default' or <<>> to indicate none
%% forwards: Local topics to subscribe.
%% replayq_dir: Directory where replayq should persist messages
%% replayq_seg_bytes: Size in bytes for each replqyq segnment file
start_link(Name, Options) ->
    gen_statem:start_link({local, Name}, ?MODULE, Options).

callback_mode() -> [state_functions, state_enter].

%% @doc Options should be a map().
init(Options) ->
    erlang:process_flag(trap_exit, true),
    Get = fun(K, D) -> maps:get(K, Options, D) end,
    QueueConfig =
        case Get(replayq_dir, undefned) of
            undefined -> #{mem_only => true};
            Dir -> #{dir => Dir,
                     seg_bytes => Get(replayq_seg_bytes, ?DEFAULT_SEG_BYTES)
                    }
        end,
    Queue = replayq:open(QueueConfig#{marshaller => fun msg_marshaller/1}),
    Topics = Get(forwards, []),
    ok = subscribe_local_topics(Topics),
    {ok, connecting,
     #{connect_module => maps:get(connect_module, Options),
       connect_config => maps:get(connect_config, Options),
       reconnect_delay_ms => maps:get(reconnect_delay_ms, Options, ?DEFAULT_RECONNECT_DELAY_MS),
       batch_bytes_limit => Get(batch_bytes_limit, ?DEFAULT_BATCH_BYTES),
       batch_count_limit => Get(batch_count_limit, ?DEFAULT_BATCH_COUNT),
       max_inflight_batches => Get(max_inflight_batches, ?DEFAULT_SEND_AHEAD),
       mountpoint => format_mountpoint(Get(mountpoint, undefined)),
       topics => Topics,
       replayq => Queue,
       inflight => []
      }}.

code_change(_Vsn, State, Data, _Extra) ->
    {ok, State, Data}.

terminate(_Reason, _State, #{replayq := Q}) ->
    _ = replayq:close(Q),
    ok.

%% @doc Connecting state is a state with timeout.
%% After each timeout, it re-enters this state and start a retry until
%% successfuly connected to remote node/cluster.
connecting(enter, _OldState, #{connect_module := ConnMod,
                               connect_config := ConnCfg,
                               reconnect_delay_ms := Timeout
                              } = State) ->
    case ConnMod:start(ConnCfg) of
        {ok, ConnRef, Conn} ->
            {next_state, connected, State#{conn_ref => ConnRef,
                                           connection => Conn
                                          }};
        {error, Reason} ->
            ?ERROR("Failed to connect with module=~p config=~p\nreason:~p",
                   [ConnMod, ConnCfg, Reason]),
            Action = {state_timeout, Timeout, reconnect},
            {keep_state, State, Action}
    end;
connecting(state_timeout, reconnect, State) ->
    %% enter connecting state again to start state timer
    {next_state, connecting, State};
connecting(Type, Content, State) ->
    common(connecting, Type, Content, State).

%% @doc Send batches to remote node/cluster when in 'connected' state.
connected(enter, _OldState, #{inflight := Inflight} = State) ->
    case retry_inflight(State#{inflight := []}, Inflight) of
        {ok, NewState} ->
            {keep_state, NewState, ?keep_sending};
        {error, NewState} ->
            {next_state, connecting, disconnect(NewState)}
    end;
connected(internal, try_publish, State) ->
    case pop_and_send(State) of
        {ok, NewState} ->
            {keep_state, NewState};
        {error, NewState} ->
            {next_state, connecting, disconnect(NewState)}
    end;
connected(info, {disconnected, ConnRef}, #{conn_ref := ConnRefNow} = State) ->
    case ConnRef =:= ConnRefNow of
        true ->
            {keep_state,
             State#{conn_ref := undefined,
                    conn := undefined
                   }};
        _ ->
            ?INFO("Stale disconnect info ~p discarded", [ConnRef]),
            {keep_state, State}
    end;
connected(Type, Content, State) ->
    common(connected, Type, Content, State).

%% Common handlers
common(_StateName, info, {dispatch, _, Msg},
       #{replayq := Q} = State) ->
    NewQ = replayq:append(Q, [transform(M) || M <- collect([Msg])]),
    {keep_state, State#{replayq => NewQ}, ?keep_sending};
common(StateName, Type, Content, State) ->
    ?INFO("Ignored unknown ~p event ~p at state ~p", [Type, Content, StateName]),
    {keep_state, State}.

collect(Acc) ->
    receive
        {dispatch, _, Msg} ->
            collect([Msg | Acc])
    after
        0 ->
            lists:reverse(Acc)
    end.

%% Retry all inflight (previously sent but not acked) batches.
retry_inflight(State, []) -> {ok, State};
retry_inflight(#{inflight := Inflight} = State,
               [#{q_ack_ref := QAckRef, batch := Batch} | T] = Remain) ->
    case do_publish(State, QAckRef, Batch) of
        {ok, NewState} ->
            retry_inflight(NewState, T);
        {error, Reason} ->
            ?ERROR("Inflight retry failed\n~p", [Reason]),
            {error, State#{inflight := Inflight ++ Remain}}
    end.

pop_and_send(#{inflight := Inflight,
               max_inflight_batches := Max
              } = State) when length(Inflight) >= Max ->
    {ok, State};
pop_and_send(#{replayq := Q,
               batch_count_limit := CountLimit,
               batch_bytes_limit := BytesLimit
              } = State) ->
    case replayq:is_empty(Q) of
        true ->
            {ok, State};
        false ->
            Opts = #{count_limit => CountLimit, bytes_limit => BytesLimit},
            {Q1, QAckRef, Batch} = replayq:pop(Q, Opts),
            do_publish(State#{replayq := Q1}, QAckRef, Batch)
    end.

%% Assert non-empty batch because we have a is_empty check earlier.
do_publish(State = #{inflight := Inflight}, QAckRef, [_ | _] = Batch) ->
    case try_publish(State, Batch) of
        {ok, Ref} ->
            NewInflight = Inflight ++ [#{q_ack_ref => QAckRef,
                                         publish_ack_ref => Ref,
                                         batch => Batch
                                        }],
            {ok, State#{inflight := NewInflight}};
        {error, Reason} ->
            ?INFO("Batch produce failed\n~p", [Reason]),
            {error, State}
    end.

subscribe_local_topics(Topics) ->
    lists:froeach(
      fun(Topic0) ->
              Topic = iolist_to_binary(Topic0),
              emqx_topic:validate({filter, Topic}) orelse erlang:error({bad_topic, Topic}),
              emqx_broker:subscribe(Topic, #{qos => ?QOS_1, subid => name()})
      end, Topics).

name() -> {_, Name} = process_info(self(), registered_name), Name.

disconnect(#{connection := Conn,
             connect_module := Module
            } = State) ->
    case Conn =:= undefined of
        true ->
            ok = Module:stop(Conn),
            State#{conn_ref => undefined,
                   connection => udnefined
                  };
        false ->
            State
    end.

transform(Msg) -> emqx_portal_msg:from_dispatch_msg(Msg).

%% Called only when replayq needs to dump it to disk.
msg_marshaller(Bin) when is_binary(Bin) -> emqx_portal_msg:from_binary(Bin);
msg_marshaller(Msg) -> emqx_portal_msg:to_binary(Msg).

%% Return ok or {error, Reason}
try_publish(#{connect_module := Module,
              connection := Connection,
              mountpoint := Mountpoint
             }, Batch) ->
    Module:publish(Connection, [emqx_portal_msg:apply_mountpoint(M, Mountpoint) || M <- Batch]).

format_mountpoint(undefined) ->
    undefined;
format_mountpoint(Prefix) ->
    binary:replace(iolist_to_binary(Prefix), <<"${node}">>, atom_to_binary(node(), utf8)).

