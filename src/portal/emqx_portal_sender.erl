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

%% @doc This module implements the sender of message batches for portal
%% between EMQX nodes/clusters.
%%
%% A portal sender is a gen_statem process which collects local node/cluster
%% messages into batches and sends over to portal receivers working in the
%% receiver node.
%%
%% A portal sender may subscribe to multiple topics (including wildcard topics)
%% in local node/cluster. However, portal workers are not designed to support
%% automatic load-balancing, i.e. in case it can not keep up with the amount of
%% messages comming in, administrator should split and balance topics between
%% sender workers.

-module(emqx_portal_sender).
-behaviour(gen_statem).

%% gen_statem callbacks
-export([terminate/3, code_change/4, init/1, callback_mode/0]).

%% state functions
-export([running/3]).

-include("emqx.hrl").

-define(DEFAULT_BATCH_COUNT, 100).
-define(DEFAULT_BATCH_BYTES, 1 bsl 20).
-define(DEFAULT_SEND_AHEAD, 8).

callback_mode() -> state_functions.

%% @doc Options should be a map()
init(Options) ->
    erlang:process_flag(trap_exit, true),
    Receiver = maps:get(receiver_node, Options),
    Get = fun(K, D) -> maps:get(K, Options, D) end,
    QueueConfig = Get(queue_config, #{mem_only => true}),
    Queue = replayq:open(QueueConfig),
    {ok, running, #{receiver_node => Receiver,
                    replayq => Queue,
                    batch_bytes_limit => Get(batch_bytes_limit, ?DEFAULT_BATCH_BYTES),
                    batch_count_limit => Get(batch_count_limit, ?DEFAULT_BATCH_COUNT),
                    max_inflight_batches => Get(max_inflight_batches, ?DEFAULT_SEND_AHEAD),
                    inflight => []
                    }}.

code_change(_Vsn, State, Data, _Extra) ->
    {ok, State, Data}.

terminate(_Reason, _State, #{replayq := Q}) ->
    _ = replayq:close(Q),
    ok.

