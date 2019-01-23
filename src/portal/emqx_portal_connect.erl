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

-module(emqx_portal_connect).

-export_type([config/0, connection/0]).

-optional_callbacks([]).

-type config() :: map().
-type connection() :: term().
-type conn_ref() :: reference().
-type batch() :: emqx_protal:batch().
-type batch_ref() :: emqx_portal:batch_ref().

%% establish the connection to remote node/cluster
%% protal worker (the caller process) should be expecting
%% a message {disconnected, conn_ref()} when disconnected.
-callback start(config()) -> {ok, conn_ref(), connect()} | {error, any()}.

%% publish to remote node/cluster
%% portal worker (the caller process) should be expecting
%% a message {batch_ack, batch_ref()} when batch is acknowledged by remote node/cluster
-callback publish(connection(), batch_ref(), batch()) -> ok | {error, any()}.

%% called when owner is shutting down.
-callback stop(connect()) -> ok.

