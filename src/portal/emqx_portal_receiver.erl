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

%% @doc This module implements the receiver of message batches for portal
%% between EMQX nodes/clusters.
%%
%% A portal receiver is a gen_statem process which receives messages from
%% remote peer nodes.
%%
%% A portal receiver should work in a pool in order to balance the load.
%% In order to ensure per-topic message order, the caller should avoid
%% mapping messages from the same sender ( @see emqx_portal_sender ) to
%% different receivers in the pool.

-module(emqx_portal_receiver).
-include("emqx.hrl").

