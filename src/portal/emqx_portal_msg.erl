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

-module(emqx_portal_msg).

-export([ to_binary/1
        , from_binary/1
        , from_dispatch_msg/1
        , apply_mountpoint/2
        ]).

-export_type([msg/0]).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").

-opaque msg() :: #{retain := boolean(),
                   topic := emqx_types:topic(),
                   payload := binary()
                  }.

%% @doc Transform dispatched message into `msg()'
-spec from_dispatch_msg(#message{}) -> msg().
from_dispatch_msg(#message{topic = Topic,
                           payload = Payload,
                           flags = #{retain := Retain}
                          }) ->
    #{retain => Retain,
      topic => Topic,
      payload => Payload
     }.

%% @doc Mount topic to a prefix.
apply_mountpoint(#{topic := Topic} = Msg, Mountpoint) ->
    Msg#{topic := topic(Mountpoint, Topic)}.

%% @doc Make `binary()' in order to make iodata to be persisted on disk.
-spec to_binary(msg()) -> binary().
to_binary(#{retain := Retain,
            topic := Topic,
            payload := Payload
           }) ->
    <<(bool_int(Retain)):8, (size(Topic)):16, Topic/binary, Payload/binary>>.

%% @doc Unmarshal binary into `msg()'.
-spec from_binary(binary()) -> msg().
from_binary(<<Retain:8, L:16, Topic:L/binary, Payload/binary>>) ->
    #{retain => Retain =:= 1,
      topic => Topic,
      payload => Payload
     }.

topic(Prefix, Topic) -> emqx_topic:prepend(Prefix, Topic).

bool_int(1) -> 1;
bool_int(true) -> 1;
bool_int(_) -> 0.

