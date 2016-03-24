-record(partition, {
    name,
    node,
    start_key,
    end_key
}).

-type start_range() :: integer().
-type end_range() :: integer().
-type database_name() :: binary().
-type partition() :: #partition{}.
-type timestamp() :: {integer(), integer()}.
-type partition_map() :: [partition()].
-type proplist() :: [{atom(), any()}].
-type kv() :: {binary(), binary()}.
