rampant
=====

A [RAMP](http://www.bailis.org/papers/ramp-sigmod2014.pdf) implementation in Erlang.

Build
-----

    $ make compile

Test
----

    $ make check

API
---

```erlang
1> rampant:create("namespace").
ok
2> {ok, Client} = rampant:client("namespace").
{ok,<0.61.0>}
3> rampant:write(Client, [{<<"k1">>, <<"v1">>}, {<<"k2">>, <<"v2">>}]).
ok
4> rampant:read(Client, [<<"k1">>, <<"k2">>]).
{ok,[{<<"k2">>,{<<"v2">>,{1905181425,1}}},
     {<<"k1">>,{<<"v1">>,{1905181425,1}}}]}
```

Notes
-----

- rampant currently only supports RAMP-f
- rampant is currently only backed by ets
- partition map handling is currently handled by in-memory mnesia
