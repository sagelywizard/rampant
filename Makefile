compile:
	rebar3 compile

run: compile
	erl -pa ./_build/default/lib/rampant/ebin -s rampant

node:
	erl -sname $(SNAME) -pa ./_build/default/lib/rampant/ebin

check:
	rebar3 ct
	rebar3 dialyzer
