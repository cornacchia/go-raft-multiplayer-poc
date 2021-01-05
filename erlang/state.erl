-module(state).

-export([start_udp/1, state_start/1, state_proc_loop/1]).

start_udp (StatePort) ->
  spawn(fun() -> state_udp(self(), StatePort) end).

state_start(State) ->
  spawn(?MODULE, state_proc_loop, [State]).

state_proc_loop (State) ->
  receive
    {get, Pid} ->
      Pid ! {state, State},
      state_proc_loop(State) ;
    {store, Message} ->
      io:format("State loop STORE\n"),
      State = store_message(State, Message),
      state_proc_loop(State)
  end.

state_udp (StatePid, StatePort) ->
  {ok, Socket} = gen_udp:open(StatePort, [binary, {active, true}]),
  io:format("State module opened socket: ~p~n", [Socket]),
  ui_state_loop(Socket, StatePid).

ui_state_loop(Socket, StatePid) ->
  inet:setopts(Socket, [{active, once}]),
  receive
    {udp, Socket, Host, Port, Bin} ->
      Message = binary_to_list(Bin),
      io:format("State UDP received: "),
      io:format(Message),
      io:format("\n"),
      StatePid ! {get, self()},
      receive
        {state, State} ->
          io:format("UDP State received state from loop\n"),
          gen_udp:send(Socket, Host, Port, <<State>>)
      end,
      ui_state_loop(Socket, StatePid)
  end.

store_message(State, Message) -> State.