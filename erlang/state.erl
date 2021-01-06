-module(state).

-export([state_start/2, state_proc_loop/2, state_udp/2]).

state_start(State, StatePort) ->
  io:format("State module running with PID: ~p~n", [self()]),
  StatePid = spawn(?MODULE, state_proc_loop, [State, erlang:monotonic_time(millisecond)]),
  spawn(?MODULE, state_udp, [StatePid, StatePort]),
  StatePid.

state_proc_loop (State, LastTs) ->
  receive
    {get, Pid} ->
     NewTs = erlang:monotonic_time(millisecond),
      Pid ! {state, State},
      state_proc_loop(State, NewTs) ;
    {store, Message} ->
      NewTs = erlang:monotonic_time(millisecond),
      Delta = float(NewTs) - float(LastTs),
      %io:format("State loop STORE\n"),
      [Players, Map] = State,
      NewPlayers = store_message(Players, Message, Delta),
      state_proc_loop([NewPlayers, Map], NewTs)
  end.

state_udp (StatePid, StatePort) ->
  {ok, Socket} = gen_udp:open(StatePort, [binary, {active, true}]),
  io:format("State module opened socket: ~p~n", [Socket]),
  ui_state_loop(Socket, StatePid).

send_map(Socket, Host, Port, Map) ->
  gen_udp:send(Socket, Host, Port, <<"Map\n">>),
  gen_udp:send(Socket, Host, Port, list_to_binary(Map ++ "\n")),
  gen_udp:send(Socket, Host, Port, <<"Map_End\n">>)
.

send_player_data(Socket, Host, Port, Data) ->
  PlayerData = [float_to_list(I) ++ "|" || I <- Data] ++ "\n",
  gen_udp:send(Socket, Host, Port, list_to_binary(PlayerData)).

send_players(Socket, Host, Port, Players) ->
  gen_udp:send(Socket, Host, Port, <<"Player\n">>),
  lists:map(fun (Data) -> send_player_data(Socket, Host, Port, Data) end, Players),
  gen_udp:send(Socket, Host, Port, <<"Player_End\n">>)
.

ui_state_loop(Socket, StatePid) ->
  inet:setopts(Socket, [{active, once}]),
  receive
    {udp, Socket, Host, Port, Bin} ->
      Message = binary_to_list(Bin),
      StatePid ! {get, self()},
      receive
        {state, State} ->
          [Players | Map] = State,
          if Message =:= "0" ->
            send_map(Socket, Host, Port, Map);
          true -> send_players(Socket, Host, Port, Players)
        end
      end,
      ui_state_loop(Socket, StatePid)
  end.

store_message(Players, Message, Delta1) ->
  io:format("Message received: ~p~p~n", [Message, Delta1]),
  Delta = 0.2,
  Speed = 5,
  ASpeed = 1.5,
  [PlayerX, PlayerY, PlayerA] = hd(Players),
  % Move UP
  if Message =:= "0" ->
    NewX = PlayerX + (math:sin(PlayerA) * Speed * Delta),
    NewY = PlayerY + (math:cos(PlayerA) * Speed * Delta),
    [[NewX, NewY, PlayerA] | tl(Players)];
  % Move DOWN
    Message =:= "2" ->
    NewX = PlayerX - (math:sin(PlayerA) * Speed * Delta),
    NewY = PlayerY - (math:cos(PlayerA) * Speed * Delta),
    [[NewX, NewY, PlayerA] | tl(Players)];
  % Rotate RIGHT
    Message =:= "1" ->
      NewA = PlayerA - (ASpeed * Delta),
      [[PlayerX, PlayerY, NewA] | tl(Players)];
  % Rotate LEFT
    Message =:= "3" ->
      NewA = PlayerA + (ASpeed * Delta),
      [[PlayerX, PlayerY, NewA] | tl(Players)];
    true ->
      io:format("Unrecognized command ~p~n", [Message]),
      [[PlayerX, PlayerY, PlayerA] | tl(Players)]
  end.