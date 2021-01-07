-module(state).

-export([state_start/2, state_proc_loop/1, state_udp/2]).

state_start(State, StatePort) ->
  io:format("State module running with PID: ~p~n", [self()]),
  StatePid = spawn(?MODULE, state_proc_loop, [State]),
  spawn(?MODULE, state_udp, [StatePid, StatePort]),
  StatePid.

parse_message(Message) -> string:split(Message, "|").

state_proc_loop (State) ->
  receive
    {get, Pid} ->
      Pid ! {state, State},
      state_proc_loop(State) ;
    {store, Message} ->
      [Players, Map] = State,
      [Id, Msg] = parse_message(Message),
      NewPlayers = store_message(Players, Id, Msg, Map),
      state_proc_loop([NewPlayers, Map])
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
  PlayerData = hd(Data) ++ ["|" ++ float_to_list(I) || I <- tl(Data)] ++ "|\n",
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

hit_wall(X, Y, Map) ->
  XRow = lists:nth(floor(X + 1), Map),
  CellCode = lists:nth(floor(Y + 1), XRow),
  if CellCode =:= 35 ->
    true;
  true -> false
  end.

store_message(Players, QueryId, Msg, Map) ->
  %io:format("Message received: ~p~n", [Message]),
  CurrentPlayer = hd(Players),
  if hd(CurrentPlayer) /= QueryId -> [CurrentPlayer|store_message(tl(Players), QueryId, Msg, Map)];
  true ->
    Delta = 0.2,
    Speed = 5,
    ASpeed = 1.5,
    [Id, PlayerX, PlayerY, PlayerA] = CurrentPlayer,
    % Move UP
    if Msg =:= "0" ->
      NewX = PlayerX + (math:sin(PlayerA) * Speed * Delta),
      NewY = PlayerY + (math:cos(PlayerA) * Speed * Delta),
      HitWall = hit_wall(NewX, NewY, Map),
      if HitWall ->
        [[Id, PlayerX, PlayerY, PlayerA] | tl(Players)];
      true ->
        [[Id, NewX, NewY, PlayerA] | tl(Players)]
      end;
    % Move DOWN
      Msg =:= "2" ->
      NewX = PlayerX - (math:sin(PlayerA) * Speed * Delta),
      NewY = PlayerY - (math:cos(PlayerA) * Speed * Delta),
      HitWall = hit_wall(NewX, NewY, Map),
      if HitWall ->
        [[Id, PlayerX, PlayerY, PlayerA] | tl(Players)];
      true ->
        [[Id, NewX, NewY, PlayerA] | tl(Players)]
      end;
    % Rotate RIGHT
      Msg =:= "1" ->
        NewA = PlayerA + (ASpeed * Delta),
        [[Id, PlayerX, PlayerY, NewA] | tl(Players)];
    % Rotate LEFT
      Msg =:= "3" ->
        NewA = PlayerA - (ASpeed * Delta),
        [[Id, PlayerX, PlayerY, NewA] | tl(Players)];
      true ->
        io:format("Unrecognized command ~p~n", [Msg]),
        [[Id, PlayerX, PlayerY, PlayerA] | tl(Players)]
    end
  end.