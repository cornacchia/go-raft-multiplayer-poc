-module(node).

-export([start/2]).

start (StatePid, CmdPort) ->
  io:format("Node module running with PID: ~p~n", [self()]),
  NodePid = self(),
  spawn(fun () -> cmd_udp(NodePid, CmdPort) end),
  node_loop(StatePid).

node_loop(StatePid) ->
  receive
    {command, Message} ->
      StatePid ! {store, Message},
      node_loop(StatePid);
    _ -> io:format("Node Loop: Unrecognized msg\n")
  end.

cmd_udp (NodePid, CmdPort) ->
  {ok, Socket} = gen_udp:open(CmdPort, [binary, {active, true}]),
  io:format("Node module opened socket: ~p~n", [Socket]),
  ui_cmd_loop(Socket, NodePid).

ui_cmd_loop(Socket, NodePid) ->
  inet:setopts(Socket, [{active, once}]),
  receive
    {udp, Socket, Host, Port, Bin} ->
      %%Convert incoming binary message to a string
      Message = binary_to_list(Bin),
      io:format("Cmd UDP received: "),
      io:format(Message),
      io:format("\n"),
      gen_udp:send(Socket, Host, Port, <<"OK">>),
      NodePid ! {command, Message},
      ui_cmd_loop(Socket, NodePid)
  end.
