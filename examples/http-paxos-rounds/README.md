# HTTP Paxos With Rounds

This example uses the same `Node<T>` implementation as the TraceForge model,
with an HTTP transport implementing `traceforge_rounds::Transport`.

Each protocol message is serialized as an `Envelope<PaxosRound, Command>` via a
small wire wrapper.
