# Memory Model

Revoq uses a simple publication rule:

1. The appender reserves a frame slot.
2. The appender fills metadata and payload.
3. The appender publishes the frame length with `memory_order_release`.
4. The cursor loads frame length with `memory_order_acquire`.
5. The cursor reads metadata and payload only after the frame is committed.

This keeps the hot path compact while making producer-consumer visibility explicit.

The default appender policy is single-appender. A multi-appender policy is
available for serialized writes.
