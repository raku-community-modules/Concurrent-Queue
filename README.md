[![Actions Status](https://github.com/lizmat/Concurrent-Queue/actions/workflows/test.yml/badge.svg)](https://github.com/lizmat/Concurrent-Queue/actions)

NAME
====

Concurrent::Queue - A lock-free concurrent queue data structure

SYNOPSIS
========

```raku
use Concurrent::Queue;

my $queue = Concurrent::Queue.new;
$queue.enqueue('who');
$queue.enqueue('what');
$queue.enqueue('why');
say $queue.dequeue;       # who
say ?$queue;              # True
say $queue.elems;         # 2
say $queue.Seq;           # (what why)
say $queue.list;          # (what why)
say $queue.dequeue;       # what
$queue.enqueue('when');
say $queue.dequeue;       # why
say $queue.dequeue;       # when
say ?$queue;              # False
say $queue.elems;         # 0

my $val = $queue.dequeue;
say $val.WHO;             # Failure
say $val.exception.WHO;   # X::Concurrent::Queue::Empty
```

DESCRIPTION
===========

Lock-free data structures may be safely used from multiple threads, yet do not use locks in their implementation. They achieve this through the use of atomic operations provided by the hardware. Nothing can make contention between threads cheap - synchronization at the CPU level is still synchronization - but lock-free data structures tend to scale better.

This lock-free queue data structure implements an [algorithm described by Maged M. Michael and Michael L. ScottL](https://www.cs.rochester.edu/u/scott/papers/1996_PODC_queues.pdf). The only (intended) differences are:

  * A `Failure` is returned to indicate emptiness, rather than a combination of boolean return value and out parameter, in order that this type feels more natural to Raku language users

  * There is an out-of-band element count (which doesn't change the algorithm at all, just increments and decrements the count after an enqueue/dequeue)

  * Raku doesn't need ABA-problem mitigation thanks to having GC

The `elems` and `Bool` method should not be used to decide whether to dequeue, unless it is known that no other thread could be performing an enqueue or dequeue at the same time. Their only use in the presence of concurrent use of the queue is for getting an approximate idea of queue size. In the presence of a single thread, the element count will be accurate (so if many workers were to enqueue data, and are known to have completed, then at that point the `elems` will be an accurate reflection of how many values were placed in the queue).

Note that there is no blocking dequeue operation. If looking for a blocking queue, consider using the Raku built-in `Channel` class. (If tempted to write code that sits in a loop testing if `dequeue` gives back a `Failure` - don't. Use `Channel` instead.)

METHODS
=======

enqueue(Any $value)
-------------------

Puts the value into the queue at its tail. Returns `Nil`.

dequeue()
---------

If the queue is not empty, removes the head value and returns it. Otherwise, returns a `Failure` containing an exception of type `X::Concurrent::Queue::Empty`.

elems()
-------

Returns the number of elements in the queue. This value can only be relied upon when it is known that no threads are interacting with the queue at the point this method is called. **Never** use the result of `elems` to decide whether to `dequeue`, since another thread may `enqueue` or `dequeue` in the meantime. Instead, check if `dequeue` returns a `Failure`.

Bool()
------

Returns `False` if the queue is empty and `True` if the queue is non-empty. The result can only be relied upon when it is known that no other threads are interacting with the queue at the point this method is called. **Never** use the result of `Bool` to decide whether to `dequeue`, since another thread may `enqueue` or `dequeue` in the meantime. Instead, check if `dequeue` returns a `Failure`.

Seq
---

Returns a `Seq` that will iterate the queue contents. The iteration will include all values that had not been dequeued at the point the `Seq` method was called. Additionally, it will incorporate any values that are enqueued during the iteration, meaning that if values are being enqueued at a rate at least as fast as the iteration is visiting them then the iteration may not terminate.

If wanting to prevent this, consider limiting the result length to that of the result of `elems` (e.g. `$cq.head($cq.elems)`). If using the queue to collect values in many threads and then iterate them in one thread afterwards, this is not a concern, since nothing will be enqueueing further values at that point. However, consider using `Concurrent::Stack` instead, since a concurrent stack's operations are cheaper than those of a concurrent queue (the same algorithmic order, but a lower constant factor).

list
----

Equivalent to `.Seq.list`; see the description of `Seq` for caveats, and remember that a `List` preserves its elements, so the potentially endless iteration could also eat endless memory.

AUTHOR
======

Jonathan Worthington

COPYRIGHT AND LICENSE
=====================

Copyright 2018 - 2024 Raku Community

Copyright 2024 Raku Community

This library is free software; you can redistribute it and/or modify it under the Artistic License 2.0.

