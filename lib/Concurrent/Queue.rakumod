class X::Concurrent::Queue::Empty is Exception {
    method message() {
        "Cannot dequeue from an empty queue"
    }
}

class Concurrent::Queue {
    # Each value in the queue is held in a node, which points to the next
    # value to dequeue, if any.
    my class Node {
        has $.value;
        # Must not rely on auto-initialization of attributes when using CAS,
        # as it will be racey.
        has Node $.next is rw = Node;
    }
    has Node $.head;
    has Node $.tail;
    has atomicint $.elems;

    submethod BUILD(--> Nil) {
        # Head and tail initially point to a dummy node.
        $!head = $!tail = Node.new;
    }

    method enqueue($value --> Nil) {
        my $node = Node.new: :$value;
        my $tail;
        loop {
            $tail = $!tail;
            my $next = $tail.next;
            if $tail === $!tail {
                if $next.DEFINITE {
                    # Something else inserted to the queue, but $!tail was not
                    # yet updated. Help by updating it (and don't check the
                    # outcome, since if we fail another thread did this work
                    # in our place).
                    cas($!tail, $tail, $next);
                }
                else {
                    last if cas($tail.next, $next, $node) === $next;
                }
            }
        }
        # Try to update $!tail to point to the new node. If we fail, it's
        # because we didn't get to do this soon enough, and another thread did
        # it in order that it could make progress. Thus a failure to swap here
        # is fine.
        cas($!tail, $tail, $node);
        $!elems⚛++;
    }

    method dequeue() {
        loop {
            my $head = $!head;
            my $tail = $!tail;
            my $next = $head.next;
            if $head === $!head {
                if $head === $tail {
                    # Head and tail point to the same place. Two cases:
                    if $next.DEFINITE {
                        # The head node has a next. This means there is an
                        # enqueue in motion that did not manage to update the
                        # $!tail yet. Help it on its way; failure to do so
                        # is fine, it just means another thread did it.
                        cas($!tail, $tail, $next);
                    }
                    else {
                        # Head and tail point to a dummy node and there's no
                        # ongoing insertion. That means the queue is empty.
                        fail X::Concurrent::Queue::Empty.new;
                    }
                }
                else {
                    if cas($!head, $head, $next) === $head {
                        # Successfully dequeued. The head node is always a
                        # dummy. The value is in the next node. That node
                        # becomes the new dummy.
                        $!elems⚛--;
                        return $next.value;
                    }
                }
            }
        }
    }

    multi method Bool(Concurrent::Queue:D: --> Bool) {
        $!elems != 0
    }

    multi method Seq(Concurrent::Queue:D: --> Seq) {
        my Node $current = $!head.next;
        gather while $current.DEFINITE {
            take $current.value;
            $current = $current.next;
        }
    }

    multi method list(Concurrent::Queue:D: --> List) {
        self.Seq.list
    }
}

=begin pod

=head1 NAME

Concurrent::Queue - A lock-free concurrent queue data structure

=head1 SYNOPSIS

=begin code :lang<raku>

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

=end code

=head1 DESCRIPTION

Lock-free data structures may be safely used from multiple threads,
yet do not use locks in their implementation. They achieve this
through the use of atomic operations provided by the hardware.
Nothing can make contention between threads cheap - synchronization
at the CPU level is still synchronization - but lock-free data
structures tend to scale better.

This lock-free queue data structure implements an
L<algorithm described by Maged M. Michael and Michael L. ScottL|https://www.cs.rochester.edu/u/scott/papers/1996_PODC_queues.pdf>.  The only (intended) differences are:

=item A C<Failure> is returned to indicate emptiness, rather than
a combination of boolean return value and out parameter, in order
that this type feels more natural to Raku language users

=item There is an out-of-band element count (which doesn't change
the algorithm at all, just increments and decrements the count
after an enqueue/dequeue)

=item Raku doesn't need ABA-problem mitigation thanks to having GC

The C<elems> and C<Bool> method should not be used to decide whether
to dequeue, unless it is known that no other thread could be performing
an enqueue or dequeue at the same time. Their only use in the presence
of concurrent use of the queue is for getting an approximate idea of
queue size. In the presence of a single thread, the element count will
be accurate (so if many workers were to enqueue data, and are known to
have completed, then at that point the C<elems> will be an accurate
reflection of how many values were placed in the queue).

Note that there is no blocking dequeue operation. If looking for a
blocking queue, consider using the Raku built-in C<Channel> class.
(If tempted to write code that sits in a loop testing if C<dequeue>
gives back a C<Failure> - don't. Use C<Channel> instead.)

=head1 METHODS

=head2 enqueue(Any $value)

Puts the value into the queue at its tail. Returns C<Nil>.

=head2 dequeue()

If the queue is not empty, removes the head value and returns it.
Otherwise, returns a C<Failure> containing an exception of type
C<X::Concurrent::Queue::Empty>.

=head2 elems()

Returns the number of elements in the queue. This value can only be
relied upon when it is known that no threads are interacting with the
queue at the point this method is called. B<Never> use the result of
C<elems> to decide whether to C<dequeue>, since another thread may
C<enqueue> or C<dequeue> in the meantime. Instead, check if C<dequeue>
returns a C<Failure>.

=head2 Bool()

Returns C<False> if the queue is empty and C<True> if the queue is
non-empty.  The result can only be relied upon when it is known that
no other threads are interacting with the queue at the point this
method is called. B<Never> use the result of C<Bool> to decide whether
to C<dequeue>, since another thread may C<enqueue> or C<dequeue> in
the meantime. Instead, check if C<dequeue> returns a C<Failure>.

=head2 Seq

Returns a C<Seq> that will iterate the queue contents. The iteration
will include all values that had not been dequeued at the point the
C<Seq> method was called. Additionally, it will incorporate any values
that are enqueued during the iteration, meaning that if values are being
enqueued at a rate at least as fast as the iteration is visiting them
then the iteration may not terminate.

If wanting to prevent this, consider limiting the result length to that
of the result of `elems` (e.g. C<$cq.head($cq.elems)>). If using the
queue to collect values in many threads and then iterate them in one
thread afterwards, this is not a concern, since nothing will be
enqueueing further values at that point. However, consider using
C<Concurrent::Stack> instead, since a concurrent stack's operations
are cheaper than those of a concurrent queue (the same algorithmic
order, but a lower constant factor).

=head2 list

Equivalent to C<.Seq.list>; see the description of C<Seq> for caveats,
and remember that a C<List> preserves its elements, so the potentially
endless iteration could also eat endless memory.

=head1 AUTHOR

Jonathan Worthington

=head1 COPYRIGHT AND LICENSE

Copyright 2018 - 2022 Jonathan Worthington

Copyright 2024 Raku Community

This library is free software; you can redistribute it and/or modify it under the Artistic License 2.0.

=end pod
