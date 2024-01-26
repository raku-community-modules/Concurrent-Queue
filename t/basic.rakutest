use Concurrent::Queue;
use Test;

given Concurrent::Queue.new -> $cq {
    is $cq.elems, 0, 'Elements count starts out as 0';
    nok $cq, 'Empty queue is falsey';
    is $cq.Seq.elems, 0, 'Empty queue snapshots to empty Seq';
    is-deeply $cq.list, (), 'Empty queue snapshots to empty list';

    my $fail = $cq.dequeue;
    isa-ok $fail, Failure, 'Dequeue of an empty queue fails';
    isa-ok $fail.exception, X::Concurrent::Queue::Empty,
        'Correct exception type in Failure';

    lives-ok { $cq.enqueue(42) }, 'Can enqueue a value';
    lives-ok { $cq.enqueue('beef') }, 'Can enqueue another value';
    is $cq.elems, 2, 'Correct element count after two enqueues';
    is-deeply $cq.Seq, (42, 'beef').Seq, 'Snapshots to Seq with correct values';
    is-deeply $cq.list, (42, 'beef'), 'Snapshots to list with correct values';
    is $cq.dequeue, 42, 'Dequeue gives the first enqueued value';
    is $cq.elems, 1, 'Correct element count after two enqueues and one dequeue';
    ok $cq, 'Non-empty queue is truthy';
    is-deeply $cq.Seq, ('beef',).Seq, 'Snapshots to Seq with correct values';
    is-deeply $cq.list, ('beef',), 'Snapshots to list with correct values';
    lives-ok { $cq.enqueue('kebab') }, 'Can enqueue another value after dequeueing';
    is-deeply $cq.Seq, ('beef','kebab').Seq, 'Snapshots to Seq with correct values';
    is-deeply $cq.list, ('beef','kebab'), 'Snapshots to list with correct values';
    is $cq.dequeue, 'beef', 'Second dequeue is second enqueued value';
    is $cq.dequeue, 'kebab', 'Third dequeue is third enqueued value';
    is $cq.elems, 0, 'Elements count should be 0 after all is dequeued';
    nok $cq, 'Empty-again queue is falsey';
    is $cq.Seq.elems, 0, 'Empty-again queue snapshots to empty Seq';
    is-deeply $cq.list, (), 'Empty-again queue snapshots to empty list';

    $fail = $cq.dequeue;
    isa-ok $fail, Failure, 'Dequeue of now-empty queue again fails';

    lives-ok { $cq.enqueue('schnitzel') }, 'Can enqueue to the now-empty queue';
    is $cq.dequeue, 'schnitzel', 'And can dequeue the value';

    $fail = $cq.dequeue;
    isa-ok $fail, Failure, 'Again, dequeue of now-empty-again queue fails';
}

done-testing;
