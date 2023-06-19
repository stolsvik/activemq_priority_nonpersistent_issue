# ActiveMQ Message ordering and prioritization are broken when persistent and non-persistent messages are mixed on the same queue

2023-06-10 - 2023-06-19

I am researching, and have identified, a problem with the default configuration of ActiveMQ's queue handling. It employs
a class called *StoreQueueCursor*, and this again employs two sub cursors, one handling persistent messages, and the
other handling non-persistent messages. The problem revolves around how it changes between these two "buckets".

_(I've found a RedHat issue from 2014 (https://issues.redhat.com/browse/ENTMQ-872) which describes the issue I angled in
from (persistent messages are always "preferred" in front of non-persistent messages, even if the latter have high
priority) - but it turns out the problem is deeper than that.)_

As described on the message-cursors page (https://activemq.apache.org/message-cursors), in the "paging for
non-persistent messages" section and image, the store queue cursor has two "buckets" to handle persistent and
non-persistent messages.

The problem arises from how it handles fetching messages from these two buckets. Basically, it switches between these
two buckets only when the current bucket is (effectively) empty.

This affects ordering _(if you on a producer alternate between persistent and non-persistent messages, they will not be
consumed in order, as the "current bucket" will be emptied first)_, and can lead to starvation _(the "current bucket" is
emptied before switching, so if producers keep up, you will never get a message from the 'opposite" bucket)_, and also
thus effectively ignores prioritization _(since it doesn't even consider the opposite bucket while the current is
non-empty)_.

My situation is that in the library Mats3 (https://mats3.io/), one often employ "interactive" messages (priority=9)
combined with non-persistent messaging - on the same queues. This then obviously leads to the completely opposite result
than the intention: The supposedly "fast, prioritized, but not entirely reliable" safe or idempotent GET-style
messages/commands will be starved if there also are a batch of "ordinary" messages going on using the same queues.

I have come up with a minimal solution that fixes my problem: I need to remove the starvation, and thus the ignoring of
prioritization. But this solution will possibly make the dispatch in-order situation worse. What I do, is to change
the 'getNextCursor()' method to always alternate between the buckets if there are messages in both. That is, if there
are messages in the opposite bucket, then switch. This fixes much - and is probably better for most users, without any
discernible side effects.

More detailed:

The problem is the combination of these three methods:

```java
class StoreQueueCursor extends AbstractPendingMessageCursor {
    // ....
    
    @Override
    public synchronized boolean hasNext() {
        try {
            getNextCursor();
        }
        catch (Exception e) {
            LOG.error("Failed to get current cursor ", e);
            throw new RuntimeException(e);
        }
        return currentCursor != null ? currentCursor.hasNext() : false;
    }

    @Override
    public synchronized MessageReference next() {
        MessageReference result = currentCursor != null ? currentCursor.next() : null;
        return result;
    }

    // ....
    
    protected synchronized PendingMessageCursor getNextCursor() throws Exception {
        if (currentCursor == null || !currentCursor.hasMessagesBufferedToDeliver()) {
            currentCursor = currentCursor == persistent ? nonPersistent : persistent;
            // sanity check
            if (currentCursor.isEmpty()) {
                currentCursor = currentCursor == persistent ? nonPersistent : persistent;
            }
        }
        return currentCursor;
    }
    
    // ....
}
```

If I change the getNextCursor() method to this code, most things gets better:

```java
protected synchronized PendingMessageCursor getNextCursor() throws Exception {
    // ?: Sanity check that nonPersistent has been set, i.e. that start() has been invoked.
    if (nonPersistent == null) {
        // -> No, not set, so don't switch currentCursor to it - so that currentCursor never becomes null.
        return currentCursor;
    }

    // Get opposite cursor:
    PendingMessageCursor oppositeCursor = currentCursor == persistent ? nonPersistent : persistent;
    // ?: Do we have any messages in the opposite?
    if (oppositeCursor.hasNext()) {
        // -> Yes, so do the switch
        currentCursor = oppositeCursor;
    }
    return currentCursor;
}
```

.. but with this, producing a bunch of persistent messages, and then non-persistent, will lead to them being fetched
alternating (even though you wanted all the persistent first, then non-persistent). Then again, if you did the opposite,
that is, produced a bunch of non-persistent, then persistent - the current solution will first dequeue all the persistent. So,
it's bad anyhow.

_(A complete solution would require a "peek" functionality to see which bucket had the highest priority message, and
identify a way to find the order between two messages when their priority was equal (an ordering on received datetime,
then messageId, could maybe be a good order?). You'd then always switch to the bucket with the "true next" message. An
alternative to "peek" might be to always have one message dequeued from each of the buckets, but not removing it: When
the `next()` method is invoked, one make sure to have present one from each of the `persistent` and `nonPersistent`
underlying buckets. Compare them, and return the "true next". When the 'remove' call comes, remove the correct message.
When the next 'hasNext + next' call comes, fetch a replacement for the one returned last time, and compare again. If the
remove semantics works as hoped, this would effectively be a replacement for the "peek" functionality.)_

Note that to easily experience this, you should set both the maxPageSize and client prefetch to 1. Otherwise, it seems
like several of these issues are masked by either the layer above, or on the client - i.e. it reorders, and takes into
consideration the prioritization. However, when you produce thousands of messages, the page size of 200 and prefetch of
1000 cannot mask it anymore, and the problem shows itself (in production, obviously!). But it is harder to observe, and
reason about, such large amounts of messages, thus setting these values to 1 gives you the full experience right away.

PS: It seems like the corresponding topic side of this, StoreDurableSubscriberCursor with TopicStorePrefetch, have had
some work done for it in 2010 for probably the same type of issue, adding a "immediatePriorityDispatch" flag and
corresponding functionality: _"ensure new high priority messages get dispatched immediately rather than at the end of
the next batch, configurable via PendingDurableSubscriberMessageStoragePolicy.immediatePriorityDispatch default true,
most relevant with prefetch=1"_. I don't fully understand this solution, but can't shake the feeling that it is a
literal patch instead of handling the underlying problem: Dequeuing from two underlying queues ("buckets") must take
into account the head of both, finding the "true next" wrt. order and priority.

**Note: There are two implementations of the "SomewhatFair" StoreQueueCursor inside this repo - one using reflection, and
one copying out enough classes until the compiler stops complaining about package-private classes and methods. Those are
in the `messagecursor_hacked` and `messagecursor_copied` packages.** 