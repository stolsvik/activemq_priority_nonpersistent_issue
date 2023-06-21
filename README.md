# ActiveMQ Message ordering and prioritization are broken when persistent and non-persistent messages are mixed on the same queue

2023-06-10 - 2023-06-21

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
that are also non-persistent. These messages are sent on the same queues as "ordinary" messages: Persistent, with normal
priority. This combination of message type, along with the described quirk, then obviously leads to the completely
opposite result than the intention: The supposedly "fast, prioritized, but not entirely reliable" safe or idempotent
GET-style messages/commands will be starved if there also are a batch of "ordinary" messages going on using the same
queues.

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

Note that to easily experience this, you should set both the maxPageSize and client prefetch to 1. Otherwise, it seems
like several of these issues are masked by either the layer above, or on the client - i.e. it reorders, and takes into
consideration the prioritization. However, when you produce thousands of messages, the page size of 200 and prefetch of
1000 cannot mask it anymore, and the problem shows itself (in production, obviously!). But it is harder to observe, and
reason about, such large amounts of messages, thus setting these values to 1 gives you the full experience right away.

_Update 2023-06-21:_ I have now also implemented a fair solution, which both takes priority and ordering into account when returning
messages from the `next()` call. It does this by keeping one message from each of the "sub cursors" in memory (by 
"filling" any missing on the `hasNext()` call), and then choosing between them.

The main code for this is as follows, the main points being in the `hasNext()`, `next()` and `remove()` methods:
```java
class StoreQueueCursor implements PendingMessageCursor {
    // ....

    /**
     * Resets the cursors.
     */
    @Override
    public synchronized void reset() {
        assertStarted();

        // :: Reset the two sub cursors
        persistent.reset();
        nonPersistent.reset();

        // :: Also null out our "heads" from the cursors
        _fromPersistent = null;
        _fromNonPersistent = null;
    }


    @Override
    public synchronized boolean isEmpty() {
        assertStarted();

        boolean empty = persistent.isEmpty() && nonPersistent.isEmpty();
        LOG.info("isEmpty(): " + empty);

        return empty;
    }

    private MessageReference _fromPersistent;
    private MessageReference _fromNonPersistent;

    private boolean _hasNextCalled; // To assert whether call flow is as expected.

    /**
     * Do we have a next cursor position? (Idempotent - multiple calls yield same result)
     */
    @Override
    public synchronized boolean hasNext() {
        assertStarted();

        // :: Pre-fill our "heads" from the two sub cursors, thus advancing 0 or 1 of the two sub cursors.

        // Note: This is a modifying operation. A message is fetched from at least one of the cursor (if there are any
        // messages, that is), and the cursor is thus advanced.

        LOG.info("# hasNext() invoked.");

        if (_fromPersistent == null) {
            if (persistent.hasNext()) {
                _fromPersistent = persistent.next();
                LOG.info("  \\- hasNext() - paged in from PERSISTENT.");
            }
            else {
                LOG.info("  \\- hasNext() - NOT paged in from PERSISTENT.");
            }
        }

        if (_fromNonPersistent == null) {
            if (nonPersistent.hasNext()) {
                _fromNonPersistent = nonPersistent.next();
                LOG.info("  \\- hasNext() - paged in from NON_PERSISTENT.");
            }
            else {
                LOG.info("  \\- hasNext() - NOT paged in from NON_PERSISTENT.");
            }
        }

        // ----- We now have a message from each of the sub cursors, if there was any.

        // Record that we've invoked hasNext, to assert "contract" in .next().
        _hasNextCalled = true;

        // Do we have a MessageReference from either of the sub cursors?
        return _fromPersistent != null || _fromNonPersistent != null;
    }


    private enum NextFromSubCursor {
        NONE, PERSISTENT, NON_PERSISTENT
    }

    private NextFromSubCursor _nextFromSubCursor = NextFromSubCursor.NONE;

    @Override
    public synchronized MessageReference next() {
        if (!_hasNextCalled) {
            throw new IllegalStateException("hasNext() has not been called before next().");
        }

        _hasNextCalled = false;

        // ?: Are they both null?
        // Note: Semantically, one should invoke .hasNext() in front of each .next(). The null-return situation shall
        // never occur.
        if (_fromPersistent == null && _fromNonPersistent == null) {
            // Note: Neither of the two sub cursors have been advanced, so we're still on NextFromSubCursor.NONE.
            LOG.info("# next() invoked - no messages available.");
            return null;
        }

        boolean pickPersistent;

        // :: If there is messages only from one of the sub cursor, then that is what we're returning.
        // ?: Has only one of the sub cursors message?
        if ((_fromPersistent == null) != (_fromNonPersistent == null)) {
            // -> Yes, only one has message - pick the one
            // Persistent is "true next" if it has message
            pickPersistent = _fromPersistent != null;
        }
        else {
            // -> No, both have messages. Find which is "true next"

            // NOTE: Priority is first evaluation, then ordering.

            int persistentPriority = _fromPersistent.getMessage().getPriority();
            int nonPersistentPriority = _fromNonPersistent.getMessage().getPriority();

            // ?: Do we have differing priorities?
            if (persistentPriority != nonPersistentPriority) {
                // -> Yes, differing priorities. Pick the highest.
                // Persistent is "true next" if it has highest priority.
                pickPersistent = persistentPriority > nonPersistentPriority;
            }
            else {
                // -> No, we have same priorities. Pick the next based on order.
                // Need to establish order. Using BrokerInTime, then messageId.
                long persistentBrokerInTime = _fromPersistent.getMessage().getBrokerInTime();
                long nonPersistentBrokerInTime = _fromNonPersistent.getMessage().getBrokerInTime();

                // ?: Do we have differing BrokerInTime?
                if (persistentBrokerInTime != nonPersistentBrokerInTime) {
                    // -> Yes, differing BrokerInTime. Pick the lowest.
                    // Persistent is "true next" if it has lowest BrokerInTime.
                    pickPersistent = persistentBrokerInTime < nonPersistentBrokerInTime;
                }
                else {
                    // -> No, we have same BrokerInTime. Pick the next based on messageId.
                    MessageId persistentMessageId = _fromPersistent.getMessageId();
                    // Defensive coding - if for some reason null, then just go for persistent as "true next"
                    // ?: Are we missing MessageId from persistent?
                    if (persistentMessageId == null) {
                        // -> Yes, missing - so just go for persistent as "true next"
                        pickPersistent = true;
                    }
                    else {
                        // -> No, we have MessageId, so compare
                        MessageId nonPersistentMessageId = _fromNonPersistent.getMessageId();
                        // Persistent is "true next" if it has lowest MessageId.
                        pickPersistent = persistentMessageId.compareTo(nonPersistentMessageId) < 0;
                    }
                }
            }
        }

        // ?: Is Persistent "true next"?
        if (pickPersistent) {
            // -> Yes, persistent is "true next"
            _nextFromSubCursor = NextFromSubCursor.PERSISTENT;
            MessageReference ret = _fromPersistent;
            _fromPersistent = null;
            LOG.info("# next() invoked - returned message from PERSISTENT.");
            return ret;
        }
        // E-> No, nonPersistent is "true next"
        _nextFromSubCursor = NextFromSubCursor.NON_PERSISTENT;
        MessageReference ret = _fromNonPersistent;
        _fromNonPersistent = null;
        LOG.info("# next() invoked - returned message from NON_PERSISTENT.");
        return ret;
    }

    @Override
    public synchronized void remove() {
        switch (_nextFromSubCursor) {
            case PERSISTENT:
                LOG.info("# remove() invoked - forwarded to PERSISTENT.");
                persistent.remove();
                break;
            case NON_PERSISTENT:
                LOG.info("# remove() invoked - forwarded to NON_PERSISTENT.");
                nonPersistent.remove();
                break;
            case NONE:
                LOG.error("# remove() invoked - NO PREVIOUS SUB CURSOR PICKED, this is an error!");
                throw new IllegalStateException("There is no current cursor position; .next() has not been invoked (or yielded null).");
        }
        _nextFromSubCursor = NextFromSubCursor.NONE;
    }

    @Override
    public synchronized void remove(MessageReference node) {
        assertStarted();

        LOG.error("# remove(MessageReference) invoked [" + node + "]");

        // :: Remove from the relevant sub cursor
        if (!node.isPersistent()) {
            nonPersistent.remove(node);
        }
        else {
            persistent.remove(node);
        }

        // :: Defensive: Also null the pre-fetched message if it is the one being removed
        // Note: MessageReference does not define equals(), so it becomes instance equality. That'll have to suffice.
        if ((_fromPersistent != null) && _fromPersistent.equals(node)) {
            _fromPersistent = null;
        }
        else if ((_fromNonPersistent != null) && _fromNonPersistent.equals(node)) {
            _fromNonPersistent = null;
        }
    }

    // ....
}

```


PS: It seems like the corresponding topic side of this, StoreDurableSubscriberCursor with TopicStorePrefetch, have had
some work done for it in 2010 for probably the same type of issue, adding a "immediatePriorityDispatch" flag and
corresponding functionality: _"ensure new high priority messages get dispatched immediately rather than at the end of
the next batch, configurable via PendingDurableSubscriberMessageStoragePolicy.immediatePriorityDispatch default true,
most relevant with prefetch=1"_. I don't fully understand this solution, but can't shake the feeling that it is a
literal patch instead of handling the underlying problem: Dequeuing from two underlying queues ("buckets") must take
into account the head of both, finding the "true next" wrt. order and priority.

**Note: There are multiple implementations of the `StoreQueueCursor` inside the `messagecursor` package.** 