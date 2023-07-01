Prefetch 1000:
maxPageSize: 200, prefetch (policy): 1000
isEmpty true, next message 3601  (seemingly 2 x 200 + 1000)
Previous batch, next message 3401 (200 diff)
Previous batch, next message 3201 (200 diff)
Note that no hasNext() invoked returning FALSE - goes directly to isEmpty:true

maxPageSize: 150, prefetch (policy): 1000
isEmpty true, previous message 3700  (seemingly 2 x 150 + 1000)
Previous batch, next message 3550 (150 diff)
Previous batch, next message 3400 (150 diff)
Note that no hasNext() invoked returning FALSE - goes directly to isEmpty:true

Odd pagesize:
maxPageSize: 101, prefetch (policy): 1000
isEmpty true, previous message 3858  (seemingly 142 + 1000) - Probably since 101 doesn't "go up" numberwise in 6000 msgs, the last batch ended up with bottom-ups of 41 messages?
Previous batch, next message 3757 (101 diff)
Previous batch, next message 3656 (101 diff)

Prefetch 2000:
maxPageSize: 100, prefetch (policy): 2000
isEmpty true, previous message 2841  (seemingly 159 + 2000) - The prefetch might now have taken too long time in the beginning, giving us an offset?
Previous batch, next message 2741 (100 diff)
Previous batch, next message 2641 (100 diff)

++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
REDUCING number of messages, only persistent, 3000 messages - also increasing time before receiver kicks in, thus
getting all messages to broker.

Prefetch 2000, 3000 messages
maxPageSize: 100, prefetch (policy): 2000
isEmpty true, previous message 800  (seemingly 2 x 100 + 2000) - Exactly as now expected: The client has prefetched 2000, and there are "two levels" of paging.
Previous batch, next message 700 (100 diff)
Previous batch, next message 600 (100 diff)

---------------------------------------------------------------
What about prefetch on the client side? How does this impact?

Baselining:
maxPageSize: 50, prefetch (policy): 400
isEmpty true, previous message 2500  (seemingly 2 x 50 + 400) - Exactly as expected.
Previous batch, next message 2450 (50 diff)
Previous batch, next message 2400 (50 diff)

Changing prefetch on the ConnectionFactory - INCREASING: Seems to be no effect, but that's wrong conclusion:
THE POINT IS THAT 1000 is "magical": It is the default, and THEN the server setting "takes".
-> connectionFactory.getPrefetchPolicy().setQueuePrefetch(1000);
maxPageSize: 50, prefetch (policy): 400
-> Doesn't do jack shit - exact same result as above.
isEmpty true, previous message 2500  (seemingly 2 x 50 + 400) - INCREASE on client didn't "take".
Previous batch, next message 2450 (50 diff)
Previous batch, next message 2400 (50 diff)

Changing prefetch on the ConnectionFactory - DECREASING: TOOK EFFECT. Lowest seemingly kicks in. NO WRONG CONCLUSION!
-> connectionFactory.getPrefetchPolicy().setQueuePrefetch(200);
maxPageSize: 50, prefetch (policy): 400
-> This kicked in. Seems to be lowest of the values - no: Client wins unless it is 1000.
isEmpty true, previous message 2700  (seemingly 2 x 50 + 200) - So, the effective prefetch is now 200.
Previous batch, next message 2650 (50 diff)
Previous batch, next message 2600 (50 diff)

Keeping prefetch on the ConnectionFactory, but decreasing on policy... That didn't give expected result!!
-> connectionFactory.getPrefetchPolicy().setQueuePrefetch(200);
maxPageSize: 50, prefetch (policy): 100
-> WTF?
isEmpty true, previous message 2700  (seemingly 2 x 50 + 200) - So, the effective prefetch is now 200!!
Previous batch, next message 2650 (50 diff)
Previous batch, next message 2600 (50 diff)

Okay, trying two other: 500, 250
-> connectionFactory.getPrefetchPolicy().setQueuePrefetch(500):
maxPageSize: 50, prefetch (policy): 250
-> Client side takes precedence
isEmpty true, previous message 2400  (seemingly 2 x 50 + 500)
Previous batch, next message 2350 (50 diff)
Previous batch, next message 2300 (50 diff)

Other side: 250, 500
-> connectionFactory.getPrefetchPolicy().setQueuePrefetch(250);
maxPageSize: 50, prefetch (policy): 500
-> Client side takes precedence
isEmpty true, previous message 2650  (seemingly 2 x 50 + 250)
Previous batch, next message 2600 (50 diff)
Previous batch, next message 2550 (50 diff)


!! Not setting client!!
maxPageSize: 50, prefetch (policy): 500
-> Okay, so server setting takes if not setting on client.
isEmpty true, previous message 2400  (seemingly 2 x 50 + 500)
Previous batch, next message 2350 (50 diff)
Previous batch, next message 2300 (50 diff)

Setting client higher than server 800 vs. 500
-> connectionFactory.getPrefetchPolicy().setQueuePrefetch(800);
maxPageSize: 50, prefetch (policy): 500
-> Okay, client wins
isEmpty true, previous message 2100  (seemingly 2 x 50 + 800)
Previous batch, next message 2050 (50 diff)
Previous batch, next message 2000 (50 diff)

Here's the smoking gun: If client says 1000, server wins.

Setting client higher than server 1000 vs. 500
-> connectionFactory.getPrefetchPolicy().setQueuePrefetch(1000);
maxPageSize: 50, prefetch (policy): 500
-> Okay, client LOSES IF IT IS 1000!!!
isEmpty true, previous message 2400  (seemingly 2 x 50 + 500)

Setting client higher than server 999 vs. 500
-> connectionFactory.getPrefetchPolicy().setQueuePrefetch(999);
maxPageSize: 50, prefetch (policy): 500
-> Okay, client wins
isEmpty true, previous message 1901  (seemingly 2 x 50 + 999)

Setting client higher than server 1001 vs. 500
-> connectionFactory.getPrefetchPolicy().setQueuePrefetch(1001);
maxPageSize: 50, prefetch (policy): 500
-> Okay, client wins
isEmpty true, previous message 1899  (seemingly 2 x 50 + 1001)



Okay, so here's the actual code doing this logic:

Basically, the server will check if the prefetch number is the default - and if yes, only then does it override
with the PolicyEntry for the queue.

Basically, the client side WINS unless it is default (1000) - in which case the serve side wins.

PolicyEntry.java (server side):

    public void configurePrefetch(Subscription subscription) {

        final int currentPrefetch = subscription.getConsumerInfo().getPrefetchSize();
        if (subscription instanceof QueueBrowserSubscription) {
            if (currentPrefetch == ActiveMQPrefetchPolicy.DEFAULT_QUEUE_BROWSER_PREFETCH) {
                ((QueueBrowserSubscription) subscription).setPrefetchSize(getQueueBrowserPrefetch());
            }
        } else if (subscription instanceof QueueSubscription) {
            if (currentPrefetch == ActiveMQPrefetchPolicy.DEFAULT_QUEUE_PREFETCH) {
                ((QueueSubscription) subscription).setPrefetchSize(getQueuePrefetch());
            }
        } else if (subscription instanceof DurableTopicSubscription) {
            if (currentPrefetch == ActiveMQPrefetchPolicy.DEFAULT_DURABLE_TOPIC_PREFETCH ||
                    subscription.getConsumerInfo().getPrefetchSize() == ActiveMQPrefetchPolicy.DEFAULT_OPTIMIZE_DURABLE_TOPIC_PREFETCH) {
                ((DurableTopicSubscription)subscription).setPrefetchSize(getDurableTopicPrefetch());
            }
        } else if (subscription instanceof TopicSubscription) {
            if (currentPrefetch == ActiveMQPrefetchPolicy.DEFAULT_TOPIC_PREFETCH) {
                ((TopicSubscription) subscription).setPrefetchSize(getTopicPrefetch());
            }
        }
        if (currentPrefetch != 0 && subscription.getPrefetchSize() == 0) {
            // tell the sub so that it can issue a pull request
            subscription.updateConsumerPrefetch(0);
        }
    }
