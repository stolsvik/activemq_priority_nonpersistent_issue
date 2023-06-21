package test.messagecursor;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.cursors.PendingMessageCursor;
import org.apache.activemq.broker.region.policy.PendingQueueMessageStoragePolicy;

/**
 * "Policy" for {@link Cleaned_StoreQueueCursor}, configurable via ActiveMQ' PolicyMap.
 */
public class Cleaned_StorePendingQueueMessageStoragePolicy implements PendingQueueMessageStoragePolicy {
    public PendingMessageCursor getQueuePendingMessageCursor(Broker broker, Queue queue) {
        return new Cleaned_StoreQueueCursor(broker, queue);
    }
}
