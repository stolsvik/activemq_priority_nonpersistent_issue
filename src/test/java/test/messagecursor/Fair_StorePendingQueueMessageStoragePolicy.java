package test.messagecursor;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.cursors.PendingMessageCursor;
import org.apache.activemq.broker.region.policy.PendingQueueMessageStoragePolicy;

/**
 * "Policy" for {@link Fair_StoreQueueCursor}, configurable via ActiveMQ' PolicyMap.
 */
public class Fair_StorePendingQueueMessageStoragePolicy implements PendingQueueMessageStoragePolicy {
    public PendingMessageCursor getQueuePendingMessageCursor(Broker broker, Queue queue) {
        return new Fair_StoreQueueCursor(broker, queue);
    }
}
