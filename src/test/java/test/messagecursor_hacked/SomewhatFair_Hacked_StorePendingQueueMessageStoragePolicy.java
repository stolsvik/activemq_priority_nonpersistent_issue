package test.messagecursor_hacked;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.region.cursors.PendingMessageCursor;
import org.apache.activemq.broker.region.policy.PendingQueueMessageStoragePolicy;

/**
 * "Policy" for {@link SomewhatFair_Hacked_StoreQueueCursor}, configurable via ActiveMQ' PolicyMap.
 */
public class SomewhatFair_Hacked_StorePendingQueueMessageStoragePolicy implements PendingQueueMessageStoragePolicy {
    public PendingMessageCursor getQueuePendingMessageCursor(Broker broker,
            org.apache.activemq.broker.region.Queue queue) {
        return new SomewhatFair_Hacked_StoreQueueCursor(broker, queue);
    }
}
