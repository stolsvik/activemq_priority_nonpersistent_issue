package test.messagecursor_copied;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.region.cursors.PendingMessageCursor;
import org.apache.activemq.broker.region.policy.PendingQueueMessageStoragePolicy;

/**
 * "Policy" for {@link SomewhatFair_Copied_StoreQueueCursor}, configurable via ActiveMQ' PolicyMap.
 */
public class SomewhatFair_Copied_StorePendingQueueMessageStoragePolicy implements PendingQueueMessageStoragePolicy {
    public PendingMessageCursor getQueuePendingMessageCursor(Broker broker,
            org.apache.activemq.broker.region.Queue queue) {
        return new SomewhatFair_Copied_StoreQueueCursor(broker, queue);
    }
}
