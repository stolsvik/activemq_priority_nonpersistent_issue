/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package test.messagecursor;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.cursors.FilePendingMessageCursor;
import org.apache.activemq.broker.region.cursors.PendingMessageCursor;
import org.apache.activemq.broker.region.cursors.VMPendingMessageCursor;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.usage.SystemUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import test.messagecursor_copied.AbstractPendingMessageCursor;
import test.messagecursor_copied.QueueStorePrefetch;

/**
 * Slight refactor of {@link SomewhatFair_Copied_StoreQueueCursor}.
 */
public class Cleaned_StoreQueueCursor extends AbstractPendingMessageCursor {

    private static final Logger LOG = LoggerFactory.getLogger(Cleaned_StoreQueueCursor.class);
    private final Broker broker;
    private final Queue queue;
    private PendingMessageCursor nonPersistent;
    private final QueueStorePrefetch persistent;
    private PendingMessageCursor currentCursor;

    /**
     * Construct
     *
     * @param broker
     * @param queue
     */
    public Cleaned_StoreQueueCursor(Broker broker, Queue queue) {
        super((queue != null ? queue.isPrioritizedMessages() : false));
        this.broker = broker;
        this.queue = queue;
        this.persistent = new QueueStorePrefetch(queue, broker);
        currentCursor = persistent;
    }

    @Override
    public synchronized void start() throws Exception {
        // Note: Setting started=true results in parent not creating Audit upon super.start().
        started = true;
        super.start();

        // Transfer properties from us to the persistent sub cursor
        persistent.setMaxBatchSize(getMaxBatchSize());
        persistent.setSystemUsage(systemUsage);
        persistent.setEnableAudit(isEnableAudit());
        persistent.setMaxAuditDepth(getMaxAuditDepth());
        persistent.setMaxProducersToAudit(getMaxProducersToAudit());
        persistent.setUseCache(isUseCache());

        // ?: Has nonPersistent not been set (which would be the common situation)
        if (nonPersistent == null) {
            // -> Not set, so set it now.
            nonPersistent = broker.getBrokerService().isPersistent()
                    ? new FilePendingMessageCursor(broker, queue.getName(), this.prioritizedMessages)
                    : new VMPendingMessageCursor(this.prioritizedMessages);

            // Transfer properties from us to this nonPersistent sub cursor
            nonPersistent.setMaxBatchSize(getMaxBatchSize());
            nonPersistent.setSystemUsage(systemUsage);
            nonPersistent.setEnableAudit(isEnableAudit());
            nonPersistent.setMaxAuditDepth(getMaxAuditDepth());
            nonPersistent.setMaxProducersToAudit(getMaxProducersToAudit());
            nonPersistent.setUseCache(isUseCache());
        }

        // Start both sub cursors - this also creates their Audit if enabled
        nonPersistent.start();
        persistent.start();
    }

    @Override
    public synchronized void stop() throws Exception {
        if (nonPersistent != null) {
            nonPersistent.destroy();
        }
        persistent.stop();
        nonPersistent.stop();
        super.stop();
        started = false;
    }

    @Override
    public synchronized boolean tryAddMessageLast(MessageReference node, long maxWait) throws Exception {
        assertStarted();
        if (node == null) {
            return true;
        }

        if (node.getMessage().isPersistent()) {
            return persistent.tryAddMessageLast(node, maxWait); // Note: ignores maxWait
        }
        else {
            return nonPersistent.tryAddMessageLast(node, maxWait);
        }
    }

    @Override
    public synchronized void addMessageFirst(MessageReference node) throws Exception {
        assertStarted();
        if (node == null) {
            return;
        }

        if (node.getMessage().isPersistent()) {
            persistent.addMessageFirst(node);
        }
        else {
            nonPersistent.addMessageFirst(node);
        }
    }

    @Override
    public synchronized void clear() {
        assertStarted();

        persistent.clear();
        nonPersistent.clear();
    }

    @Override
    public synchronized boolean hasNext() {
        assertStarted();

        // Get opposite cursor:
        PendingMessageCursor oppositeCursor = currentCursor == persistent ? nonPersistent : persistent;
        // ?: Do we have any messages in the opposite?
        if (oppositeCursor.hasNext()) {
            // -> Yes, so do the switch
            currentCursor = oppositeCursor;
        }

        return currentCursor.hasNext();
    }

    @Override
    public synchronized MessageReference next() {
        // No need to 'assertStarted()' due to semantics in how currentCursor can be set.
        return currentCursor.next();
    }

    @Override
    public synchronized void remove() {
        // No need to 'assertStarted()' due to semantics in how currentCursor can be set.
        currentCursor.remove();
    }

    @Override
    public synchronized void remove(MessageReference node) {
        assertStarted();

        if (!node.isPersistent()) {
            nonPersistent.remove(node);
        }
        else {
            persistent.remove(node);
        }
    }

    @Override
    public synchronized void reset() {
        assertStarted();

        nonPersistent.reset();
        persistent.reset();
    }

    @Override
    public void release() {
        assertStarted();

        nonPersistent.release();
        persistent.release();
    }

    @Override
    public void rollback(MessageId id) {
        assertStarted();

        nonPersistent.rollback(id);
        persistent.rollback(id);
    }

    @Override
    public void rebase() {
        assertStarted();

        persistent.rebase();
        nonPersistent.rebase();
        reset();
    }

    @Override
    public synchronized void gc() {
        super.gc(); // no-op
        persistent.gc();
        // ?: Present? (Will be invoked after 'started = false', and thus cannot 'assertStarted()')
        if (nonPersistent != null) {
            nonPersistent.gc();
        }
    }

    @Override
    public synchronized int size() {
        assertStarted();

        return persistent.size() + nonPersistent.size();
    }

    @Override
    public synchronized long messageSize() {
        assertStarted();

        return persistent.messageSize() + nonPersistent.messageSize();
    }

    @Override
    public synchronized boolean isEmpty() {
        assertStarted();

        return persistent.isEmpty() && nonPersistent.isEmpty();
    }


    /**
     * Informs the Broker if the subscription needs to intervention to recover it's state e.g. DurableTopicSubscriber
     * may do
     *
     * @return true if recovery required
     * @see PendingMessageCursor
     */
    @Override
    public boolean isRecoveryRequired() {
        return false;
    }

    @Override
    public boolean isCacheEnabled() {
        assertStarted();

        return persistent.isCacheEnabled() && nonPersistent.isCacheEnabled();
    }

    private void assertStarted() {
        if (!started) {
            throw new IllegalStateException("Not yet started. [" + this + "].");
        }
    }

    /**
     * @return the nonPersistent Cursor
     */
    public PendingMessageCursor getNonPersistent() {
        return nonPersistent;
    }

    /**
     * @param nonPersistent
     *         cursor to set
     */
    public void setNonPersistent(PendingMessageCursor nonPersistent) {
        if (nonPersistent == null) {
            throw new NullPointerException("nonPersistent");
        }
        this.nonPersistent = nonPersistent;
    }

    /**
     * @return the persistent Cursor
     */
    public PendingMessageCursor getPersistent() {
        return persistent;
    }

    @Override
    public void setMaxBatchSize(int maxBatchSize) {
        super.setMaxBatchSize(maxBatchSize);
        persistent.setMaxBatchSize(maxBatchSize);
        // ?: Present yet? - Can be set before start.
        if (nonPersistent != null) {
            nonPersistent.setMaxBatchSize(maxBatchSize);
        }
    }


    @Override
    public void setMaxProducersToAudit(int maxProducersToAudit) {
        super.setMaxProducersToAudit(maxProducersToAudit);
        persistent.setMaxProducersToAudit(maxProducersToAudit);
        // ?: Present yet? - Will be set before start.
        if (nonPersistent != null) {
            nonPersistent.setMaxProducersToAudit(maxProducersToAudit);
        }
    }

    @Override
    public void setMaxAuditDepth(int maxAuditDepth) {
        super.setMaxAuditDepth(maxAuditDepth);
        persistent.setMaxAuditDepth(maxAuditDepth);
        // ?: Present yet? - Will be set before start.
        if (nonPersistent != null) {
            nonPersistent.setMaxAuditDepth(maxAuditDepth);
        }
    }

    @Override
    public void setEnableAudit(boolean enableAudit) {
        super.setEnableAudit(enableAudit);
        persistent.setEnableAudit(enableAudit);
        // ?: Present yet? - Will be set before start.
        if (nonPersistent != null) {
            nonPersistent.setEnableAudit(enableAudit);
        }
    }

    @Override
    public void setUseCache(boolean useCache) {
        super.setUseCache(useCache);
        persistent.setUseCache(useCache);
        // ?: Present yet? - Will be set before start.
        if (nonPersistent != null) {
            nonPersistent.setUseCache(useCache);
        }
    }

    @Override
    public void setMemoryUsageHighWaterMark(int memoryUsageHighWaterMark) {
        super.setMemoryUsageHighWaterMark(memoryUsageHighWaterMark);
        persistent.setMemoryUsageHighWaterMark(memoryUsageHighWaterMark);
        // ?: Present yet? - Will be set before start.
        if (nonPersistent != null) {
            nonPersistent.setMemoryUsageHighWaterMark(memoryUsageHighWaterMark);
        }
    }

    @Override
    public void setSystemUsage(SystemUsage usageManager) {
        super.setSystemUsage(usageManager);
        persistent.setSystemUsage(usageManager);
        // ?: Present yet? - Will be set before start.
        if (nonPersistent != null) {
            nonPersistent.setSystemUsage(usageManager);
        }
    }
}
