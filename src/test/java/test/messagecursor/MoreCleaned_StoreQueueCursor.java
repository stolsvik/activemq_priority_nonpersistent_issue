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

import org.apache.activemq.ActiveMQMessageAudit;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.BaseDestination;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.cursors.FilePendingMessageCursor;
import org.apache.activemq.broker.region.cursors.PendingMessageCursor;
import org.apache.activemq.broker.region.cursors.VMPendingMessageCursor;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.usage.SystemUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import test.messagecursor_copied.QueueStorePrefetch;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * PendingMessageCursor for use on queues, made up of two "sub cursors" - one QueueStorePrefetch for persistent
 * messages, and another for nonPersistent, which is either FilePendingMessageCursor or VMPendingMessageCursor.
 * <p>
 * Note: We do audit in this class, not in the two delegated cursors/prefetch
 */
public class MoreCleaned_StoreQueueCursor implements PendingMessageCursor {

    private static final Logger LOG = LoggerFactory.getLogger(MoreCleaned_StoreQueueCursor.class);
    private final Broker broker;
    private final Queue queue;
    private PendingMessageCursor nonPersistent;
    private QueueStorePrefetch persistent;
    private PendingMessageCursor currentCursor;

    // ---------------

    protected final boolean prioritizedMessages;
    protected boolean started = false;
    protected int memoryUsageHighWaterMark = 70;
    protected int maxBatchSize = BaseDestination.MAX_PAGE_SIZE;
    protected SystemUsage systemUsage;
    protected int maxProducersToAudit = BaseDestination.MAX_PRODUCERS_TO_AUDIT;
    protected int maxAuditDepth = BaseDestination.MAX_AUDIT_DEPTH;
    protected boolean enableAudit = true;
    protected ActiveMQMessageAudit audit;
    protected boolean useCache = true;

    public MoreCleaned_StoreQueueCursor(Broker broker, Queue queue) {
        this.prioritizedMessages = (queue != null && queue.isPrioritizedMessages());
        this.broker = broker;
        this.queue = queue;
    }

    @Override
    public synchronized void start() throws Exception {
        // Set the Audit and init props if not explicitly invoked before.
        setEnableAudit(isEnableAudit());

        // ?: Have we already started it?
        if (!started) {
            persistent = new QueueStorePrefetch(queue, broker);
            // Transfer properties from us to the persistent sub cursor
            persistent.setMaxBatchSize(getMaxBatchSize());
            persistent.setSystemUsage(getSystemUsage());
            persistent.setUseCache(isUseCache());
            // First set the message audit, then enable it - otherwise, the sub cursor first makes its own.
            persistent.setMessageAudit(getMessageAudit());
            persistent.setEnableAudit(isEnableAudit());

            // ?: Has nonPersistent not been set (which would be the common situation)
            if (nonPersistent == null) {
                // -> Not set, so set it now. (Setter will transfer properties from us to it).
                setNonPersistent(broker.getBrokerService().isPersistent()
                        ? new FilePendingMessageCursor(broker, queue.getName(), this.prioritizedMessages)
                        : new VMPendingMessageCursor(this.prioritizedMessages));

                // Transfer properties from us to this nonPersistent sub cursor
                nonPersistent.setMaxBatchSize(getMaxBatchSize());
                nonPersistent.setSystemUsage(getSystemUsage());
                nonPersistent.setUseCache(isUseCache());
                // First set the message audit, then enable it - otherwise, the sub cursor first makes its own.
                nonPersistent.setMessageAudit(getMessageAudit());
                nonPersistent.setEnableAudit(isEnableAudit());
            }
        }


        // Start both sub cursors - this also creates their Audit if enabled
        nonPersistent.start();
        persistent.start();

        // Start with persistent as current cursor.
        currentCursor = persistent;

        started = true;
    }

    @Override
    public synchronized void stop() throws Exception {
        assertStarted();

        gc(); // Gc's both
        persistent.stop();
        nonPersistent.destroy();
        nonPersistent.stop();
        started = false;
    }

    @Override
    public synchronized boolean tryAddMessageLast(MessageReference node, long maxWait) throws Exception {
        assertStarted();
        if (node == null) {
            return true;
        }

        // :: Add to correct sub cursor
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

        // :: Add to correct sub cursor
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
    public void rebase() {
        assertStarted();

        persistent.rebase();
        nonPersistent.rebase();
        reset();
    }

    @Override
    public synchronized void gc() {
        assertStarted();

        persistent.gc();
        nonPersistent.gc();
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

    @Override
    public boolean hasMessagesBufferedToDeliver() {
        return persistent.hasMessagesBufferedToDeliver() && nonPersistent.hasMessagesBufferedToDeliver();
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
     *         cursor to set - Note: no setting of properties (MaxBatchSize, SystemUsage, UseCache), you must do that.
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
        this.maxBatchSize = maxBatchSize;
        // ?: Present yet? - Can be set before start.
        if (started) {
            persistent.setMaxBatchSize(maxBatchSize);
            nonPersistent.setMaxBatchSize(maxBatchSize);
        }
    }


    @Override
    public void setUseCache(boolean useCache) {
        this.useCache = useCache;
        // ?: Present yet? - Will be set before start.
        if (started) {
            persistent.setUseCache(useCache);
            nonPersistent.setUseCache(useCache);
        }
    }

    /**
     * @param memoryUsageHighWaterMark
     *         the memoryUsageHighWaterMark to set
     */
    @Override
    public void setMemoryUsageHighWaterMark(int memoryUsageHighWaterMark) {
        this.memoryUsageHighWaterMark = memoryUsageHighWaterMark;
        // ?: Present yet? - Will be set before start.
        if (started) {
            persistent.setMemoryUsageHighWaterMark(memoryUsageHighWaterMark);
            nonPersistent.setMemoryUsageHighWaterMark(memoryUsageHighWaterMark);
        }
    }

    @Override
    public void setSystemUsage(SystemUsage usageManager) {
        this.systemUsage = usageManager;
        // ?: Present yet? - Will be set before start.
        if (started) {
            persistent.setSystemUsage(usageManager);
            nonPersistent.setSystemUsage(usageManager);
        }
    }


    // ==============================================================


    @Override
    public void add(ConnectionContext context, Destination destination) throws Exception {
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<MessageReference> remove(ConnectionContext context, Destination destination) throws Exception {
        return Collections.EMPTY_LIST;
    }

    @Override
    public boolean addMessageLast(MessageReference node) throws Exception {
        return tryAddMessageLast(node, INFINITE_WAIT);
    }

    @Override
    public void addRecoveredMessage(MessageReference node) throws Exception {
        addMessageLast(node);
    }

    @Override
    public boolean isEmpty(Destination destination) {
        return isEmpty();
    }

    @Override
    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    @Override
    public void resetForGC() {
        reset();
    }

    @Override
    public boolean hasSpace() {
        // allow isFull to verify parent usage and otherwise enforce local memoryUsageHighWaterMark
        return systemUsage == null || (!isParentFull() && systemUsage.getMemoryUsage().getPercentUsage() < memoryUsageHighWaterMark);
    }

    private boolean isParentFull() {
        boolean result = false;
        if (systemUsage != null) {
            if (systemUsage.getMemoryUsage().getParent() != null) {
                return systemUsage.getMemoryUsage().getParent().getPercentUsage() >= 100;
            }
        }
        return result;
    }

    @Override
    public boolean isFull() {
        return systemUsage != null && systemUsage.getMemoryUsage().isFull();
    }

    /**
     * @return the memoryUsageHighWaterMark
     */
    @Override
    public int getMemoryUsageHighWaterMark() {
        return memoryUsageHighWaterMark;
    }

    /**
     * @return the usageManager
     */
    @Override
    public SystemUsage getSystemUsage() {
        return this.systemUsage;
    }

    /**
     * destroy the cursor
     *
     * @throws Exception
     */
    @Override
    public void destroy() throws Exception {
        stop();
    }

    /**
     * Page in a restricted number of messages
     *
     * @param maxItems
     *         maximum number of messages to return
     * @return a list of paged in messages
     */
    @Override
    public LinkedList<MessageReference> pageInList(int maxItems) {
        throw new RuntimeException("Not supported");
    }

    @Override
    public boolean isTransient() {
        return false;
    }

    @Override
    public boolean isUseCache() {
        return useCache;
    }


    // ===== Audit stuff. Forwarding to delegates - they share the one single Audit instance.

    /**
     * @param enableAudit
     *         whether to enable audit - will set previous set props on audit.
     */
    @Override
    public synchronized void setEnableAudit(boolean enableAudit) {
        this.enableAudit = enableAudit;
        if (enableAudit && audit == null) {
            audit = new ActiveMQMessageAudit(maxAuditDepth, maxProducersToAudit);
            // Set any previous set props
            audit.setAuditDepth(maxAuditDepth);
            audit.setMaximumNumberOfProducersToTrack(maxProducersToAudit);
        }
        if (!enableAudit && audit != null) {
            audit.clear();
            audit = null;
        }

        // "Forward" the setting of audit to sub-cursors
        setMessageAudit(audit);
    }

    /**
     * @return the enableAudit
     */
    @Override
    public synchronized boolean isEnableAudit() {
        return enableAudit;
    }

    /**
     * set the audit - Note: no setting of AuditDepth nor MaximumNumberOfProducersToTrack.
     *
     * @param audit
     *         new audit component
     */
    @Override
    public synchronized void setMessageAudit(ActiveMQMessageAudit audit) {
        this.audit = audit;
        // :: If we've started, then forward.
        if (started) {
            persistent.setMessageAudit(audit);
            nonPersistent.setMessageAudit(audit);
        }
    }

    /**
     * @return the audit
     */
    @Override
    public synchronized ActiveMQMessageAudit getMessageAudit() {
        return audit;
    }

    /**
     * @param maxProducersToAudit
     *         the maxProducersToAudit to set
     */
    @Override
    public synchronized void setMaxProducersToAudit(int maxProducersToAudit) {
        this.maxProducersToAudit = maxProducersToAudit;
        // ?: Have we created the audit already?
        if (audit != null) {
            // -> Yes, so set it on it.
            audit.setMaximumNumberOfProducersToTrack(maxProducersToAudit);
        }
    }

    /**
     * @return the maxProducersToAudit
     */
    @Override
    public synchronized int getMaxProducersToAudit() {
        return maxProducersToAudit;
    }

    /**
     * @param maxAuditDepth
     *         the maxAuditDepth to set
     */
    @Override
    public synchronized void setMaxAuditDepth(int maxAuditDepth) {
        this.maxAuditDepth = maxAuditDepth;
        // ?: Have we created the audit already?
        if (audit != null) {
            // -> Yes, so set it on it.
            audit.setAuditDepth(maxAuditDepth);
        }
    }

    /**
     * @return the maxAuditDepth
     */
    @Override
    public synchronized int getMaxAuditDepth() {
        return maxAuditDepth;
    }

    @Override
    public synchronized void rollback(MessageId id) {
        // :: If we have audit, and are started, then forward.
        if ((audit != null) && started) {
            persistent.rollback(id);
            nonPersistent.rollback(id);
        }
    }
}
