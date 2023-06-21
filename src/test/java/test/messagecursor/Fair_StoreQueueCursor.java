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
 * <p/>
 * This variant performs a .next() on both sub cursors, and then compares the two for which to return from calls to its
 * .next(). It constantly keeps a message from each of the sub cursors - fetching any missing on .hasNext(). In .next(),
 * it compares the two. An assumption is done wrt. when a message is removed from the actual store: Hopefully this does
 * not happen until the .remove() method is invoked on the sub cursor.
 * <p/>
 * Note: This directly implements {@link PendingMessageCursor}, not inheriting from 'AbstractPendingMessageCursor', the
 * latter is "collapsed" into this to get some understanding of how it actually works.
 */
public class Fair_StoreQueueCursor implements PendingMessageCursor {

    private static final Logger LOG = LoggerFactory.getLogger(Fair_StoreQueueCursor.class);
    private final Broker broker;
    private final Queue queue;
    private PendingMessageCursor nonPersistent;
    private QueueStorePrefetch persistent;

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

    public Fair_StoreQueueCursor(Broker broker, Queue queue) {
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

    /**
     * destroy the cursor
     *
     * @throws Exception
     */
    @Override
    public void destroy() throws Exception {
        stop();
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

        // :: Also null out our "heads" from the cursors
        _fromPersistent = null;
        _fromNonPersistent = null;
    }


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
        // Note: Semantically, one should invoke .hashNext() in front of each .next(). The null-return situation shall
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
    public boolean hasMessagesBufferedToDeliver() {
        assertStarted();

        return persistent.hasMessagesBufferedToDeliver() || nonPersistent.hasMessagesBufferedToDeliver();
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

    // Remains from AbstractPendingMessageCursor

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
        return systemUsage;
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
