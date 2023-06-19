package test;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.BaseDestination;
import org.apache.activemq.broker.region.policy.IndividualDeadLetterStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.disk.journal.Journal.JournalDiskSyncStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import test.messagecursor_copied.SomewhatFair_Copied_StorePendingQueueMessageStoragePolicy;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import java.net.InetAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Endre Stølsvik 2023-01-14 17:56 - http://stolsvik.com/, endre@stolsvik.com
 */
public class Util {

    public static final String TEST_QUEUE = "TestQueue";

    private static final Logger log = LoggerFactory.getLogger(Util.class);

    /**
     * Create a ActiveMQ broker with a connected ConnectionFactory.
     *
     * @param syncStrategy
     *         which {@link JournalDiskSyncStrategy} to use, <code>null</code> means non-persistent broker.
     * @param maxPageSize
     *         {@link BaseDestination#MAX_PAGE_SIZE} is 2000
     * @param prefetch
     *         {@link ActiveMQPrefetchPolicy#DEFAULT_QUEUE_PREFETCH} is 1000
     * @param useSomewhatFair_StoreQueueCursor
     *         use the hacked StoreQueueCursor.
     * @return a {@link BrokerAndConnectionFactory} instance, holding a broker and a ConnectionFactory.
     * @throws Exception
     *         if creation of broker throws.
     */
    static BrokerAndConnectionFactory createBroker(JournalDiskSyncStrategy syncStrategy,
            int maxPageSize, int prefetch, boolean useSomewhatFair_StoreQueueCursor) throws Exception {
        // :: CREATING BROKER
        BrokerService brokerService = new BrokerService();
        // brokerService.setBrokerName(BROKER_NAME);
        // Do persistence-adaptor
        if (syncStrategy != null) {
            KahaDBPersistenceAdapter kahaDBPersistenceAdapter = new KahaDBPersistenceAdapter();
            kahaDBPersistenceAdapter.setJournalDiskSyncStrategy(syncStrategy.name());
            // NOTE: This following value is default 1000, i.e. sync interval of 1 sec.
            // Interestingly, setting it to a much lower value, e.g. 10 or 25, seemingly doesn't severely impact
            // performance of the PERIODIC strategy. Thus, instead of potentially losing a full second's worth of
            // messages if someone literally pulled the power cord of the ActiveMQ instance, you'd lose much less.
            kahaDBPersistenceAdapter.setJournalDiskSyncInterval(25);
            brokerService.setPersistenceAdapter(kahaDBPersistenceAdapter);
        }
        else {
            brokerService.setPersistent(false);
        }

        // Make this as network-realistic as possible
        String hostname = InetAddress.getLocalHost().getHostName();

        try {
            TransportConnector connector = new TransportConnector();
            connector.setUri(new URI("nio://" + hostname + ":61617"));
            brokerService.addConnector(connector);
        }
        catch (Exception e) {
            throw new IllegalStateException(e);
        }

        // :: Set Individual DLQ - which you most definitely should do in production.
        IndividualDeadLetterStrategy individualDeadLetterStrategy = new IndividualDeadLetterStrategy();
        individualDeadLetterStrategy.setQueuePrefix("DLQ.");
        individualDeadLetterStrategy.setTopicPrefix("DLQ.");
        // .. Send expired messages to DLQ (Note: true is default)
        individualDeadLetterStrategy.setProcessExpired(true);
        // .. Also DLQ non-persistent messages
        individualDeadLetterStrategy.setProcessNonPersistent(true);
        individualDeadLetterStrategy.setUseQueueForTopicMessages(true); // true is also default
        individualDeadLetterStrategy.setUseQueueForQueueMessages(true); // true is also default.

        // :: Create destination policy entry for QUEUES:
        PolicyEntry allQueuesPolicy = new PolicyEntry();
        allQueuesPolicy.setDestination(new ActiveMQQueue(">")); // all queues
        // .. add the IndividualDeadLetterStrategy
        allQueuesPolicy.setDeadLetterStrategy(individualDeadLetterStrategy);
        // .. we do use prioritization, and this should ensure that priority information is handled in queue, and
        // persisted to store. Store JavaDoc: "A hint to the store to try recover messages according to priority"
        allQueuesPolicy.setPrioritizedMessages(true);

        // :: Create policy entry for TOPICS:
        PolicyEntry allTopicsPolicy = new PolicyEntry();
        allTopicsPolicy.setDestination(new ActiveMQTopic(">")); // all topics
        // .. add the IndividualDeadLetterStrategy, not sure if that is ever relevant for plain Topics.
        allTopicsPolicy.setDeadLetterStrategy(individualDeadLetterStrategy);
        // .. and prioritization, not sure if that is ever relevant for Topics.
        allTopicsPolicy.setPrioritizedMessages(true);

        /*
         * Set the prefetch to provided argument (this affects the client, unless the client overrides by setting it)
         */
        allQueuesPolicy.setQueuePrefetch(prefetch);
        allTopicsPolicy.setTopicPrefetch(prefetch);


        /*
         * Set the MaxPageSize to provided argument (this is serverside
         */
        allQueuesPolicy.setMaxPageSize(maxPageSize);
        allTopicsPolicy.setMaxPageSize(maxPageSize);


        /*
         * If desired, set the changed 'SomewhatFair_Copied_StoreQueueCursor', which half-way fixes the problem.
         */
        if (useSomewhatFair_StoreQueueCursor) {
            allQueuesPolicy.setPendingQueuePolicy(new SomewhatFair_Copied_StorePendingQueueMessageStoragePolicy());
        }
//        allQueuesPolicy.setPendingQueuePolicy(new VMPendingQueueMessageStoragePolicy());
//        allQueuesPolicy.setPendingQueuePolicy(new StorePendingQueueMessageStoragePolicy());
//        allTopicsPolicy.setPendingSubscriberPolicy(new VMPendingSubscriberMessageStoragePolicy());

        // Memory-stuff
//        brokerService.getSystemUsage().getMemoryUsage().setLimit(100 * 1024 * 1024L);
//        brokerService.getSystemUsage().getStoreUsage().setLimit(1024 * 1024 * 1024 * 100L);
//        brokerService.getSystemUsage().getTempUsage().setLimit(1024 * 1024 * 1024 * 100L);
//        allQueuesPolicy.setMemoryLimit(50 * 1024 * 1024);
//

        // .. create the PolicyMap containing the two destination policies
        PolicyMap policyMap = new PolicyMap();
        policyMap.put(allQueuesPolicy.getDestination(), allQueuesPolicy);
        policyMap.put(allTopicsPolicy.getDestination(), allTopicsPolicy);
        // .. set this PolicyMap containing our PolicyEntry on the broker.
        brokerService.setDestinationPolicy(policyMap);

        // No need for JMX registry.
        brokerService.setUseJmx(false);
        // No need for Advisory Messages.
        brokerService.setAdvisorySupport(false);
        // We'll shut it down ourselves.
        brokerService.setUseShutdownHook(false);
        // Start it
        brokerService.start();

        log.info("getVmConnectorURI: " + brokerService.getVmConnectorURI());
        log.info("getDefaultSocketURIString: " + brokerService.getDefaultSocketURIString());
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerService.getDefaultSocketURIString());
        // :: We're using message priorities
        connectionFactory.setMessagePrioritySupported(true);

        return new BrokerAndConnectionFactoryActiveMqImpl(brokerService, connectionFactory);
    }


    public interface BrokerAndConnectionFactory {
        void closeBroker();

        ActiveMQConnectionFactory getConnectionFactory();
    }


    public static class BrokerAndConnectionFactoryActiveMqImpl implements BrokerAndConnectionFactory {
        private final BrokerService _brokerService;
        private final ActiveMQConnectionFactory _connectionFactory;

        public BrokerAndConnectionFactoryActiveMqImpl(BrokerService brokerService,
                ActiveMQConnectionFactory connectionFactory) {
            _brokerService = brokerService;
            _connectionFactory = connectionFactory;
        }

        @Override
        public void closeBroker() {
            try {
                _brokerService.stop();
            }
            catch (Exception e) {
                throw new IllegalStateException("Could not stop.", e);
            }
        }

        @Override
        public ActiveMQConnectionFactory getConnectionFactory() {
            return _connectionFactory;
        }
    }


    public static void takeNap(int ms) {
        try {
            Thread.sleep(ms);
        }
        catch (InterruptedException e) {
            throw new IllegalStateException("Got interrupted.", e);
        }
    }

    public static void sendMessage(Session session, int index, int deliveryMode, int priority) throws JMSException {
        Queue queue = session.createQueue(Util.TEST_QUEUE);
        MessageProducer producer = session.createProducer(queue);
        Message message = session.createMessage();
        sendMessage(producer, message, index, deliveryMode, priority);
        producer.close();
    }

    public static void sendMessage(MessageProducer producer, Message message, int index, int deliveryMode,
            int priority) throws JMSException {
        String description = "msg";
        description += ":" + index;
        description += ":" + (deliveryMode == DeliveryMode.PERSISTENT ? "PERSISTENT" : "NON_PERSISTENT");
        description += ":" + (priority < 4 ? "LOW" : (priority > 4 ? "HIGH" : "DEFAULT")) + "[" + priority + "]";
        message.setStringProperty("description", description);
        message.setIntProperty("index", index);
        producer.send(message, deliveryMode, priority, 0);
    }

    public static void clearQueue() throws Exception {
        BrokerAndConnectionFactory broker = createBroker(JournalDiskSyncStrategy.PERIODIC, 2000, 2000, false);
        Connection connection = broker.getConnectionFactory().createConnection();
        try {
            int receivedMessages = 0;
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(TEST_QUEUE);
            MessageConsumer consumer = session.createConsumer(queue);
            do {
                Message msg = consumer.receive(100);
                if (msg == null) {
                    log.info("Cleared messages! - [" + receivedMessages + "] cleared.");
                    return;
                }
                log.info("Cleared message: " + msg.getStringProperty("description") + msg);
            } while (++receivedMessages != 10_000);
            throw new AssertionError("Didn't stop getting messages!");
        }
        finally {
            connection.close();
            broker.closeBroker();
        }
    }

    public static List<Message> receiveMessages(ConnectionFactory connectionFactory,
            int numMessages) throws JMSException {
        takeNap(500);

        List<Message> messages = new ArrayList<>(numMessages);
        Connection connection = connectionFactory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(TEST_QUEUE);
        MessageConsumer consumer = session.createConsumer(queue);
        int receivedMessages = 0;
        do {
            Message msg = consumer.receive(60_000);
            if (msg == null) {
                break;
            }
            log.info("Received message: " + msg.getStringProperty("description") + msg);
            messages.add(msg);
        } while (++receivedMessages != numMessages);

        if (messages.size() == numMessages) {
            log.info("Received all expected [" + numMessages + "]...");
        }
        else {
            throw new AssertionError("Didn't receive expected number of messages [" + numMessages
                    + "]: " + messages);
        }

        connection.close();

        for (Message message : messages) {
            System.out.println(message.getIntProperty("index") + " - "
                    + message.getStringProperty("description")
                    + " [" + message.getJMSMessageID() + "]");
        }

        return messages;
    }

    public static int[] receiveMessagesInt(ConnectionFactory connectionFactory,
            int numMessages) throws JMSException {
        return receiveMessages(connectionFactory, numMessages).stream()
                .mapToInt(message -> {
                    try {
                        return message.getIntProperty("index");
                    }
                    catch (JMSException e) {
                        throw new RuntimeException("damn");
                    }
                })
                .toArray();
    }
}
