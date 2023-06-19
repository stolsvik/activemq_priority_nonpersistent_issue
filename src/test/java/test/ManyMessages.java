package test;

import io.mats3.test.MatsTestHelp;
import org.apache.activemq.store.kahadb.disk.journal.Journal.JournalDiskSyncStrategy;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import test.Util.BrokerAndConnectionFactory;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Session;

/**
 * Mostly for "visual inspection" of the resulting console output.
 *
 * @author Endre Stølsvik 2023-06-10 21:11 - http://stolsvik.com/, endre@stolsvik.com
 */
public class ManyMessages {
    private static final Logger log = MatsTestHelp.getClassLogger();

    @BeforeClass
    public static void clearQueue() throws Exception {
        Util.clearQueue();
    }

    @Test
    public void test() throws Exception {
        runTest(DeliveryMode.PERSISTENT, 4, DeliveryMode.NON_PERSISTENT, 9);
    }

    // ----- The common test method.

    private void runTest(int persistenceSet1, int prioritySet1, int persistenceSet2, int prioritySet2) throws Exception {
        BrokerAndConnectionFactory brokerAndConnectionFactory = Util.createBroker(JournalDiskSyncStrategy.PERIODIC, 10, 1, false);

        int standardMessages = 5000;
        int interactiveMessages = 1000;

        Connection connection1 = brokerAndConnectionFactory.getConnectionFactory().createConnection();
        Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);

        for (int i = 0; i < standardMessages; i++) {
            Util.sendMessage(session1, i, persistenceSet1, prioritySet1);
        }
        log.info("SENT: Standard messages!");

        for (int i = 0; i < interactiveMessages; i++) {
            Util.sendMessage(session1, i, persistenceSet2, prioritySet2);
        }
        log.info("SENT: INTERACTIVE messages!");

        int[] indices = Util.receiveMessagesInt(brokerAndConnectionFactory.getConnectionFactory(), standardMessages + interactiveMessages);

        connection1.close();
        brokerAndConnectionFactory.closeBroker();
    }
}
