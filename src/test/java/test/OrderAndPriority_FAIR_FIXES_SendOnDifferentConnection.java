package test;

import org.apache.activemq.store.kahadb.disk.journal.Journal.JournalDiskSyncStrategy;
import org.junit.Assert;
import test.Util.BrokerAndConnectionFactory;

import javax.jms.Connection;
import javax.jms.Session;


/**
 * @author Endre Stølsvik 2023-06-10 00:16 - http://stolsvik.com/, endre@stolsvik.com
 */
public class OrderAndPriority_FAIR_FIXES_SendOnDifferentConnection extends OrderAndPriority_Base {

    protected void test(int persistenceSet1, int prioritySet1, int persistenceSet2, int prioritySet2,
            int[] expectedOrder) throws Exception {

        BrokerAndConnectionFactory brokerAndConnectionFactory =
                Util.createBroker(JournalDiskSyncStrategy.PERIODIC, 1, 1, true);

        Connection connection1 = brokerAndConnectionFactory.getConnectionFactory().createConnection();
        Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Util.sendMessage(session1, 0, persistenceSet1, prioritySet1);
        Util.sendMessage(session1, 1, persistenceSet1, prioritySet1);
        Util.sendMessage(session1, 2, persistenceSet1, prioritySet1);
        Util.sendMessage(session1, 3, persistenceSet1, prioritySet1);
        connection1.close();

        Connection connection2 = brokerAndConnectionFactory.getConnectionFactory().createConnection();
        Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Util.sendMessage(session2, 4, persistenceSet2, prioritySet2);
        Util.sendMessage(session2, 5, persistenceSet2, prioritySet2);
        Util.sendMessage(session2, 6, persistenceSet2, prioritySet2);
        Util.sendMessage(session2, 7, persistenceSet2, prioritySet2);
        connection2.close();

        int[] indices = Util.receiveMessagesInt(brokerAndConnectionFactory.getConnectionFactory(), 8);

        brokerAndConnectionFactory.closeBroker();
        Assert.assertArrayEquals(expectedOrder, indices);
    }
}
