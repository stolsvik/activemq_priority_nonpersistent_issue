package test;

import org.apache.activemq.store.kahadb.disk.journal.Journal.JournalDiskSyncStrategy;
import org.junit.Assert;
import test.Util.BrokerAndConnectionFactory;

import javax.jms.Connection;
import javax.jms.Session;


/**
 * @author Endre Stølsvik 2023-06-08 23:33 - http://stolsvik.com/, endre@stolsvik.com
 */
public class OrderAndPriority_FAIR_FIXES_SendOnSameConnection extends OrderAndPriority_Base {

    @Override
    protected void test(int persistenceSet1, int prioritySet1, int persistenceSet2, int prioritySet2,
            int[] expectedOrder) throws Exception {

        BrokerAndConnectionFactory brokerAndConnectionFactory =
                Util.createBroker(JournalDiskSyncStrategy.PERIODIC, 1, 1, true);

        Connection connection = brokerAndConnectionFactory.getConnectionFactory().createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Util.sendMessage(session, 0, persistenceSet1, prioritySet1);
        Util.sendMessage(session, 1, persistenceSet1, prioritySet1);
        Util.sendMessage(session, 2, persistenceSet1, prioritySet1);
        Util.sendMessage(session, 3, persistenceSet1, prioritySet1);

        Util.sendMessage(session, 4, persistenceSet2, prioritySet2);
        Util.sendMessage(session, 5, persistenceSet2, prioritySet2);
        Util.sendMessage(session, 6, persistenceSet2, prioritySet2);
        Util.sendMessage(session, 7, persistenceSet2, prioritySet2);

        int[] indices = Util.receiveMessagesInt(brokerAndConnectionFactory.getConnectionFactory(), 8);
        connection.close();
        brokerAndConnectionFactory.closeBroker();

        Assert.assertArrayEquals(expectedOrder, indices);
    }
}
