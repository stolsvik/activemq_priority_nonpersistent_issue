package test;

import org.apache.activemq.store.kahadb.disk.journal.Journal.JournalDiskSyncStrategy;
import org.junit.BeforeClass;
import org.junit.Test;
import test.Util.BrokerAndConnectionFactory;

import javax.jms.DeliveryMode;

/**
 * @author Endre Stølsvik 2023-06-21 14:01 - http://stolsvik.com/, endre@stolsvik.com
 */
public abstract class OrderAndPriority_Base {

    // ----- The common abstract test method.

    protected abstract void test(int persistenceSet1, int prioritySet1, int persistenceSet2, int prioritySet2,
            int[] expectedOrder) throws Exception;

    @BeforeClass
    public static void clearQueue() throws Exception {
        Util.clearQueue();
    }

    /*
     * Ordering: Keeps order, unless, except NON-PERSISTENT before PERSISTENT
     *
     * !!PERSISTENT takes precedence, even though NON_PERSISTENCE was sent first!!
     */

    // === Works when using same persistence modes!

    @Test
    public void inOrder_allPersistent_Good() throws Exception {
        test(DeliveryMode.PERSISTENT, 4, DeliveryMode.PERSISTENT, 4, new int[]{0, 1, 2, 3, 4, 5, 6, 7});
    }

    @Test
    public void inOrder_allNonPersistent_Good() throws Exception {
        test(DeliveryMode.NON_PERSISTENT, 4, DeliveryMode.NON_PERSISTENT, 4, new int[]{0, 1, 2, 3, 4, 5, 6, 7});
    }

    // .. and "works" if persistent comes first, as persistent will get precedence anyway!

    @Test
    public void inOrder_persistentBeforeNonPersistent_Good() throws Exception {
        test(DeliveryMode.PERSISTENT, 4, DeliveryMode.NON_PERSISTENT, 4, new int[]{0, 1, 2, 3, 4, 5, 6, 7});
    }

    // .. BUT, if we put the NON_PERSISTENT for set 1, and then PERSISTENT for set 2: Persistent takes precedence.

    @Test
    public void FAIL_inOrder_nonPersistentBeforePersistent_AllDefaultPri4_Fails() throws Exception {
        test(DeliveryMode.NON_PERSISTENT, 4, DeliveryMode.PERSISTENT, 4, new int[]{0, 1, 2, 3, 4, 5, 6, 7});
    }


    /*
     * Introduce differing prioritization - higher pri should come first, even if sent last!
     *
     * !!PERSISTENT takes precedence, even though NON_PERSISTENCE has higher priority!
     */

    // === Works when using same persistence modes!

    @Test
    public void prioritizationSentDifferentOrder_AllPersistent_0_9_Good() throws Exception {
        test(DeliveryMode.PERSISTENT, 0, DeliveryMode.PERSISTENT, 9, new int[]{4, 5, 6, 7, 0, 1, 2, 3});
    }

    @Test
    public void prioritizationSentDifferentOrder_AllNonPersistent_0_9_Good() throws Exception {
        test(DeliveryMode.NON_PERSISTENT, 0, DeliveryMode.NON_PERSISTENT, 9, new int[]{4, 5, 6, 7, 0, 1, 2, 3});
    }


    /*
     * .. BUT, fails if persistent comes first, as that takes "absolute precedence" over non-persistent.
     */

    @Test
    public void FAIL_prioritizationSentDifferentOrder_persistentBeforeNonPersistent_0_9_Fails() throws Exception {
        test(DeliveryMode.PERSISTENT, 0, DeliveryMode.NON_PERSISTENT, 9, new int[]{4, 5, 6, 7, 0, 1, 2, 3});
    }

    /*
     * .. it "works" if persistence is the high-pri set, because persistent takes precedence anyway.
     */

    @Test
    public void prioritizationSentDifferentOrder_nonPersistentBeforePersistent_0_9_Good() throws Exception {
        test(DeliveryMode.NON_PERSISTENT, 0, DeliveryMode.PERSISTENT, 9, new int[]{4, 5, 6, 7, 0, 1, 2, 3});
    }
}
