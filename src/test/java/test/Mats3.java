package test;

import io.mats3.MatsInitiator.MatsInitiate;
import io.mats3.impl.jms.JmsMatsFactory;
import io.mats3.impl.jms.JmsMatsJmsSessionHandler_Pooling;
import io.mats3.serial.json.MatsSerializerJson;
import io.mats3.test.MatsTestHelp;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.store.kahadb.disk.journal.Journal.JournalDiskSyncStrategy;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import test.Util.BrokerAndConnectionFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Endre Stølsvik 2023-06-10 02:15 - http://stolsvik.com/, endre@stolsvik.com
 */
public class Mats3 {
    private static final Logger log = MatsTestHelp.getClassLogger();

    @Test
    public void onlyPersistentWithPrioritized_ok() throws Exception {
        doTest(false, 1, false);
    }

    @Test
    public void FAIL_mixedNonPersistentWithPrioritized_fail() throws Exception {
        doTest(true, 1, false);
    }

    @Test
    public void PREFETCH_MASKS_PROBLEM_mixedNonPersistentWithPrioritized() throws Exception {
        doTest(true, 1000, false);
    }

    @Test
    public void FAIR_CURSOR_FIXES_mixedNonPersistentWithPrioritized() throws Exception {
        doTest(true, 1, true);
    }

    private static final String TERMINATOR = MatsTestHelp.terminator();

    public void doTest(boolean useNonPersistentForInteractive, int prefetch, boolean useSomewhatFair_StoreQueueCursor) throws Exception {

        // :: ARRANGE

        BrokerAndConnectionFactory broker = Util.createBroker(JournalDiskSyncStrategy.PERIODIC, 200, prefetch, useSomewhatFair_StoreQueueCursor);
        ActiveMQConnectionFactory connectionFactory = broker.getConnectionFactory();
        JmsMatsFactory<String> matsFactory = JmsMatsFactory.createMatsFactory_JmsOnlyTransactions("test", "#test#",
                JmsMatsJmsSessionHandler_Pooling.create(connectionFactory), MatsSerializerJson.create());

        matsFactory.getFactoryConfig().setConcurrency(4);


        int interactiveMessages = 1000;
        int standardMessages = 3000;

        CountDownLatch interactive_countDown = new CountDownLatch(interactiveMessages);
        CountDownLatch standard_countDown = new CountDownLatch(standardMessages);

        matsFactory.terminator(TERMINATOR, StateTO.class, DataTO.class, (context, sto, dto) -> {
            Util.takeNap(1);
            if (context.isInteractive()) {
                log.info("## INTERACTIVE! " + dto.number + " - Interactive:" + context.isInteractive());
                interactive_countDown.countDown();
            }
            else {
                log.info("-- standard! " + dto.number + " - Interactive:" + context.isInteractive());
                standard_countDown.countDown();
            }
        });

        // :: ACT

        // : First send standard, then send interactive:
        // Standard
        matsFactory.getDefaultInitiator().initiateUnchecked(msg1 -> {
            for (int i1 = 0; i1 < standardMessages; i1++) {
                DataTO dto1 = new DataTO(i1, "Standard");
                sendMessage(false, false, dto1, msg1, TERMINATOR);
            }
            log.info("SENT: Standard messages!");
        });

        // Interactive:
        matsFactory.getDefaultInitiator().initiateUnchecked(msg -> {
            for (int i = 0; i < interactiveMessages; i++) {
                DataTO dto = new DataTO(i, "INTERACTIVE");
                sendMessage(true, useNonPersistentForInteractive, dto, msg, TERMINATOR);
            }
            log.info("SENT: INTERACTIVE messages!");
        });

        // :: WAIT FOR RECEPTION

        AtomicLong timestampInteractiveFinished = new AtomicLong();
        AtomicLong timestampStandardFinished = new AtomicLong();

        new Thread(() -> {
            try {
                interactive_countDown.await();
                timestampInteractiveFinished.set(System.currentTimeMillis());
                log.info("ALL INTERACTIVE CAME THOUGH!");
            }
            catch (InterruptedException e) {
            }
        }, "interactive_waiter").start();
        new Thread(() -> {
            try {
                standard_countDown.await();
                timestampStandardFinished.set(System.currentTimeMillis());
                log.info("ALL STANDARD CAME THOUGH!");
            }
            catch (InterruptedException e) {
            }
        }, "standard_waiter").start();

        interactive_countDown.await();
        standard_countDown.await();

        matsFactory.close();
        broker.closeBroker();

        // :: ASSERT

        // Let the setting go through.
        Util.takeNap(100);

        log.info("timestampInteractiveFinished:[" + timestampInteractiveFinished + "]," + " timestampStandardFinished:[" + timestampStandardFinished + "]");

        Assert.assertTrue("Interactive should complete before standard.", timestampInteractiveFinished.get() < timestampStandardFinished.get());
    }

    private void sendMessage(boolean interactive, boolean nonPersistent, DataTO dto,  MatsInitiate msg, String queue) {
        if (interactive) {
            msg.interactive();
        }
        if (nonPersistent) {
            msg.nonPersistent();
        }

        msg.traceId(MatsTestHelp.traceId()).from(MatsTestHelp.from("test")).to(queue).send(dto);
    }

    record DataTO(int number, String string) {
    }

    record StateTO(int number1, double number2) {
    }
}
