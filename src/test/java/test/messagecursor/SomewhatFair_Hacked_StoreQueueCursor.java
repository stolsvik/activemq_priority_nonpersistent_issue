package test.messagecursor;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.region.cursors.PendingMessageCursor;
import org.apache.activemq.broker.region.cursors.StoreQueueCursor;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;

/**
 * An extension of {@link StoreQueueCursor} which must use reflection on the private fields to achieve its goal of
 * overriding the {@link #getNextCursor()} method.
 * <p>
 * A better alternative would be to re-implement the entire class. This was a tad problematic due to needed classes and
 * methods being package-private, but by copying 4 classes out, it worked: {@link SomewhatFair_Copied_StoreQueueCursor}.
 */
public class SomewhatFair_Hacked_StoreQueueCursor extends StoreQueueCursor {
    private static final VarHandle CURRENT_CURSOR_FIELD;
    private static final VarHandle NON_PERSISTENT_FIELD;
    private static final MethodHandle PERSISTENT_FIELD_GETTER;

    static {
        try {
            NON_PERSISTENT_FIELD = MethodHandles.privateLookupIn(StoreQueueCursor.class, MethodHandles.lookup())
                    .findVarHandle(StoreQueueCursor.class, "nonPersistent", PendingMessageCursor.class);
            CURRENT_CURSOR_FIELD = MethodHandles.privateLookupIn(StoreQueueCursor.class, MethodHandles.lookup())
                    .findVarHandle(StoreQueueCursor.class, "currentCursor", PendingMessageCursor.class);

            Field persistent = StoreQueueCursor.class.getDeclaredField("persistent");
            persistent.setAccessible(true);
            Lookup lookup = MethodHandles.lookup();
            PERSISTENT_FIELD_GETTER = lookup.unreflectGetter(persistent);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new AssertionError("Cant reflect.", e);
        }
    }

    public SomewhatFair_Hacked_StoreQueueCursor(Broker broker, org.apache.activemq.broker.region.Queue queue) {
        super(broker, queue);
    }


    protected synchronized PendingMessageCursor getNextCursor() throws Exception {
        // ?: Sanity check that nonPersistent has been set, i.e. that start() has been invoked.
        PendingMessageCursor nonPersistent = (PendingMessageCursor) NON_PERSISTENT_FIELD.get(this);
        if (nonPersistent == null) {
            // -> No, not set, so don't evaluate switching - so that currentCursor never becomes null.
            return (PendingMessageCursor) CURRENT_CURSOR_FIELD.get(this);
        }

        // :: Get opposite cursor:
        PendingMessageCursor persistent;
        try {
            persistent = (PendingMessageCursor) PERSISTENT_FIELD_GETTER.invoke(this);
        }
        catch (Throwable e) {
            throw new AssertionError("Couldn't invoke getter of field 'StoreQueueCursor.persistent'.", e);
        }
        PendingMessageCursor currentCursor = (PendingMessageCursor) CURRENT_CURSOR_FIELD.get(this);
        // .. actual oppositeCursor resolving:
        PendingMessageCursor oppositeCursor = currentCursor == persistent ? nonPersistent : persistent;

        // ?: Do we have any messages in the opposite?
        if (oppositeCursor.hasNext()) {
            // -> Yes, so do the switch
            CURRENT_CURSOR_FIELD.set(this, oppositeCursor);
            System.out.println("Swithced to: " + oppositeCursor);
            return oppositeCursor;
        }
        else {
            System.out.println("DID NOT switch, keeping: " + currentCursor);
        }
        // E-> Return existing current
        return currentCursor;
    }
}
