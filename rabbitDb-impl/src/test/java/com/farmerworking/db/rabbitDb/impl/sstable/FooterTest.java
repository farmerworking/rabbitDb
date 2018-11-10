package com.farmerworking.db.rabbitDb.impl.sstable;

import com.farmerworking.db.rabbitDb.api.Status;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by John on 18/10/15.
 */
public class FooterTest {
    @Test
    public void testSimple() throws Exception {
        Footer footer = new Footer();

        footer.setIndexHandle(new BlockHandle(123, 165465464));
        footer.setMetaIndexHandle(new BlockHandle(46464954, 456));

        String encode = footer.encode();

        Footer another = new Footer();
        assertNull(another.getIndexHandle());
        assertNull(another.getMetaIndexHandle());

        Status status = another.decodeFrom(encode);
        assertTrue(status.isOk());
        assertEquals(another.getIndexHandle().getOffset(), 123l);
        assertEquals(another.getIndexHandle().getSize(), 165465464l);

        assertEquals(another.getMetaIndexHandle().getOffset(), 46464954l);
        assertEquals(another.getMetaIndexHandle().getSize(), 456l);
    }
}