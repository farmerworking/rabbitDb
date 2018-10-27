package com.farmerworking.db.rabbitDb.impl.sstable;

import com.farmerworking.db.rabbitDb.api.Options;
import com.farmerworking.db.rabbitDb.api.Status;
import com.farmerworking.db.rabbitDb.impl.ByteWiseComparator;
import com.farmerworking.db.rabbitDb.impl.Slice;
import com.farmerworking.db.rabbitDb.impl.utils.ErrorRandomAccessFile;
import com.farmerworking.db.rabbitDb.impl.utils.StringSink;
import com.farmerworking.db.rabbitDb.impl.utils.StringSource;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by John on 18/10/24.
 */
public class TableTest {
    StringSource file;
    Options options;

    @Before
    public void setUp() throws Exception {
        file = new StringSource("");
        options = new Options();
        options.comparator(ByteWiseComparator.getInstance());
    }

    @Test
    public void testOpenSizeLessThanFooterEncodeLength() throws Exception {
        Pair<Status, Table> pair = Table.open(options, file, Footer.ENCODE_LENGTH - 1);
        assertTrue(pair.getLeft().isCorruption());
    }

    @Test
    public void testOpenReadFooterError() throws Exception {
        Pair<Status, Table> pair = Table.open(options, new ErrorRandomAccessFile(), Footer.ENCODE_LENGTH);
        assertTrue(pair.getLeft().isIOError());
    }

    @Test
    public void testOpenMalformedFooter() throws Exception {
        Footer footer = new Footer();
        StringBuilder builder = new StringBuilder();
        footer.setIndexHandle(new BlockHandle(0, 0));
        footer.setMetaIndexHandle(new BlockHandle(0, 0));
        footer.encodeTo(builder);
        int index = builder.length() - 1;
        builder.setCharAt(
                index, (char)(builder.charAt(index) + 1)
        );
        file = new StringSource(builder.toString());

        Pair<Status, Table> pair = Table.open(options, file, Footer.ENCODE_LENGTH);
        assertTrue(pair.getLeft().isCorruption());
    }

    @Test
    public void testIndexBlockChecksumMissMatch1() {
        char[] tableContent = prepareIndexBlockChecksumMissMatchCase();

        Pair<Status, Table> pair = Table.open(options, new StringSource(new String(tableContent)), tableContent.length);
        assertTrue(pair.getLeft().isOk());
    }

    @Test
    public void testIndexBlockChecksumMissMatch2() {
        char[] tableContent = prepareIndexBlockChecksumMissMatchCase();

        options.paranoidChecks(true);
        Pair<Status, Table> pair = Table.open(options, new StringSource(new String(tableContent)), tableContent.length);
        assertTrue(pair.getLeft().isCorruption());
        assertEquals("block checksum mismatch", pair.getLeft().getMessage());
    }

    private char[] prepareIndexBlockChecksumMissMatchCase() {
        StringSink target = new StringSink();
        TableBuilder builder = new TableBuilder(options, target);
        builder.add(new Slice("a"), new Slice("b"));
        Status status = builder.finish();
        assertTrue(status.isOk());

        // modify index block checksum
        char[] tableContent = target.getContent().toCharArray();
        int index = tableContent.length - Footer.ENCODE_LENGTH - 1;
        tableContent[index] = (char)((int)tableContent[index] + 1);
        return tableContent;
    }
}