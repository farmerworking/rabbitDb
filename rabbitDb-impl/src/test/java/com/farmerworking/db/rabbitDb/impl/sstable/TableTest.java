package com.farmerworking.db.rabbitDb.impl.sstable;

import com.farmerworking.db.rabbitDb.api.Options;
import com.farmerworking.db.rabbitDb.api.Status;
import com.farmerworking.db.rabbitDb.impl.BloomFilterPolicy;
import com.farmerworking.db.rabbitDb.impl.ByteWiseComparator;
import com.farmerworking.db.rabbitDb.api.Slice;
import com.farmerworking.db.rabbitDb.impl.utils.ErrorRandomAccessFile;
import com.farmerworking.db.rabbitDb.impl.utils.StringSink;
import com.farmerworking.db.rabbitDb.impl.utils.StringSource;
import com.farmerworking.db.rabbitDb.impl.utils.TestHashFilter;
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

    @Test
    public void testEmptyMetaIndex() throws Exception {
        options.filterPolicy(new BloomFilterPolicy(10));
        Table table = new Table(options, new Block(new Slice("")), new StringSource(""));

        Footer footer = new Footer();
        footer.setMetaIndexHandle(new BlockHandle(1000, 0));

        table.readMeta(footer);
        assertNull(table.filter);
    }

    @Test
    public void testMetaIndexReadBlockError() throws Exception {
        options.filterPolicy(new BloomFilterPolicy(10));
        Table table = new Table(options, new Block(new Slice("")), new StringSource(""));

        table.setFile(new ErrorRandomAccessFile());
        Footer footer = new Footer();
        footer.setMetaIndexHandle(new BlockHandle(1000, 1000));

        table.readMeta(footer);
        assertNull(table.filter);
    }

    @Test
    public void testFilterNotMatch() throws Exception {
        options.filterPolicy(new BloomFilterPolicy(10));
        StringSink target = new StringSink();
        TableBuilder builder = new TableBuilder(options, target);
        builder.add(new Slice("a"), new Slice("b"));
        builder.finish();

        options.filterPolicy(new TestHashFilter());
        Pair<Status, Table> pair = Table.open(options, new StringSource(target.getContent()), target.getContent().length());
        assertTrue(pair.getLeft().isOk());
        assertNull(pair.getRight().filter);
    }

    @Test
    public void testFilterBlockDecodeError() throws Exception {
        options.filterPolicy(new BloomFilterPolicy(10));
        StringSink target = new StringSink();
        TableBuilder builder = new TableBuilder(options, target);
        builder.add(new Slice("a"), new Slice("b"));
        builder.finish();

        options.filterPolicy(null);
        Pair<Status, Table> pair = Table.open(options, new StringSource(target.getContent()), target.getContent().length());
        assertTrue(pair.getLeft().isOk());
        assertNull(pair.getRight().filter);

        Table table = pair.getRight();
        table.readFilter(new Slice(""));
        assertNull(pair.getRight().filter);
    }

    @Test
    public void testFilterReadBlockError() throws Exception {
        options.filterPolicy(new BloomFilterPolicy(10));
        StringSink target = new StringSink();
        TableBuilder builder = new TableBuilder(options, target);
        builder.add(new Slice("a"), new Slice("b"));
        builder.finish();

        options.filterPolicy(null);
        Pair<Status, Table> pair = Table.open(options, new StringSource(target.getContent()), target.getContent().length());
        assertTrue(pair.getLeft().isOk());
        assertNull(pair.getRight().filter);

        Table table = pair.getRight();
        table.setFile(new ErrorRandomAccessFile());
        StringBuilder s = new StringBuilder();
        BlockHandle blockHandle = new BlockHandle(100, 100);
        blockHandle.encodeTo(s);
        table.readFilter(new Slice(s.toString()));

        assertNull(pair.getRight().filter);
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