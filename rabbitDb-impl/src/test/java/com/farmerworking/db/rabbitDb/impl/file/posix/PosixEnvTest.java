package com.farmerworking.db.rabbitDb.impl.file.posix;

import com.farmerworking.db.rabbitDb.api.Status;
import com.farmerworking.db.rabbitDb.impl.file.Env;
import com.farmerworking.db.rabbitDb.impl.file.RandomAccessFile;
import com.farmerworking.db.rabbitDb.impl.file.SequentialFile;
import com.farmerworking.db.rabbitDb.impl.file.WritableFile;
import com.farmerworking.db.rabbitDb.impl.utils.TestUtils;
import com.google.common.io.Files;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import static org.junit.Assert.*;
/**
 * Created by John on 18/11/10.
 */
public class PosixEnvTest {
    private Env env;

    @Before
    public void setUp() throws Exception {
        env = new PosixEnv();
    }

    @After
    public void tearDown() throws Exception {
        String directory = env.getTestDirectory().getRight();
        for(File file : new File(directory).listFiles()) {
            env.delete(file.getAbsolutePath());
        }

        env.delete(directory);
    }

    @Test
    public void testReadWrite() throws Exception {
        // Get file to use for testing.
        Pair<Status, String> testDirectory = env.getTestDirectory();
        assertTrue(testDirectory.getLeft().isOk());
        String testFileName = testDirectory.getRight() + "/open_on_read.txt";
        Pair<Status, WritableFile> pair = env.newWritableFile(testFileName);
        assertTrue(pair.getLeft().isOk());
        WritableFile writableFile = pair.getRight();

        // Fill a file with data generated via a sequence of randomly sized writes.
        Random random = new Random();
        int dataSize = 10 * 1048576;
        StringBuilder data = new StringBuilder();
        while (data.length() < dataSize) {
            int length = random.nextInt(262144) + 1;
            String tmp = TestUtils.randomString(length);
            assertTrue(writableFile.append(tmp).isOk());
            data.append(tmp);
            if (length % 10 == 0) {
                assertTrue(writableFile.flush().isOk());
            }
        }
        assertTrue(writableFile.sync().isOk());
        assertTrue(writableFile.close().isOk());

        Pair<Status, SequentialFile> pair2 = env.newSequentialFile(testFileName);
        assertTrue(pair2.getLeft().isOk());
        SequentialFile sequentialFile = pair2.getRight();

        StringBuilder readResult = new StringBuilder();
        while(readResult.length() < data.length()) {
            int length = Math.min(random.nextInt(262144) + 1, data.length() - readResult.length());
            Pair<Status, String> pair3 = sequentialFile.read(length);
            assertTrue(pair3.getLeft().isOk());

            if (length > 0) {
                assertTrue(pair3.getRight().length() > 0);
            }
            assertEquals(pair3.getRight().length(), length);
            readResult.append(pair3.getRight());
        }
        assertEquals(readResult.toString(), data.toString());
    }

    @Test
    public void testOpenNonExistentFile() throws Exception {
        // Write some test data to a single file that will be opened |n| times.
        Pair<Status, String> testDirectory = env.getTestDirectory();
        assertTrue(testDirectory.getLeft().isOk());

        String nonExistFile = testDirectory.getRight() + "/non_existent_file";
        Pair<Status, Boolean> pair = env.isFileExists(nonExistFile);
        assertTrue(pair.getLeft().isOk());
        assertFalse(pair.getRight());

        Pair<Status, RandomAccessFile> pair1 = env.newRandomAccessFile(nonExistFile);
        assertFalse(pair1.getLeft().isOk());
        assertNull(pair1.getRight());

        Pair<Status, SequentialFile> pair2 = env.newSequentialFile(nonExistFile);
        assertFalse(pair2.getLeft().isOk());
        assertNull(pair2.getRight());
    }

    @Test
    public void testReopenWritableFile() throws Exception {
        // Write some test data to a single file that will be opened |n| times.
        Pair<Status, String> testDirectory = env.getTestDirectory();
        assertTrue(testDirectory.getLeft().isOk());

        String testFileName = testDirectory.getRight() + "/reopen_writable_file.txt";
        env.delete(testFileName);

        Pair<Status, WritableFile> pair = env.newWritableFile(testFileName);
        assertTrue(pair.getLeft().isOk());
        WritableFile writableFile = pair.getRight();
        assertTrue(writableFile.append("hello, world!").isOk());
        assertTrue(writableFile.close().isOk());

        Pair<Status, WritableFile> pair2 = env.newWritableFile(testFileName);
        assertTrue(pair2.getLeft().isOk());
        writableFile = pair2.getRight();
        assertTrue(writableFile.append("42").isOk());
        assertTrue(writableFile.close().isOk());

        Pair<Status, String> pair3 = readFileToString(testFileName);
        assertTrue(pair3.getLeft().isOk());
        assertEquals(pair3.getRight(), "42");
    }

    @Test
    public void testReopenAppendableFile() throws Exception {
        // Write some test data to a single file that will be opened |n| times.
        Pair<Status, String> testDirectory = env.getTestDirectory();
        assertTrue(testDirectory.getLeft().isOk());

        String testFileName = testDirectory.getRight() + "/reopen_appendable_file.txt";
        env.delete(testFileName);

        Pair<Status, WritableFile> pair = env.newAppendableFile(testFileName);
        assertTrue(pair.getLeft().isOk());
        WritableFile writableFile = pair.getRight();
        assertTrue(writableFile.append("hello, world!").isOk());
        assertTrue(writableFile.close().isOk());

        Pair<Status, WritableFile> pair2 = env.newAppendableFile(testFileName);
        assertTrue(pair2.getLeft().isOk());
        writableFile = pair2.getRight();
        assertTrue(writableFile.append("42").isOk());
        assertTrue(writableFile.close().isOk());

        Pair<Status, String> pair3 = readFileToString(testFileName);
        assertTrue(pair3.getLeft().isOk());
        assertEquals(pair3.getRight(), "hello, world!42");
    }

    private Pair<Status, String> readFileToString(String filename) {
        try {
            return Pair.of(Status.ok(), StringUtils.join(Files.readLines(new File(filename), StandardCharsets.ISO_8859_1), ""));
        } catch (IOException e) {
            return Pair.of(Status.iOError(e.getMessage()), null);
        }
    }
}