package com.farmerworking.db.rabbitDb.impl.file;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by John on 18/11/8.
 */
public class FileNameUtilTest {
    class Case {
        String fileName;
        long fileNumber;
        FileType type;

        public Case(String fileName, long fileNumber, FileType type) {
            this.fileName = fileName;
            this.fileNumber = fileNumber;
            this.type = type;
        }
    }

    @Test
    public void testParse() throws Exception {
        List<Case> cases = new ArrayList<>();
        cases.add(new Case("0.sst", 0, FileType.TableFile));
        cases.add(new Case("0.ldb", 0, FileType.TableFile));

        for (Case item : cases) {
            Pair<Long, FileType> result = FileNameUtil.parseFileName(item.fileName);
            assertNotNull(result);
            assertEquals(item.fileNumber, result.getLeft().longValue());
            assertEquals(item.type, result.getRight());
        }
    }

    @Test
    public void testParseError() throws Exception {
        List<String> errors = Lists.newArrayList(
                "",
                "foo",
                "foo-dx-100.log",
                ".log",
                "",
                "manifest",
                "CURREN",
                "CURRENTX",
                "MANIFES",
                "MANIFEST",
                "MANIFEST-",
                "XMANIFEST-3",
                "MANIFEST-3x",
                "LOC",
                "LOCKx",
                "LO",
                "LOGx",
                "18446744073709551616.log",
                "184467440737095516150.log",
                "100",
                "100.",
                "100.lop"
        );

        for(String item : errors) {
            Pair<Long, FileType> result = FileNameUtil.parseFileName(item);
            assertNull(result);
        }
    }

    @Test
    public void testConstruction() throws Exception {
        String fileName = FileNameUtil.tableFileName("bar", 200);
        assertTrue(fileName.startsWith("bar/"));

        Pair<Long, FileType> pair = FileNameUtil.parseFileName(fileName);
        assertNotNull(pair);
        assertEquals(pair.getLeft().longValue(), 200l);
        assertEquals(pair.getRight(), FileType.TableFile);
    }
}