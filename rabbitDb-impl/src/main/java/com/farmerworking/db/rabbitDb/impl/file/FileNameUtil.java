package com.farmerworking.db.rabbitDb.impl.file;

import org.apache.commons.lang3.tuple.Pair;

public class FileNameUtil {
    public static String tableFileName(String dbname, long fileNumber) {
        assert fileNumber > 0;
        return makeFileName(dbname, fileNumber, "ldb");
    }

    public static String sstTableFileName(String dbname, long fileNumber) {
        assert fileNumber > 0;
        return makeFileName(dbname, fileNumber, "sst");
    }

    private static String makeFileName(String dbname, long fileNumber, String suffix) {
        return dbname + "/" + fileNumber + "." + suffix;
    }

    public static Pair<Long, FileType> parseFileName(String fileName) {
        if (fileName.endsWith(".sst") || fileName.endsWith(".ldb")) {
            FileType fileType = FileType.TableFile;
            long fileNumber = Long.parseLong(
                    fileName.substring(
                            fileName.indexOf("/") + 1,
                            fileName.indexOf(".")));
            return Pair.of(fileNumber, fileType);
        } else {
            return null;
        }
    }
}
