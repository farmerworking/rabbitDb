package com.farmerworking.db.rabbitDb.api;

import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class Status {
    private short code;
    private String message;

    public Status() {
        this.code = Code.kOk.value;
    }

    public Status(Status status) {
        this.code = status.code;
        this.message = status.message;
    }

    private Status(Code code, String msg1, String msg2) {
        this.code = code.value;
        this.message = msg1.toString() + ": " + msg2.toString();
    }

    private Status(Code code, String msg) {
        this.code = code.value;
        this.message = msg.toString();
    }

    public static Status ok() {
        return new Status();
    }

    public static Status notFound(String msg1) {
        return new Status(Code.kNotFound, msg1);
    }

    public static Status corruption(String msg1) {
        return new Status(Code.kCorruption, msg1);
    }

    public static Status notSupported(String msg) {
        return new Status(Code.kNotSupported, msg);
    }

    public static Status invalidArgument(String msg) {
        return new Status(Code.kInvalidArgument, msg);
    }

    public static Status iOError(String msg) {
        return new Status(Code.kIOError, msg);
    }

    public static Status notFound(String msg1, String msg2) {
        return new Status(Code.kNotFound, msg1, msg2);
    }

    public static Status corruption(String msg1, String msg2) {
        return new Status(Code.kCorruption, msg1, msg2);
    }

    public static Status notSupported(String msg, String msg2) {
        return new Status(Code.kNotSupported, msg, msg2);
    }

    public static Status invalidArgument(String msg, String msg2) {
        return new Status(Code.kInvalidArgument, msg, msg2);
    }

    public static Status iOError(String msg, String msg2) {
        return new Status(Code.kIOError, msg, msg2);
    }

    // Returns true iff the status indicates success.
    public boolean isOk() {
        return (code == Code.kOk.value);
    }

    public boolean isNotOk() {
        return (code != Code.kOk.value);
    }

    // Returns true iff the status indicates a NotFound error.
    public boolean isNotFound() {
        return code == Code.kNotFound.value;
    }

    // Returns true iff the status indicates a Corruption error.
    public boolean isCorruption() {
        return code == Code.kCorruption.value;
    }

    // Returns true iff the status indicates an IOError.
    public boolean isIOError() {
        return code == Code.kIOError.value;
    }

    // Returns true iff the status indicates a NotSupportedError.
    public boolean isNotSupportedError() {
        return code == Code.kNotSupported.value;
    }

    // Returns true iff the status indicates an InvalidArgument.
    public boolean isInvalidArgument() {
        return code == Code.kInvalidArgument.value;
    }

    // Return a string representation of this status suitable for printing.
    // Returns the string "OK" for success.
    @Override
    public String toString() {
        String result;

        Code valueOf = Code.valueOf(code);
        if (valueOf == null) {
            result = String.format("Unknown code(%d)", code);
        } else {
            result = valueOf.display;
        }

        return result + (StringUtils.isEmpty(message) ? "" : ": " + message);
    }

    enum Code {
        kOk(0, "OK"),
        kNotFound(1, "NotFound"),
        kCorruption(2, "Corruption"),
        kNotSupported(3, "Not implemented"),
        kInvalidArgument(4, "Invalid argument"),
        kIOError(5, "IO error");

        private static Map<Short, Code> map = new HashMap<>();

        static {
            map.put(kOk.value, kOk);
            map.put(kNotFound.value, kNotFound);
            map.put(kCorruption.value, kCorruption);
            map.put(kNotSupported.value, kNotSupported);
            map.put(kInvalidArgument.value, kInvalidArgument);
            map.put(kIOError.value, kIOError);
        }

        private short value;
        private String display;

        Code(Integer i, String display) {
            this.value = i.shortValue();
            this.display = display;
        }

        public static Code valueOf(short code) {
            if (map.containsKey(code)) {
                return map.get(code);
            } else {
                return null;
            }
        }
    }
}

