package com.farmerworking.db.rabbitDb;

public class OverflowUtils {
  public static char unsignedInt8Overflow(char value) {
    int result;
    int charAsInt = (int) value;
    if (charAsInt >= 0 && charAsInt <= 255) {
      result = charAsInt;
    } else {
      result = charAsInt % 256;
    }

    assert result >= 0 && result <= 255;
    return (char) result;
  }
}
