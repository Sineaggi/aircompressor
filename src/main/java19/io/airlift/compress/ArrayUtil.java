package io.airlift.compress;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.Buffer;

public class ArrayUtil {
    private ArrayUtil(MemorySegment array) {
        this.arraySegment = array;
    }
    private final MemorySegment arraySegment;
    public static ArrayUtil of(byte[] array) {
        return new ArrayUtil(MemorySegment.ofArray(array));
    }
    public static ArrayUtil of(Buffer buffer) {
        return new ArrayUtil(MemorySegment.ofBuffer(buffer));
    }
    public byte getByte(long offset) {
        return arraySegment.get(ValueLayout.JAVA_BYTE, offset);
    }
    public void putByte(long output, byte value) {
        arraySegment.set(ValueLayout.JAVA_BYTE, output, value);
    }
}
