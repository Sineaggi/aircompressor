package io.airlift.compress.zstd;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.Buffer;

import static io.airlift.compress.zstd.Constants.SIZE_OF_SHORT;

public class ArrayUtil {
    // see https://stackoverflow.com/questions/73430301/java-foreign-memory-varhandlesegmentviewbase-error-misaligned-access-at-addr
    private static final ValueLayout.OfByte JAVA_BYTE = ValueLayout.JAVA_BYTE.withBitAlignment(8);
    private static final ValueLayout.OfShort JAVA_SHORT = ValueLayout.JAVA_SHORT.withBitAlignment(8);
    private static final ValueLayout.OfInt JAVA_INT = ValueLayout.JAVA_INT.withBitAlignment(8);
    private static final ValueLayout.OfLong JAVA_LONG = ValueLayout.JAVA_LONG.withBitAlignment(8);
    private final MemorySegment memorySegment;
    private ArrayUtil(MemorySegment memorySegment) {
        this.memorySegment = memorySegment;
        this.positionAndLimitSet = false;
        this.position = 0;
        this.limit = 0;
    }
    private ArrayUtil(MemorySegment memorySegment, long position, long limit) {
        this.memorySegment = memorySegment;
        this.positionAndLimitSet = true;
        this.position = position;
        this.limit = limit;
    }
    private final boolean positionAndLimitSet;

    private final long position;
    private final long limit;
    public static ArrayUtil ofArray(byte[] array) {
        return new ArrayUtil(MemorySegment.ofArray(array));
    }
    public static ArrayUtil ofBuffer(Buffer buffer) {
        if (buffer.isDirect()) {
            long position = buffer.position();
            buffer.position(0);
            return new ArrayUtil(MemorySegment.ofBuffer(buffer), position, buffer.limit());
        }
        else if (buffer.hasArray()) {
            long position = buffer.position();
            buffer.position(0);
            return new ArrayUtil(MemorySegment.ofBuffer(buffer), position, buffer.limit());
        }
        else {
            throw new IllegalArgumentException("Unsupported ByteBuffer implementation " + buffer.getClass().getName());
        }
    }

    public byte getByte(long offset) {
        return memorySegment.get(JAVA_BYTE, offset);
    }

    public void putByte(long offset, byte value) {
        memorySegment.set(JAVA_BYTE, offset, value);
    }

    public short getShort(long offset) {
        return memorySegment.get(JAVA_SHORT, offset);
    }

    public void putShort(long offset, short value) {
        memorySegment.set(JAVA_SHORT, offset, value);
    }

    public int get24BitLittleEndian(long offset)
    {
        return memorySegment.get(JAVA_SHORT, offset) & 0xFFFF
                | memorySegment.get(JAVA_BYTE, offset + SIZE_OF_SHORT) << 16;
    }

    public void put24BitLittleEndian(long offset, int value)
    {
        memorySegment.set(JAVA_SHORT, offset, (short) value);
        memorySegment.set(JAVA_BYTE, offset + SIZE_OF_SHORT, (byte) (value >>> Short.SIZE));
    }

    public int getInt(long offset) {
        return memorySegment.get(JAVA_INT, offset);
    }

    public void putInt(long offset, int value) {
        memorySegment.set(JAVA_INT, offset, value);
    }

    public long getLong(long offset) {
        return memorySegment.get(JAVA_LONG, offset);
    }

    public void putLong(long offset, long value) {
        memorySegment.set(JAVA_LONG, offset, value);
    }

    public void copyMemory(long srcOffset, ArrayUtil destBase, long destOffset, long bytes) {
        MemorySegment.copy(memorySegment, srcOffset, destBase.memorySegment, destOffset, bytes);
    }

    public long position() {
        if (positionAndLimitSet) {
            return position;
        }
        throw new RuntimeException("position not set");
    }

    public long limit() {
        if (positionAndLimitSet) {
            return limit;
        }
        throw new RuntimeException("limit not set");
    }

}
