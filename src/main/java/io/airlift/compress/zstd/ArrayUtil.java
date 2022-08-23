package io.airlift.compress.zstd;

import java.nio.Buffer;

import static io.airlift.compress.zstd.UnsafeUtil.UNSAFE;
import static io.airlift.compress.zstd.UnsafeUtil.getAddress;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public class ArrayUtil {
    private ArrayUtil(Object base, long baseAddress) {
        this.base = base;
        this.baseAddress = baseAddress;
    }
    private final Object base;
    private final long baseAddress;
    public static ArrayUtil ofArray(byte[] array) {
        return new ArrayUtil(array, ARRAY_BYTE_BASE_OFFSET);
    }
    public static ArrayUtil ofBuffer(Buffer array) {
        return new ArrayUtil(null, getAddress(array));
    }

    public byte getByte(long offset) {
        return UNSAFE.getByte(base, baseAddress + offset);
    }

    public void putByte(long offset, byte value) {
        UNSAFE.putByte(base, baseAddress + offset, value);
    }

    public void putShort(long offset, short value) {
        UNSAFE.putShort(base, baseAddress + offset, value);
    }

    public short getShort(long offset) {
        return UNSAFE.getShort(base, baseAddress + offset);
    }

    public int getInt(long offset) {
        return UNSAFE.getInt(base, baseAddress + offset);
    }

    public long getLong(long offset) {
        return UNSAFE.getLong(base, baseAddress + offset);
    }

    public void putLong(long offset, long value) {
        UNSAFE.putLong(base, baseAddress + offset, value);
    }

    public void putInt(long offset, int value) {
        UNSAFE.putInt(base, baseAddress + offset, value);
    }

    public void copyMemory(long srcOffset, ArrayUtil destBase, long destOffset, long bytes) {
        UNSAFE.copyMemory(base, baseAddress + srcOffset, destBase.base, destBase.baseAddress + destOffset, bytes);
    }
}
