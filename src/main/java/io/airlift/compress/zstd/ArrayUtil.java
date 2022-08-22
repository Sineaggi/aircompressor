package io.airlift.compress.zstd;

import java.nio.Buffer;

import static io.airlift.compress.zstd.UnsafeUtil.UNSAFE;

public class ArrayUtil {
    private ArrayUtil(Object base) {
        this.base = base;
    }
    private final Object base;
    public static ArrayUtil of(byte[] array) {
        return new ArrayUtil(array);
    }
    public static ArrayUtil of(Buffer array) {
        if (array.hasArray()) {
            return new ArrayUtil(array.array());
        }
        return new ArrayUtil(null);
    }

    public byte getByte(long offset) {
        return UNSAFE.getByte(base, offset);
    }

    public void putByte(long output, byte value) {
        UNSAFE.putByte(base, output, value);
    }

    public void putShort(long offset, short value) {
        UNSAFE.putShort(base, offset, value);
    }

    public short getShort(long offset) {
        return UNSAFE.getShort(base, offset);
    }

    public int getInt(long offset) {
        return UNSAFE.getInt(base, offset);
    }

    public long getLong(long offset) {
        return UNSAFE.getLong(base, offset);
    }

    public void putLong(long offset, long value) {
        UNSAFE.putLong(base, offset, value);
    }

    public void putInt(long offset, int value) {
        UNSAFE.putInt(base, offset, value);
    }

    public void copyMemory(long input, byte[] literals, long literalsAddress, int literalSize) {
        UNSAFE.copyMemory(base, input, literals, literalsAddress, literalsAddress);
    }
}
