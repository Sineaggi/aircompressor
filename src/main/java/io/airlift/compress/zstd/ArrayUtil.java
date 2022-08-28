package io.airlift.compress.zstd;

import java.nio.Buffer;

import static io.airlift.compress.zstd.Constants.SIZE_OF_SHORT;
import static io.airlift.compress.zstd.UnsafeUtil.UNSAFE;
import static io.airlift.compress.zstd.UnsafeUtil.getAddress;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public class ArrayUtil {
    private ArrayUtil(Object base, long baseAddress) {
        this.base = base;
        this.baseAddress = baseAddress;
        // todo: fix this concept
        this.positionAndLimitSet = false;
        this.position = 0;
        this.limit = 0;
    }
    private ArrayUtil(Object base, long baseAddress, long position, long limit) {
        this.base = base;
        this.baseAddress = baseAddress;
        this.positionAndLimitSet = true;
        this.position = position;
        this.limit = limit;
    }
    private final Object base;
    private final long baseAddress;
    private final boolean positionAndLimitSet;

    private final long position;
    private final long limit;
    public static ArrayUtil ofArray(byte[] array) {
        return new ArrayUtil(array, ARRAY_BYTE_BASE_OFFSET);
    }
    public static ArrayUtil ofBuffer(Buffer buffer) {
        // todo: do not pre-calculate position maybe?
        if (buffer.isDirect()) {
            long address = getAddress(buffer);
            //address = address + buffer.position();
            return new ArrayUtil(null, address, buffer.position(), buffer.limit());
        }
        else if (buffer.hasArray()) {
            long address = ARRAY_BYTE_BASE_OFFSET + buffer.arrayOffset();// + buffer.position();
            return new ArrayUtil(buffer.array(), address, buffer.position(), buffer.limit());
        }
        else {
            throw new IllegalArgumentException("Unsupported ByteBuffer implementation " + buffer.getClass().getName());
        }
    }

    public byte getByte(long offset) {
        return UNSAFE.getByte(base, baseAddress + offset);
    }

    public void putByte(long offset, byte value) {
        UNSAFE.putByte(base, baseAddress + offset, value);
    }

    public short getShort(long offset) {
        return UNSAFE.getShort(base, baseAddress + offset);
    }

    public void putShort(long offset, short value) {
        UNSAFE.putShort(base, baseAddress + offset, value);
    }

    public int get24BitLittleEndian(long offset)
    {
        return UNSAFE.getShort(base, baseAddress + offset) & 0xFFFF
            | UNSAFE.getByte(base, baseAddress + offset + SIZE_OF_SHORT) << 16;
    }

    public void put24BitLittleEndian(long offset, int value)
    {
        UNSAFE.putShort(base, baseAddress + offset, (short) value);
        UNSAFE.putByte(base, baseAddress + offset + SIZE_OF_SHORT, (byte) (value >>> Short.SIZE));
    }

    public int getInt(long offset) {
        return UNSAFE.getInt(base, baseAddress + offset);
    }

    public void putInt(long offset, int value) {
        UNSAFE.putInt(base, baseAddress + offset, value);
    }

    public long getLong(long offset) {
        return UNSAFE.getLong(base, baseAddress + offset);
    }

    public void putLong(long offset, long value) {
        UNSAFE.putLong(base, baseAddress + offset, value);
    }

    public void copyMemory(long srcOffset, ArrayUtil destBase, long destOffset, long bytes) {
        UNSAFE.copyMemory(base, baseAddress + srcOffset, destBase.base, destBase.baseAddress + destOffset, bytes);
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
