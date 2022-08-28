package io.airlift.compress.zstd;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;

import java.nio.Buffer;
import java.nio.ByteBuffer;

import static io.airlift.compress.zstd.Constants.SIZE_OF_SHORT;

public class ArrayUtil {
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
            return new ArrayUtil(MemorySegment.ofByteBuffer((ByteBuffer) buffer), position, buffer.limit());
        }
        else if (buffer.hasArray()) {
            long position = buffer.position();
            buffer.position(0);
            return new ArrayUtil(MemorySegment.ofByteBuffer((ByteBuffer) buffer), position, buffer.limit());
        }
        else {
            throw new IllegalArgumentException("Unsupported ByteBuffer implementation " + buffer.getClass().getName());
        }
    }

    public byte getByte(long offset) {
        return MemoryAccess.getByteAtOffset(memorySegment, offset);
    }

    public void putByte(long offset, byte value) {
        MemoryAccess.setByteAtOffset(memorySegment, offset, value);
    }

    public short getShort(long offset) {
        return MemoryAccess.getShortAtOffset(memorySegment, offset);
    }

    public void putShort(long offset, short value) {
        MemoryAccess.setShortAtOffset(memorySegment, offset, value);
    }

    public int get24BitLittleEndian(long offset)
    {
        return MemoryAccess.getShortAtOffset(memorySegment, offset) & 0xFFFF
                | (MemoryAccess.getByteAtOffset(memorySegment, offset + SIZE_OF_SHORT) & 0xFF) << 16;
    }

    public void put24BitLittleEndian(long offset, int value)
    {
        MemoryAccess.setShortAtOffset(memorySegment, offset, (short) value);
        MemoryAccess.setByteAtOffset(memorySegment, offset + SIZE_OF_SHORT, (byte) (value >>> Short.SIZE));
    }

    public int getInt(long offset) {
        return MemoryAccess.getIntAtOffset(memorySegment, offset);
    }

    public void putInt(long offset, int value) {
        MemoryAccess.setIntAtOffset(memorySegment, offset, value);
    }

    public long getLong(long offset) {
        return MemoryAccess.getLongAtOffset(memorySegment, offset);
    }

    public void putLong(long offset, long value) {
        MemoryAccess.setLongAtOffset(memorySegment, offset, value);
    }

    public void copyMemory(long srcOffset, ArrayUtil destBase, long destOffset, long bytes) {
        destBase.memorySegment.asSlice(destOffset).copyFrom(memorySegment.asSlice(srcOffset, bytes));
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
