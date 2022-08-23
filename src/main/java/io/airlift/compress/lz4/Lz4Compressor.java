/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.airlift.compress.lz4;

import io.airlift.compress.Compressor;
import io.airlift.compress.zstd.ArrayUtil;

import java.nio.Buffer;
import java.nio.ByteBuffer;

import static io.airlift.compress.lz4.Lz4RawCompressor.MAX_TABLE_SIZE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * This class is not thread-safe
 */
public class Lz4Compressor
        implements Compressor
{
    private final int[] table = new int[MAX_TABLE_SIZE];

    @Override
    public int maxCompressedLength(int uncompressedSize)
    {
        return Lz4RawCompressor.maxCompressedLength(uncompressedSize);
    }

    @Override
    public int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        verifyRange(input, inputOffset, inputLength);
        verifyRange(output, outputOffset, maxOutputLength);

        long inputAddress = 0 + inputOffset;
        long outputAddress = 0 + outputOffset;

        return Lz4RawCompressor.compress(ArrayUtil.ofArray(input), inputAddress, inputLength, ArrayUtil.ofArray(output), outputAddress, maxOutputLength, table);
    }

    @Override
    public void compress(ByteBuffer inputBuffer, ByteBuffer outputBuffer)
    {
        // Java 9+ added an overload of various methods in ByteBuffer. When compiling with Java 11+ and targeting Java 8 bytecode
        // the resulting signatures are invalid for JDK 8, so accesses below result in NoSuchMethodError. Accessing the
        // methods through the interface class works around the problem
        // Sidenote: we can't target "javac --release 8" because Unsafe is not available in the signature data for that profile
        Buffer input = inputBuffer;
        Buffer output = outputBuffer;

        ArrayUtil inputBase;
        long inputAddress;
        long inputLimit;
        if (input.isDirect()) {
            inputBase = ArrayUtil.ofBuffer(input);
            long address = 0;
            inputAddress = address + input.position();
            inputLimit = address + input.limit();
        }
        else if (input.hasArray()) {
            inputBase = ArrayUtil.ofArray((byte[])input.array());
            inputAddress = input.arrayOffset() + input.position();
            inputLimit = input.arrayOffset() + input.limit();
        }
        else {
            throw new IllegalArgumentException("Unsupported input ByteBuffer implementation " + input.getClass().getName());
        }

        ArrayUtil outputBase;
        long outputAddress;
        long outputLimit;
        if (output.isDirect()) {
            outputBase = ArrayUtil.ofBuffer(output);
            long address = 0;
            outputAddress = address + output.position();
            outputLimit = address + output.limit();
        }
        else if (output.hasArray()) {
            outputBase = ArrayUtil.ofArray((byte[])output.array());
            outputAddress = 0 + output.arrayOffset() + output.position();
            outputLimit = 0 + output.arrayOffset() + output.limit();
        }
        else {
            throw new IllegalArgumentException("Unsupported output ByteBuffer implementation " + output.getClass().getName());
        }

        // HACK: Assure JVM does not collect Slice wrappers while compressing, since the
        // collection may trigger freeing of the underlying memory resulting in a segfault
        // There is no other known way to signal to the JVM that an object should not be
        // collected in a block, and technically, the JVM is allowed to eliminate these locks.
        synchronized (input) {
            synchronized (output) {
                int written = Lz4RawCompressor.compress(
                        inputBase,
                        inputAddress,
                        (int) (inputLimit - inputAddress),
                        outputBase,
                        outputAddress,
                        outputLimit - outputAddress,
                        table);
                output.position(output.position() + written);
            }
        }
    }

    private static void verifyRange(byte[] data, int offset, int length)
    {
        requireNonNull(data, "data is null");
        if (offset < 0 || length < 0 || offset + length > data.length) {
            throw new IllegalArgumentException(format("Invalid offset or length (%s, %s) in array of length %s", offset, length, data.length));
        }
    }
}
