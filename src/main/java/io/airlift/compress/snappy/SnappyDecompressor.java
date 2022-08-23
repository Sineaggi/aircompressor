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
package io.airlift.compress.snappy;

import io.airlift.compress.Decompressor;
import io.airlift.compress.MalformedInputException;
import io.airlift.compress.zstd.ArrayUtil;

import java.lang.reflect.Array;
import java.nio.Buffer;
import java.nio.ByteBuffer;

import static io.airlift.compress.snappy.UnsafeUtil.getAddress;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public class SnappyDecompressor
        implements Decompressor
{
    public static int getUncompressedLength(byte[] compressed, int compressedOffset)
    {
        long compressedAddress = ARRAY_BYTE_BASE_OFFSET + compressedOffset;
        long compressedLimit = ARRAY_BYTE_BASE_OFFSET + compressed.length;

        return SnappyRawDecompressor.getUncompressedLength(ArrayUtil.ofArray(compressed), compressedAddress, compressedLimit);
    }

    @Override
    public int decompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
            throws MalformedInputException
    {
        verifyRange(input, inputOffset, inputLength);
        verifyRange(output, outputOffset, maxOutputLength);

        long inputOffset = ARRAY_BYTE_BASE_OFFSET + inputOffset;
        long inputLimit = inputOffset + inputLength;
        long outputOffset = ARRAY_BYTE_BASE_OFFSET + outputOffset;
        long outputLimit = outputOffset + maxOutputLength;

        return SnappyRawDecompressor.decompress(ArrayUtil.ofArray(input), inputOffset, inputLimit, ArrayUtil.ofArray(output), outputOffset, outputLimit);
    }

    @Override
    public void decompress(ByteBuffer inputBuffer, ByteBuffer outputBuffer)
            throws MalformedInputException
    {
        // Java 9+ added an overload of various methods in ByteBuffer. When compiling with Java 11+ and targeting Java 8 bytecode
        // the resulting signatures are invalid for JDK 8, so accesses below result in NoSuchMethodError. Accessing the
        // methods through the interface class works around the problem
        // Sidenote: we can't target "javac --release 8" because Unsafe is not available in the signature data for that profile
        Buffer input = inputBuffer;
        Buffer output = outputBuffer;

        ArrayUtil inputBase = ArrayUtil.ofBuffer(input);
        long inputOffset;
        long inputLimit;
        if (input.isDirect()) {
            long address = getAddress(input);
            inputOffset = address + input.position();
            inputLimit = address + input.limit();
        }
        else if (input.hasArray()) {
            inputOffset = ARRAY_BYTE_BASE_OFFSET + input.arrayOffset() + input.position();
            inputLimit = ARRAY_BYTE_BASE_OFFSET + input.arrayOffset() + input.limit();
        }
        else {
            throw new IllegalArgumentException("Unsupported input ByteBuffer implementation " + input.getClass().getName());
        }

        ArrayUtil outputBase = ArrayUtil.ofBuffer(output);
        long outputOffset;
        long outputLimit;
        if (output.isDirect()) {
            long address = getAddress(output);
            outputOffset = address + output.position();
            outputLimit = address + output.limit();
        }
        else if (output.hasArray()) {
            outputOffset = ARRAY_BYTE_BASE_OFFSET + output.arrayOffset() + output.position();
            outputLimit = ARRAY_BYTE_BASE_OFFSET + output.arrayOffset() + output.limit();
        }
        else {
            throw new IllegalArgumentException("Unsupported output ByteBuffer implementation " + output.getClass().getName());
        }

        // HACK: Assure JVM does not collect Slice wrappers while decompressing, since the
        // collection may trigger freeing of the underlying memory resulting in a segfault
        // There is no other known way to signal to the JVM that an object should not be
        // collected in a block, and technically, the JVM is allowed to eliminate these locks.
        synchronized (input) {
            synchronized (output) {
                int written = SnappyRawDecompressor.decompress(inputBase, inputOffset, inputLimit, outputBase, outputOffset, outputLimit);
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
