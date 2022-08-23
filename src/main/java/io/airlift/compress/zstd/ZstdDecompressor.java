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
package io.airlift.compress.zstd;

import io.airlift.compress.Decompressor;
import io.airlift.compress.MalformedInputException;

import java.nio.Buffer;
import java.nio.ByteBuffer;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ZstdDecompressor
        implements Decompressor
{
    private final ZstdFrameDecompressor decompressor = new ZstdFrameDecompressor();

    @Override
    public int decompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
            throws MalformedInputException
    {
        // todo: so even the base case is fucked by ARRAY_BYTE_BASE_OFFSET garbage. need to internalize this.
        verifyRange(input, inputOffset, inputLength);
        verifyRange(output, outputOffset, maxOutputLength);

        long inputAddress = inputOffset;
        long inputLimit = inputAddress + inputLength;
        long outputAddress = outputOffset;
        long outputLimit = outputAddress + maxOutputLength;

        return decompressor.decompress(ArrayUtil.ofArray(input), inputAddress, inputLimit, ArrayUtil.ofArray(output), outputAddress, outputLimit);
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
            outputAddress = output.arrayOffset() + output.position();
            outputLimit = output.arrayOffset() + output.limit();
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
                int written = new ZstdFrameDecompressor().decompress(inputBase, inputAddress, inputLimit, outputBase, outputAddress, outputLimit);
                output.position(output.position() + written);
            }
        }
    }

    public static long getDecompressedSize(byte[] input, int offset, int length)
    {
        int baseAddress = offset;
        return ZstdFrameDecompressor.getDecompressedSize(ArrayUtil.ofArray(input), baseAddress, baseAddress + length);
    }

    private static void verifyRange(byte[] data, int offset, int length)
    {
        requireNonNull(data, "data is null");
        if (offset < 0 || length < 0 || offset + length > data.length) {
            throw new IllegalArgumentException(format("Invalid offset or length (%s, %s) in array of length %s", offset, length, data.length));
        }
    }
}
