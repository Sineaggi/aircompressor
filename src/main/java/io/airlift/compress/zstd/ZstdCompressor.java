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

import io.airlift.compress.Compressor;

import java.nio.Buffer;
import java.nio.ByteBuffer;

import static io.airlift.compress.zstd.Constants.MAX_BLOCK_SIZE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ZstdCompressor
        implements Compressor
{
    @Override
    public int maxCompressedLength(int uncompressedSize)
    {
        int result = uncompressedSize + (uncompressedSize >>> 8);

        if (uncompressedSize < MAX_BLOCK_SIZE) {
            result += (MAX_BLOCK_SIZE - uncompressedSize) >>> 11;
        }

        return result;
    }

    @Override
    public int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        verifyRange(input, inputOffset, inputLength);
        verifyRange(output, outputOffset, maxOutputLength);

        //long inputOffset = 0 + inputOffset;
        //long outputOffset = 0 + outputOffset;

        return ZstdFrameCompressor.compress(ArrayUtil.ofArray(input), inputOffset, inputOffset + inputLength, ArrayUtil.ofArray(output), outputOffset, outputOffset + maxOutputLength, CompressionParameters.DEFAULT_COMPRESSION_LEVEL);
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

        ArrayUtil inputBase = ArrayUtil.ofBuffer(input);
        long inputOffset = inputBase.position();
        long inputLimit = inputBase.limit();

        ArrayUtil outputBase = ArrayUtil.ofBuffer(output);
        long outputOffset = outputBase.position();
        long outputLimit = outputBase.limit();

        // HACK: Assure JVM does not collect Slice wrappers while compressing, since the
        // collection may trigger freeing of the underlying memory resulting in a segfault
        // There is no other known way to signal to the JVM that an object should not be
        // collected in a block, and technically, the JVM is allowed to eliminate these locks.
        synchronized (input) {
            synchronized (output) {
                int written = ZstdFrameCompressor.compress(
                        inputBase,
                        inputOffset,
                        inputLimit,
                        outputBase,
                        outputOffset,
                        outputLimit,
                        CompressionParameters.DEFAULT_COMPRESSION_LEVEL);
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
