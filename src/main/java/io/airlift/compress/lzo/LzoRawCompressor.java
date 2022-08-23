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
package io.airlift.compress.lzo;

import io.airlift.compress.zstd.ArrayUtil;

import java.util.Arrays;

import static io.airlift.compress.lzo.LzoConstants.SIZE_OF_INT;
import static io.airlift.compress.lzo.LzoConstants.SIZE_OF_LONG;
import static io.airlift.compress.lzo.LzoConstants.SIZE_OF_SHORT;

public final class LzoRawCompressor
{
    public static final int LAST_LITERAL_SIZE = 5;
    public static final int MIN_MATCH = 4;

    private static final int MAX_INPUT_SIZE = 0x7E000000;   /* 2 113 929 216 bytes */

    private static final int HASH_LOG = 12;

    private static final int MIN_TABLE_SIZE = 16;
    public static final int MAX_TABLE_SIZE = (1 << HASH_LOG);

    private static final int COPY_LENGTH = 8;
    private static final int MATCH_FIND_LIMIT = COPY_LENGTH + MIN_MATCH;

    private static final int MIN_LENGTH = MATCH_FIND_LIMIT + 1;

    private static final int ML_BITS = 4;
    private static final int RUN_BITS = 8 - ML_BITS;
    private static final int RUN_MASK = (1 << RUN_BITS) - 1;

    private static final int MAX_DISTANCE = 0b1100_0000_0000_0000 - 1;

    private static final int SKIP_TRIGGER = 6;  /* Increase this value ==> compression run slower on incompressible data */

    private LzoRawCompressor() {}

    private static int hash(long value, int mask)
    {
        // Multiplicative hash. It performs the equivalent to
        // this computation:
        //
        //  value * frac(a)
        //
        // for some real number 'a' with a good & random mix
        // of 1s and 0s in its binary representation
        //
        // For performance, it does it using fixed point math
        return (int) ((value * 889523592379L >>> 28) & mask);
    }

    public static int maxCompressedLength(int sourceLength)
    {
        return sourceLength + sourceLength / 255 + 16;
    }

    public static int compress(
            final ArrayUtil inputBase,
            final long inputOffset,
            final int inputLength,
            final ArrayUtil outputBase,
            final long outputOffset,
            final long maxOutputLength,
            final int[] table)
    {
        int tableSize = computeTableSize(inputLength);
        Arrays.fill(table, 0, tableSize, 0);

        int mask = tableSize - 1;

        if (inputLength > MAX_INPUT_SIZE) {
            throw new IllegalArgumentException("Max input length exceeded");
        }

        if (maxOutputLength < maxCompressedLength(inputLength)) {
            throw new IllegalArgumentException("Max output length must be larger than " + maxCompressedLength(inputLength));
        }

        // nothing compresses to nothing
        if (inputLength == 0) {
            return 0;
        }

        long input = inputOffset;
        long output = outputOffset;

        final long inputLimit = inputOffset + inputLength;
        final long matchFindLimit = inputLimit - MATCH_FIND_LIMIT;
        final long matchLimit = inputLimit - LAST_LITERAL_SIZE;

        if (inputLength < MIN_LENGTH) {
            output = emitLastLiteral(true, outputBase, output, inputBase, input, inputLimit - input);
            return (int) (output - outputOffset);
        }

        long anchor = input;

        // First Byte
        // put position in hash
        table[hash(inputBase.getLong(input), mask)] = (int) (input - inputOffset);

        input++;
        int nextHash = hash(inputBase.getLong(input), mask);

        boolean done = false;
        boolean firstLiteral = true;
        do {
            long nextInputIndex = input;
            int findMatchAttempts = 1 << SKIP_TRIGGER;
            int step = 1;

            // find 4-byte match
            long matchIndex;
            do {
                int hash = nextHash;
                input = nextInputIndex;
                nextInputIndex += step;

                step = (findMatchAttempts++) >>> SKIP_TRIGGER;

                if (nextInputIndex > matchFindLimit) {
                    output = emitLastLiteral(firstLiteral, outputBase, output, inputBase, anchor, inputLimit - anchor);
                    return (int) (output - outputOffset);
                }

                // get position on hash
                matchIndex = inputOffset + table[hash];
                nextHash = hash(inputBase.getLong(nextInputIndex), mask);

                // put position on hash
                table[hash] = (int) (input - inputOffset);
            }
            while (inputBase.getInt(matchIndex) != inputBase.getInt(input) || matchIndex + MAX_DISTANCE < input);

            // catch up
            while ((input > anchor) && (matchIndex > inputOffset) && (inputBase.getByte(input - 1) == inputBase.getByte(matchIndex - 1))) {
                --input;
                --matchIndex;
            }

            int literalLength = (int) (input - anchor);

            output = emitLiteral(firstLiteral, inputBase, anchor, outputBase, output, literalLength);
            firstLiteral = false;

            // next match
            while (true) {
                int offset = (int) (input - matchIndex);

                // find match length
                input += MIN_MATCH;
                int matchLength = count(inputBase, input, matchIndex + MIN_MATCH, matchLimit);
                input += matchLength;

                // write copy command
                output = emitCopy(outputBase, output, offset, matchLength + MIN_MATCH);
                anchor = input;

                // are we done?
                if (input > matchFindLimit) {
                    done = true;
                    break;
                }

                long position = input - 2;
                table[hash(inputBase.getLong(position), mask)] = (int) (position - inputOffset);

                // Test next position
                int hash = hash(inputBase.getLong(input), mask);
                matchIndex = inputOffset + table[hash];
                table[hash] = (int) (input - inputOffset);

                if (matchIndex + MAX_DISTANCE < input || inputBase.getInt(matchIndex) != inputBase.getInt(input)) {
                    input++;
                    nextHash = hash(inputBase.getLong(input), mask);
                    break;
                }

                // go for another match
            }
        }
        while (!done);

        // Encode Last Literals
        output = emitLastLiteral(false, outputBase, output, inputBase, anchor, inputLimit - anchor);

        return (int) (output - outputOffset);
    }

    private static int count(ArrayUtil inputBase, final long start, long matchStart, long matchLimit)
    {
        long current = start;

        // first, compare long at a time
        while (current < matchLimit - (SIZE_OF_LONG - 1)) {
            long diff = inputBase.getLong(matchStart) ^ inputBase.getLong(current);
            if (diff != 0) {
                current += Long.numberOfTrailingZeros(diff) >> 3;
                return (int) (current - start);
            }

            current += SIZE_OF_LONG;
            matchStart += SIZE_OF_LONG;
        }

        if (current < matchLimit - (SIZE_OF_INT - 1) && inputBase.getInt(matchStart) == inputBase.getInt(current)) {
            current += SIZE_OF_INT;
            matchStart += SIZE_OF_INT;
        }

        if (current < matchLimit - (SIZE_OF_SHORT - 1) && inputBase.getShort(matchStart) == inputBase.getShort(current)) {
            current += SIZE_OF_SHORT;
            matchStart += SIZE_OF_SHORT;
        }

        if (current < matchLimit && inputBase.getByte(matchStart) == inputBase.getByte(current)) {
            ++current;
        }

        return (int) (current - start);
    }

    private static long emitLastLiteral(
            boolean firstLiteral,
            final ArrayUtil outputBase,
            long output,
            final ArrayUtil inputBase,
            final long inputOffset,
            final long literalLength)
    {
        output = encodeLiteralLength(firstLiteral, outputBase, output, literalLength);
        inputBase.copyMemory(inputOffset, outputBase, output, literalLength);
        output += literalLength;

        // write stop command
        // this is a 0b0001_HMMM command with a zero match offset
        outputBase.putByte(output++, (byte) 0b0001_0001);
        outputBase.putShort(output, (byte) 0);
        output += SIZE_OF_SHORT;

        return output;
    }

    private static long emitLiteral(
            boolean firstLiteral,
            ArrayUtil inputBase,
            long input,
            ArrayUtil outputBase,
            long output,
            int literalLength)
    {
        output = encodeLiteralLength(firstLiteral, outputBase, output, literalLength);

        final long outputLimit = output + literalLength;
        do {
            outputBase.putLong(output, inputBase.getLong(input));
            input += SIZE_OF_LONG;
            output += SIZE_OF_LONG;
        }
        while (output < outputLimit);

        return outputLimit;
    }

    private static long encodeLiteralLength(
            boolean firstLiteral,
            final ArrayUtil outBase,
            long output,
            long length)
    {
        if (firstLiteral && length < (0xFF - 17)) {
            outBase.putByte(output++, (byte) (length + 17));
        }
        else if (length < 4) {
            // Small literals are encoded in the low two bits trailer of the previous command.  The
            // trailer is a little endian short, so we need to adjust the byte 2 back in the output.
            outBase.putByte(output - 2, (byte) (outBase.getByte(output - 2) | length));
        }
        else {
            length -= 3;
            if (length > RUN_MASK) {
                outBase.putByte(output++, (byte) 0);

                long remaining = length - RUN_MASK;
                while (remaining > 255) {
                    outBase.putByte(output++, (byte) 0);
                    remaining -= 255;
                }
                outBase.putByte(output++, (byte) remaining);
            }
            else {
                outBase.putByte(output++, (byte) length);
            }
        }
        return output;
    }

    private static long emitCopy(ArrayUtil outputBase, long output, int matchOffset, int matchLength)
    {
        if (matchOffset > MAX_DISTANCE || matchOffset < 1) {
            throw new IllegalArgumentException("Unsupported copy offset: " + matchOffset);
        }

        // use short command for small copy with small offset
        if (matchLength <= 8 && matchOffset <= 2048) {
            // 0bMMMP_PPLL 0bPPPP_PPPP

            // encodes matchLength and matchOffset - 1
            matchLength--;
            matchOffset--;

            outputBase.putByte(output++, (byte) (((matchLength) << 5) | ((matchOffset & 0b111) << 2)));
            outputBase.putByte(output++, (byte) (matchOffset >>> 3));

            return output;
        }

        // lzo encodes matchLength - 2
        matchLength -= 2;

        if (matchOffset >= (1 << 15)) {
            // 0b0001_1MMM (0bMMMM_MMMM)* 0bPPPP_PPPP_PPPP_PPLL
            output = encodeMatchLength(outputBase, output, matchLength, 0b0000_0111, 0b0001_1000);
        }
        else if (matchOffset > (1 << 14)) {
            // 0b0001_0MMM (0bMMMM_MMMM)* 0bPPPP_PPPP_PPPP_PPLL
            output = encodeMatchLength(outputBase, output, matchLength, 0b0000_0111, 0b0001_0000);
        }
        else {
            // 0b001M_MMMM (0bMMMM_MMMM)* 0bPPPP_PPPP_PPPP_PPLL
            output = encodeMatchLength(outputBase, output, matchLength, 0b0001_1111, 0b0010_0000);

            // this command encodes matchOffset - 1
            matchOffset--;
        }

        output = encodeOffset(outputBase, output, matchOffset);
        return output;
    }

    private static long encodeOffset(final ArrayUtil outputBase, final long outputOffset, final int offset)
    {
        outputBase.putShort(outputOffset, (short) (offset << 2));
        return outputOffset + 2;
    }

    private static long encodeMatchLength(ArrayUtil outputBase, long output, int matchLength, int baseMatchLength, int command)
    {
        if (matchLength <= baseMatchLength) {
            outputBase.putByte(output++, (byte) (command | matchLength));
        }
        else {
            outputBase.putByte(output++, (byte) command);
            long remaining = matchLength - baseMatchLength;
            while (remaining > 510) {
                outputBase.putShort(output, (short) 0);
                output += SIZE_OF_SHORT;
                remaining -= 510;
            }
            if (remaining > 255) {
                outputBase.putByte(output++, (byte) 0);
                remaining -= 255;
            }
            outputBase.putByte(output++, (byte) remaining);
        }
        return output;
    }

    private static int computeTableSize(int inputSize)
    {
        // smallest power of 2 larger than inputSize
        int target = Integer.highestOneBit(inputSize - 1) << 1;

        // keep it between MIN_TABLE_SIZE and MAX_TABLE_SIZE
        return Math.max(Math.min(target, MAX_TABLE_SIZE), MIN_TABLE_SIZE);
    }
}
