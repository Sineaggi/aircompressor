package io.airlift.compress.zstd;

import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class ArrayUtilTest {

    private final byte[] bytes = new byte[] {88, 88, 88, 88, 97, 98, 99, 100, 101, 102, 103, 104, 32, 97, 98, 99, 100, 101, 102, 103, 104, 32, 97, 98, 99, 100, 101, 102, 103, 104, 32, 97, 98, 99, 100, 101, 102, 103, 104, 32, 97, 98, 99, 100, 101, 102, 103, 104, 32, 97, 98, 99, 100, 101, 102, 103, 104, 32, 65, 66, 67};

    private byte[] bytes() {
        return bytes.clone();
    }

    @Test
    public void testBytes() {
        assertThat(bytes().length)
                .isEqualTo(61);
    }

    @Test
    public void testInt() {
        ArrayUtil arrayUtil = ArrayUtil.ofArray(bytes());

        assertThat(arrayUtil.getInt(2))
                .isEqualTo(1650546776);
    }
    @Test
    public void testLong() {
        ArrayUtil arrayUtil = ArrayUtil.ofArray(bytes());

        assertThat(arrayUtil.getLong(2))
                .isEqualTo(7378413942531512408L);

        //7378413942531512408
    }
}
