package uk.dioxic.grib;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FloatIntegerTest {

    @Test
    void floatConversion() {

        float f = -0.34534535f;

        int i = Float.floatToIntBits(f);

        System.out.println("float: " + f);
        System.out.println("int: " + i);

        assertThat(Float.intBitsToFloat(i)).isEqualTo(f);

    }

}
