package org.davidmoten.rxjava3.pool;

import org.junit.Test;

import com.github.davidmoten.junit.Asserts;

public class ConsumersTest {

    @Test
    public void isUtilityClass() {
        Asserts.assertIsUtilityClass(Consumers.class);
        Asserts.assertIsUtilityClass(Consumers.DoNothingHolder.class);
    }
    
    @Test
    public void testDoNothing() throws Throwable {
        Consumers.doNothing().accept(1);
    }

}
