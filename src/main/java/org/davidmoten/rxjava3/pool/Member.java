package org.davidmoten.rxjava3.pool;

public interface Member<T> extends Checkin {

    T value();

    /**
     * This method should not throw. Feel free to add logging so that you are aware
     * of a problem with disposal.
     */
    void disposeValue();

}
