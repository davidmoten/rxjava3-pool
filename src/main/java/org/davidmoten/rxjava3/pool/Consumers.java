package org.davidmoten.rxjava3.pool;

import io.reactivex.rxjava3.functions.Consumer;

final class Consumers {

    private Consumers() {
        // prevent instantiation
    }

    static final class DoNothingHolder {
        
        private DoNothingHolder() {
            // prevent instantiation    
        }
        
        static final Consumer<Object> value = new Consumer<Object>() {

            @Override
            public void accept(Object arg0) throws Exception {
                // do nothing
            }

        };
    }

    @SuppressWarnings("unchecked")
    static <T> Consumer<T> doNothing() {
        return (Consumer<T>) DoNothingHolder.value;
    }

}
