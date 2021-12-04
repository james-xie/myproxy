package com.gllue.common.concurrent.executor;


import java.util.concurrent.ExecutorService;

/**
 * Base class for executor builders.
 */
public abstract class ExecutorBuilder {

    private final String name;

    public ExecutorBuilder(String name) {
        this.name = name;
    }

    protected String name() {
        return name;
    }
    public abstract ExecutorService build();
}
