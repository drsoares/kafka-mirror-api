package com.drsoares.mirror;

public interface Mirror {

    /**
     *  Starts the mirror
     */
    void start();

    /**
     * Gracefully stops the mirror
     */
    void stop();
}
