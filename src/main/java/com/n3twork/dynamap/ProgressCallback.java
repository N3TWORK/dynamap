package com.n3twork.dynamap;

public interface ProgressCallback {

    /**
     * Report progress back to the caller. The caller can return a signal to cancel the request.
     * @param progressCount a number to indicate the progress made
     * @return true if process should continue, false if the process should be cancelled
     */
    boolean reportProgress(int progressCount);

}
