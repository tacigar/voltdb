package org.voltdb.exceptions;

public class SimulatedExitException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final int status;
    public SimulatedExitException(int status) {
        this.status = status;
    }
    public int getStatus() {
        return status;
    }
}