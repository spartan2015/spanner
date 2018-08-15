package com.excellenceengineeringsolutions.copydb;

public class MigrationFailed extends RuntimeException {

    public MigrationFailed() {
        super();
    }

    public MigrationFailed(String message) {
        super(message);
    }

    public MigrationFailed(String message, Throwable cause) {
        super(message, cause);
    }

    public MigrationFailed(Throwable cause) {
        super(cause);
    }
}
