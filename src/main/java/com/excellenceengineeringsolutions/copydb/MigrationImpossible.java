package com.excellenceengineeringsolutions.copydb;

public class MigrationImpossible extends RuntimeException {

    public MigrationImpossible() {
        super();
    }

    public MigrationImpossible(String message) {
        super(message);
    }

    public MigrationImpossible(String message, Throwable cause) {
        super(message, cause);
    }

    public MigrationImpossible(Throwable cause) {
        super(cause);
    }
}
