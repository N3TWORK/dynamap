package com.n3twork.dynamap;

public enum DynamapReturnValue {

    NONE("NONE"),
    ALL_OLD("ALL_OLD"),
    UPDATED_OLD("UPDATED_OLD"),
    ALL_NEW("ALL_NEW"),
    UPDATED_NEW("UPDATED_NEW");

    private String value;

    private DynamapReturnValue(String value) {
        this.value = value;
    }

    public String toString() {
        return this.value;
    }

}
