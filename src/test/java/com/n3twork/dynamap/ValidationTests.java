package com.n3twork.dynamap;

import org.testng.annotations.Test;

public class ValidationTests {

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalArgumentException: Table TestWithTtl has 2 ttl fields defined. At most one is allowed.")
    public void schemaWithTwoTtlFieldsShouldFailValidation() {
        new SchemaRegistry(getClass().getResourceAsStream("/TestSchemaWithTwoTtlFields.json"));
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalArgumentException: Table Test, type TestDocument, has invalid equals fields: \\[not_a_valid_field\\].")
    public void badEqualsFieldsShouldFailValidation() {
        new SchemaRegistry(getClass().getResourceAsStream("/BadEqualsFields.json"));
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "java.lang.IllegalArgumentException: Table Test, type TestDocument, has invalid hashCode fields: \\[not_a_valid_field\\].")
    public void badHashCodeFieldsShouldFailValidation() {
        new SchemaRegistry(getClass().getResourceAsStream("/BadHashCodeFields.json"));
    }
}
