package com.n3twork.dynamap;

import com.n3twork.dynamap.test.TestDocumentBean;

public class TestDocumentBeanSubclass extends TestDocumentBean {

    protected TestDocumentBeanSubclass() {
        super();
    }

    public TestDocumentBeanSubclass(String id, Integer sequence) {
        super(id, sequence);
    }

}
