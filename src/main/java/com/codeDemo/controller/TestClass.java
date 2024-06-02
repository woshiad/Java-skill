package com.codeDemo.controller;

import lombok.Data;

@Data
public class TestClass {

    private String nameA;

    public TestClass tt(){
        TestClass testClass = new TestClass();
        testClass.setNameA("名称A");
        return testClass;
    }
}
