package org.gitdemo;

import com.codeDemo.JavaCodeDemoApplication;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = JavaCodeDemoApplication.class)
class JavaCodeDemoApplicationTests {

    @Test
    void contextLoads() {
        System.out.println(111);
    }

}
