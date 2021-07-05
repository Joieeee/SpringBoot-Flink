package com.founder.bigdata.compute.demo.controller;

import com.founder.bigdata.compute.demo.service.impl.TestFlinkImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * <p>
 * description:Springboot与Flink集成
 * </p>
 *
 * @author Guan 2021/07/02 9:32
 * @program demo
 */
@RestController
public class FlinkDemoController {

    @Autowired
    TestFlinkImpl testFlink;

    @RequestMapping("/test")
    public String get() {

        testFlink.test();

        return "  >>>>>>>>>>>>>>>>>>>>>  ";
    }
}
