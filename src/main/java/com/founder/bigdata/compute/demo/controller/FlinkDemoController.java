package com.founder.bigdata.compute.demo.controller;

import com.founder.bigdata.compute.demo.bean.Student;
import com.founder.bigdata.compute.demo.service.impl.TestFlinkImpl;
import com.founder.bigdata.compute.demo.utils.MySQLSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
    public String get() throws Exception {

        testFlink.test();
/*
        //1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.准备数据
        DataStream<Student> studentDataStreamSource = env.addSource(new MySQLSource());
        testFlink.getStudentFiledDistinct(2,studentDataStreamSource);
        env.execute();
*/

        return "  这是一个Flink代码测试程序...  ";
    }
}
