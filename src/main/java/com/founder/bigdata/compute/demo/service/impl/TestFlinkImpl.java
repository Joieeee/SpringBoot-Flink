package com.founder.bigdata.compute.demo.service.impl;

import com.founder.bigdata.compute.demo.bean.Student;
import com.founder.bigdata.compute.demo.dao.TestFlinkS;
import com.founder.bigdata.compute.demo.utils.MySQLSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.swing.*;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * <p>
 * description:
 * </p>
 *
 * @author Guan 2021/07/05 9:12
 * @program demo
 */
@Service
public class TestFlinkImpl implements TestFlinkS {
    @Override
    public void test() {
        System.out.println("=========  流程开始   >>>>>>>>>  演示Flink Job  <<<<<<<<<<<<<<   ========== ");
        //准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        //准备数据
//        DataStreamSource<String> dataStreamSource = env.fromElements("qqq", "aaa", "ee");

        DataStream<Student> studentDS = env.addSource(new MySQLSource()).setParallelism(1);

        studentDS.print();

        try {
            env.execute();

        } catch (Exception e) {
            System.out.println("Error executing flink job: " + e.getMessage());
        }
        System.out.println("******演示结束******");
    }
}
