package com.founder.bigdata.compute.demo.service.impl;

import com.founder.bigdata.compute.demo.bean.Student;
import com.founder.bigdata.compute.demo.dao.TestFlinkS;
import com.founder.bigdata.compute.demo.utils.MySQLSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.stereotype.Service;

import java.util.function.Function;


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
        //1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        //2.准备数据
//        DataStreamSource<String> dataStreamSource = env.fromElements("qqq", "aaa", "ee");
        DataStream<Student> studentDataStreamSource = env.addSource(new MySQLSource());
        studentDataStreamSource.print();

        //3.数据处理转换(去重操作)
        SingleOutputStreamOperator<Tuple2<String, Tuple2<String, Integer>>> outputStreamOperator = studentDataStreamSource.keyBy(Student::getId).process(new DataDistinct());
        //4.输出结果
        outputStreamOperator.print();
        try {
            //5.触发执行
            env.execute();

        } catch (Exception e) {
            System.out.println("Error executing flink job: " + e.getMessage());
        }
        System.out.println("******演示结束******");
    }
}
