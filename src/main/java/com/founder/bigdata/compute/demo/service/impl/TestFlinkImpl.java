package com.founder.bigdata.compute.demo.service.impl;

import com.founder.bigdata.compute.demo.bean.Student;
import com.founder.bigdata.compute.demo.dao.TestFlinkS;
import com.founder.bigdata.compute.demo.utils.MySQLSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.stereotype.Service;


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
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.准备数据
        DataStream<Student> studentDataStreamSource = env.addSource(new MySQLSource());
        studentDataStreamSource.print();

        //3.数据处理转换(去重操作)
        SingleOutputStreamOperator<String> outputStreamOperator = studentDataStreamSource.keyBy(Student::getName).process(new DataDistinct());

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

    public void getStudentFiledDistinct(Integer type,DataStream<Student> studentDataStreamSource) {
        //1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //3.数据处理转换(去重操作)
        if (type == 0) {
            DataStream<String> outputStreamOperator = studentDataStreamSource.keyBy("id").process(new DataDistinct());
            //4.输出结果
            outputStreamOperator.print();
        } else if (type == 1) {
            DataStream<String> outputStreamOperator = studentDataStreamSource.keyBy("name").process(new DataDistinct());
            outputStreamOperator.print();
        } else if (type == 2) {
            DataStream<String> outputStreamOperator = studentDataStreamSource.keyBy("age").process(new DataDistinct());
            outputStreamOperator.print();
        } else if (type == 3) {
            DataStream<String> outputStreamOperator = studentDataStreamSource.keyBy("id", "name").process(new DataDistinct());
            outputStreamOperator.print();
        } else if (type == 4) {
            DataStream<String> outputStreamOperator = studentDataStreamSource.keyBy("id", "age").process(new DataDistinct());
            outputStreamOperator.print();
        } else if (type == 5) {
            DataStream<String> outputStreamOperator = studentDataStreamSource.keyBy("name", "age").process(new DataDistinct());
            outputStreamOperator.print();
        } else{
            DataStream<String> outputStreamOperator = studentDataStreamSource.keyBy("id", "age", "name").process(new DataDistinct());
            outputStreamOperator.print();
        }
        try {
            //5.触发执行
            env.execute();
        } catch (Exception e) {
            System.out.println("Error executing flink job: " + e.getMessage());
        }
    }
}
