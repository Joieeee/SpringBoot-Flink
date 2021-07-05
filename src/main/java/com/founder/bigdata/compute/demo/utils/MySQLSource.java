package com.founder.bigdata.compute.demo.utils;

import com.founder.bigdata.compute.demo.bean.Student;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * description:
 * </p>
 *
 * @author Guan 2021/07/02 15:17
 * @program demo
 */
public class MySQLSource extends RichParallelSourceFunction<Student> {

    private Connection conn = null;
    private PreparedStatement ps = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        //加载驱动,开启连接
        conn = DriverManager.getConnection("jdbc:mysql://192.168.88.161:3306/flink_test", "root", "123456");
        String sql = "select id,name,age from `t_student`";
        ps = conn.prepareStatement(sql);
    }

//    private boolean flag = true;

    @Override
    public void run(SourceContext ctx) throws Exception {
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            int id = resultSet.getInt("id");
            String name = resultSet.getString("name");
            int age = resultSet.getInt("age");
            //封装数据到student集合中
            ctx.collect(new Student(id, name, age));
        }
//        TimeUnit.SECONDS.sleep(5);
    }

    //取消任务 执行cancel
    @Override
    public void cancel() {
//        flag = false;
    }

    //结束任务,关闭连接
    @Override
    public void close() throws Exception {
        if (conn != null) {
            conn.close();
        }
        if (ps != null) {
            ps.close();
        }
    }
}