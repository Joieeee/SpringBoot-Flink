package com.founder.bigdata.compute.demo.utils;

import com.founder.bigdata.compute.demo.bean.Student;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

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
        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/db01", "root", "123456");
        String sql = "select id,name,age from `t_student`";
        ps = conn.prepareStatement(sql);
    }

//    private boolean flag = true;

    @Override
    public void run(SourceContext ctx) throws Exception {
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            String id = resultSet.getString("id");
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