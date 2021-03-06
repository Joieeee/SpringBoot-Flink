package com.founder.bigdata.compute.demo.service.impl;


import com.founder.bigdata.compute.demo.bean.Student;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * <p>
 * description: 去重
 * </p>
 *
 * @author Guan 2021/07/06 9:21
 * @program demo
 */
public class DataDistinct extends ProcessFunction<Student, String> {

    /**
     * 定义变量表示数据的状态
     */
    private ValueState<Boolean> existState;
//    private ValueState<String> existState;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化状态，name是dataState
        StateTtlConfig ttlConfig = StateTtlConfig
                //设置state存活时间10s
                .newBuilder(Time.seconds(10))
                //设置过期时间更新方式(-- OnReadAndWrite -读取时也更新 ; -- onCreateAndWrite -仅在创建和写入时更新)
                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                //设置数据过期的状态,过期的数据不能再被访问(-- NeverReturnExpired -不返回过期数据 ; -- ReturnExpiredIfNotCleanedUp -会返回过期但未清理的数据)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                //处理完1000个状态查询时候，会启用一次CompactFilter
                .cleanupInRocksdbCompactFilter(1000)
                .build();

        ValueStateDescriptor<Boolean> existStateDesc = new ValueStateDescriptor<>("dataState", Boolean.class);
//        ValueStateDescriptor<String> existStateDesc = new ValueStateDescriptor<>("dataState", String.class);
        existStateDesc.enableTimeToLive(ttlConfig);
        existState = this.getRuntimeContext().getState(existStateDesc);
    }

    /**
     * 去重操作输出
     * 通过判断状态来实现去重
     *
     * @param student 传入的数据类型
     * @param ctx     作为中间变量
     * @param out     输出类型
     * @throws Exception
     */
    @Override
    public void processElement(Student student, Context ctx, Collector<String> out) throws Exception {
        //如果不存在则收集输出
        if (existState.value() == null) {
            existState.update(true);
            out.collect(student.getId() + ": \t(" + student.getName() + " , " + student.getAge() + ")");
        }

        /*int i = 0;

        if (existState.value() == null) {
            existState.update(student.toString());
            out.collect(student.getId() + ": \t(" + student.getName() + " , " + student.getAge() + ")");
        } else if (existState.value().equals(student.toString())) {
            i++;
            System.out.println(student.toString() + "\t全字段重复" + i + "次");
        }*/
    }


}

