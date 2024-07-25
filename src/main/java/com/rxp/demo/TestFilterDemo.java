package com.rxp.demo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @author alanchan
 *
 */
public class TestFilterDemo {
    // 构造User数据源
    public static DataStreamSource<User> sourceUser(StreamExecutionEnvironment env) {
        DataStreamSource<User> source = env.fromCollection(Arrays.asList(
                new User(1, "alan1", "1", "1@1.com", 12, 1000),
                new User(2, "alan2", "2", "2@2.com", 19, 200),
                new User(3, "alan1", "3", "3@3.com", 28, 1500),
                new User(5, "alan1", "5", "5@5.com", 15, 500),
                new User(4, "alan2", "4", "4@4.com", 30, 400)));
        return source;
    }

    // 构造User数据源
    public static DataStreamSource<Integer> sourceList(StreamExecutionEnvironment env) {
        List<Integer> data = new ArrayList<Integer>();
        for (int i = 1; i <= 10; i++) {
            data.add(i);
        }
        DataStreamSource<Integer> source = env.fromCollection(data);

        return source;
    }

    // 过滤出大于5的数字，内部匿名类
    public static void filterFunction1(StreamExecutionEnvironment env) throws Exception {
        DataStream<Integer> source = sourceList(env);

        SingleOutputStreamOperator<Integer> sink = source.map(new MapFunction<Integer, Integer>() {
            public Integer map(Integer value) throws Exception {
                return value + 1;
            }
        }).filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer value) throws Exception {
                return value > 5;
            }
        });
        sink.print();
//		1> 10
//		14> 7
//		16> 9
//		13> 6
//		2> 11
//		15> 8
    }

    // lambda实现
    public static void filterFunction2(StreamExecutionEnvironment env) throws Exception {
        DataStream<Integer> source = sourceList(env);
        SingleOutputStreamOperator<Integer> sink = source.map(i -> i + 1).filter(value -> value > 5);
        sink.print();
//		12> 7
//		15> 10
//		11> 6
//		13> 8
//		14> 9
//		16> 11
    }

    // 查询user id大于3的记录
    public static void filterFunction3(StreamExecutionEnvironment env) throws Exception {
        DataStream<User> source = sourceUser(env);
        SingleOutputStreamOperator<User> sink = source.filter(user -> user.getId() > 3);
        sink.print();
//		14> User(id=5, name=alan1, pwd=5, email=5@5.com, age=15, balance=500.0)
//		15> User(id=4, name=alan2, pwd=4, email=4@4.com, age=30, balance=400.0)
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        filterFunction3(env);
        env.execute();

    }

}
