package com.rxp.demo;

import java.util.Arrays;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @author alanchan
 *
 */
public class TestReduceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//		env.setParallelism(4);// 设置数据分区数量
        reduceFunction2(env);
        env.execute();

    }

    // 构造User数据源
    public static DataStreamSource<User> source(StreamExecutionEnvironment env) {
        DataStreamSource<User> source = env.fromCollection(Arrays.asList(
                new User(1, "alan1", "1", "1@1.com", 12, 1000),
                new User(2, "alan2", "2", "2@2.com", 19, 200),
                new User(3, "alan1", "3", "3@3.com", 28, 1500),
                new User(5, "alan1", "5", "5@5.com", 15, 500),
                new User(4, "alan2", "4", "4@4.com", 30, 400)));
        return source;
    }

    // 按照name进行balance进行sum
    public static void reduceFunction1(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<User> source = source(env);

        KeyedStream<User, String> keyedStream = source.keyBy(user -> user.getName());

        SingleOutputStreamOperator<User> sink = keyedStream.reduce(new ReduceFunction<User>() {
            @Override
            public User reduce(User value1, User value2) throws Exception {
                double balance = value1.getBalance() + value2.getBalance();
                return new User(value1.getId(), value1.getName(), "", "", 0, balance);
            }
        });

        //
        sink.print();
    }

    // 按照name进行balance进行sum lambda
    public static void reduceFunction2(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<User> source = source(env);

        KeyedStream<User, String> userKeyBy = source.keyBy(user -> user.getName());


        SingleOutputStreamOperator<User> sink = userKeyBy.reduce((user1, user2) -> {
            User user = user1;
            user.setBalance(user1.getBalance() + user2.getBalance());
            return user;
        });
        sink.print();
    }

}

