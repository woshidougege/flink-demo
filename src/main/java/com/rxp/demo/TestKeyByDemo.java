package com.rxp.demo;

import java.util.Arrays;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @author alanchan
 *
 */
public class TestKeyByDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//		env.setParallelism(4);// 设置数据分区数量
        keyByFunction6(env);
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

    // 按照name进行keyby 对于POJO类型，KeyBy可以通过keyBy(fieldName)指定字段进行分区
    public static void keyByFunction1(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<User> source = source(env);

        KeyedStream<User, String> sink = source.keyBy(new KeySelector<User, String>() {
            @Override
            public String getKey(User value) throws Exception {
                return value.getName();
            }
        });

        sink.map(user -> {
            System.out.println("当前线程ID：" + Thread.currentThread().getId() + ",user:" + user.toString());
            return user;
        });

        sink.print();

    }

    // lambda 对于POJO类型，KeyBy可以通过keyBy(fieldName)指定字段进行分区
    public static void keyByFunction2(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<User> source = source(env);
        KeyedStream<User, String> sink = source.keyBy(user -> user.getName());

        // 演示keyby后的数据输出
        sink.map(user -> {
            System.out.println("当前线程ID：" + Thread.currentThread().getId() + ",user:" + user.toString());
            return user;
        });

        sink.print();

    }

    // 对于Tuple类型，KeyBy可以通过keyBy(fieldPosition)指定字段进行分区。lambda
    public static void keyByFunction3(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<User> source = source(env);
        SingleOutputStreamOperator<Tuple2<String, User>> userTemp = source.map((MapFunction<User, Tuple2<String, User>>) user -> {
            return new Tuple2<String, User>(user.getName(), user);
        }).returns(Types.TUPLE(Types.STRING, Types.POJO(User.class)));

        KeyedStream<Tuple2<String, User>, Tuple> sink = userTemp.keyBy(0);

        // 演示keyby后的数据输出
        sink.map(user -> {
            System.out.println("当前线程ID：" + Thread.currentThread().getId() + ",user:" + user.f1.toString());
            return user.f1;
        });
        sink.print();

    }

    // 对于Tuple类型，KeyBy可以通过keyBy(fieldPosition)指定字段进行分区。
    public static void keyByFunction4(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<User> source = source(env);
        SingleOutputStreamOperator<Tuple2<String, User>> userTemp = source.map(new MapFunction<User, Tuple2<String, User>>() {

            @Override
            public Tuple2<String, User> map(User value) throws Exception {
                return new Tuple2<String, User>(value.getName(), value);
            }
        });

        KeyedStream<Tuple2<String, User>, String> sink = userTemp.keyBy(new KeySelector<Tuple2<String, User>, String>() {

            @Override
            public String getKey(Tuple2<String, User> value) throws Exception {
                return value.f0;
            }
        });

        // 演示keyby后的数据输出
        sink.map(user -> {
            System.out.println("当前线程ID：" + Thread.currentThread().getId() + ",user:" + user.f1.toString());
            return user.f1;
        });

//		sink.map(new MapFunction<Tuple2<String, User>, String>() {
//
//			@Override
//			public String map(Tuple2<String, User> value) throws Exception {
//				System.out.println("当前线程ID：" + Thread.currentThread().getId() + ",user:" + value.f1.toString());
//				return null;
//			}});
        sink.print();
    }

    // 对于一般类型，如上，KeyBy可以通过keyBy(new KeySelector {...})指定字段进行分区。
    // 按照name的前4位进行keyby
    public static void keyByFunction5(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<User> source = source(env);
        KeyedStream<User, String> sink = source.keyBy(new KeySelector<User, String>() {

            @Override
            public String getKey(User value) throws Exception {
//				String temp = value.getName().substring(0, 4);
                return value.getName().substring(0, 4);
            }
        });

        sink.map(user -> {
            System.out.println("当前线程ID：" + Thread.currentThread().getId() + ",user:" + user.toString());
            return user;
        });
        sink.print();

    }

    // 对于一般类型，如上，KeyBy可以通过keyBy(new KeySelector {...})指定字段进行分区。 lambda
    // 按照name的前4位进行keyby
    public static void keyByFunction6(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<User> source = source(env);
        KeyedStream<User, String> sink = source.keyBy(user -> user.getName().substring(0, 4));
        sink.map(user -> {
            System.out.println("当前线程ID：" + Thread.currentThread().getId() + ",user:" + user.toString());
            return user;
        });
        sink.print();
    }

}

