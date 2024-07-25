package com.rxp.demo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @author alanchan
 *
 */
public class TestMapDemo {

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // source

        // transformation
        mapFunction5(env);
        // sink
        // execute
        env.execute();
    }

    // 构造一个list，然后将list中数字乘以2输出，内部匿名类实现
    public static void mapFunction1(StreamExecutionEnvironment env) throws Exception {

        List<Integer> data = new ArrayList<Integer>();
        for (int i = 1; i <= 10; i++) {
            data.add(i);
        }
        DataStreamSource<Integer> source = env.fromCollection(data);

        SingleOutputStreamOperator<Integer> sink = source.map(new MapFunction<Integer, Integer>() {

            @Override
            public Integer map(Integer inValue) throws Exception {
                return inValue * 2;
            }
        });

        sink.print();
//		9> 12
//		4> 2
//		10> 14
//		8> 10
//		13> 20
//		7> 8
//		12> 18
//		11> 16
//		5> 4
//		6> 6
    }

    // 构造一个list，然后将list中数字乘以2输出，lambda实现
    public static void mapFunction2(StreamExecutionEnvironment env) throws Exception {
        List<Integer> data = new ArrayList<Integer>();
        for (int i = 1; i <= 10; i++) {
            data.add(i);
        }
        DataStreamSource<Integer> source = env.fromCollection(data);
        SingleOutputStreamOperator<Integer> sink = source.map(i -> 2 * i);
        sink.print();
//		3> 4
//		4> 6
//		9> 16
//		7> 12
//		10> 18
//		2> 2
//		6> 10
//		5> 8
//		8> 14
//		11> 20
    }

    // 构造User数据源
    public static DataStreamSource<User> source(StreamExecutionEnvironment env) {
        DataStreamSource<User> source = env.fromCollection(Arrays.asList(
                        new User(1, "alan1", "1", "1@1.com", 12, 1000),
                        new User(2, "alan2", "2", "2@2.com", 19, 200),
                        new User(3, "alan1", "3", "3@3.com", 28, 1500),
                        new User(5, "alan1", "5", "5@5.com", 15, 500),
                        new User(4, "alan2", "4", "4@4.com", 30, 400)
                )
        );
        return source;
    }

    // lambda实现用户对象的balance×2和age+5功能
    public static SingleOutputStreamOperator<User> mapFunction3(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<User> source = source(env);

        SingleOutputStreamOperator<User> sink = source.map((MapFunction<User, User>) user -> {
            User user2 = user;
            user2.setAge(user.getAge() + 5);
            user2.setBalance(user.getBalance() * 2);

            return user2;
        });
        sink.print();
//		10> User(id=1, name=alan1, pwd=1, email=1@1.com, age=17, balance=2000.0)
//		14> User(id=4, name=alan2, pwd=4, email=4@4.com, age=35, balance=800.0)
//		11> User(id=2, name=alan2, pwd=2, email=2@2.com, age=24, balance=400.0)
//		12> User(id=3, name=alan1, pwd=3, email=3@3.com, age=33, balance=3000.0)
//		13> User(id=5, name=alan1, pwd=5, email=5@5.com, age=20, balance=1000.0)
        return sink;
    }

    // lambda实现balance*2和age+5后，balance》=2000和age》=20的数据过滤出来
    public static SingleOutputStreamOperator<User> mapFunction4(StreamExecutionEnvironment env) throws Exception {

        SingleOutputStreamOperator<User> sink = mapFunction3(env).filter(user -> user.getBalance() >= 2000 && user.getAge() >= 20);
        sink.print();
//		15> User(id=1, name=alan1, pwd=1, email=1@1.com, age=17, balance=2000.0)
//		1> User(id=3, name=alan1, pwd=3, email=3@3.com, age=33, balance=3000.0)
//		16> User(id=2, name=alan2, pwd=2, email=2@2.com, age=24, balance=400.0)
//		3> User(id=4, name=alan2, pwd=4, email=4@4.com, age=35, balance=800.0)
//		2> User(id=5, name=alan1, pwd=5, email=5@5.com, age=20, balance=1000.0)
//		1> User(id=3, name=alan1, pwd=3, email=3@3.com, age=33, balance=3000.0)
        return sink;
    }

    // lambda实现balance*2和age+5后，balance》=2000和age》=20的数据过滤出来并通过flatmap收集
    public static SingleOutputStreamOperator<User> mapFunction5(StreamExecutionEnvironment env) throws Exception {

        SingleOutputStreamOperator<User> sink = mapFunction4(env).flatMap((FlatMapFunction<User, User>) (user, out) -> {
            if (user.getBalance() >= 3000) {
                out.collect(user);
            }
        }).returns(User.class);

        sink.print();
//		8> User(id=5, name=alan1, pwd=5, email=5@5.com, age=20, balance=1000.0)
//		7> User(id=3, name=alan1, pwd=3, email=3@3.com, age=33, balance=3000.0)
//		6> User(id=2, name=alan2, pwd=2, email=2@2.com, age=24, balance=400.0)
//		9> User(id=4, name=alan2, pwd=4, email=4@4.com, age=35, balance=800.0)
//		5> User(id=1, name=alan1, pwd=1, email=1@1.com, age=17, balance=2000.0)
//		7> User(id=3, name=alan1, pwd=3, email=3@3.com, age=33, balance=3000.0)
//		7> User(id=3, name=alan1, pwd=3, email=3@3.com, age=33, balance=3000.0)
        return sink;
    }

}
