package com.rxp.demo;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @author alanchan
 *
 */
public class TestAggregationsDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        aggregationsFunction2(env);
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

    //分组统计sum、max、min、maxby、minby
    public static void aggregationsFunction(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<User> source = source(env);

        KeyedStream<User, String> userTemp=	source.keyBy(user->user.getName());
        DataStream sink = null;
        //1、根据name进行分区统计balance之和 alan1----2500/alan2----600
//		16> User(id=1, name=alan1, pwd=1, email=1@1.com, age=12, balance=1000.0)
//		1> User(id=2, name=alan2, pwd=2, email=2@2.com, age=19, balance=200.0)
//		16> User(id=1, name=alan1, pwd=1, email=1@1.com, age=12, balance=2500.0)
//		1> User(id=2, name=alan2, pwd=2, email=2@2.com, age=19, balance=600.0)
//		16> User(id=1, name=alan1, pwd=1, email=1@1.com, age=12, balance=3000.0)
        sink = userTemp.sum("balance");

        //2、根据name进行分区统计balance的max alan1----1500/alan2----400
//		 1> User(id=2, name=alan2, pwd=2, email=2@2.com, age=19, balance=200.0)
//		 16> User(id=1, name=alan1, pwd=1, email=1@1.com, age=12, balance=1000.0)
//		 16> User(id=1, name=alan1, pwd=1, email=1@1.com, age=12, balance=1500.0)
//		 1> User(id=2, name=alan2, pwd=2, email=2@2.com, age=19, balance=400.0)
//		 16> User(id=1, name=alan1, pwd=1, email=1@1.com, age=12, balance=1500.0)
        sink = userTemp.max("balance");//1@1.com-3000 --  2@2.com-300

        //3、根据name进行分区统计balance的min  alan1----500/alan2---200
//		16> User(id=1, name=alan1, pwd=1, email=1@1.com, age=12, balance=1000.0)
//		16> User(id=1, name=alan1, pwd=1, email=1@1.com, age=12, balance=1000.0)
//		1> User(id=2, name=alan2, pwd=2, email=2@2.com, age=19, balance=200.0)
//		16> User(id=1, name=alan1, pwd=1, email=1@1.com, age=12, balance=500.0)
//		1> User(id=2, name=alan2, pwd=2, email=2@2.com, age=19, balance=200.0)
        sink = userTemp.min("balance");

        //4、根据name进行分区统计balance的maxBy alan2----400/alan1----1500
//		1> User(id=2, name=alan2, pwd=2, email=2@2.com, age=19, balance=200.0)
//		1> User(id=4, name=alan2, pwd=4, email=4@4.com, age=30, balance=400.0)
//		16> User(id=1, name=alan1, pwd=1, email=1@1.com, age=12, balance=1000.0)
//		16> User(id=3, name=alan1, pwd=3, email=3@3.com, age=28, balance=1500.0)
//		16> User(id=3, name=alan1, pwd=3, email=3@3.com, age=28, balance=1500.0)
        sink = userTemp.maxBy("balance");

        //5、根据name进行分区统计balance的minBy alan2----200/alan1----500
//		1> User(id=2, name=alan2, pwd=2, email=2@2.com, age=19, balance=200.0)
//		1> User(id=2, name=alan2, pwd=2, email=2@2.com, age=19, balance=200.0)
//		16> User(id=1, name=alan1, pwd=1, email=1@1.com, age=12, balance=1000.0)
//		16> User(id=1, name=alan1, pwd=1, email=1@1.com, age=12, balance=1000.0)
//		16> User(id=5, name=alan1, pwd=5, email=5@5.com, age=15, balance=500.0)
        sink = userTemp.minBy("balance");

        sink.print();

    }

    public static void aggregationsFunction2(StreamExecutionEnvironment env) throws Exception {
        List list = new ArrayList<Tuple3<Integer, Integer, Integer>>();
        list.add(new Tuple3<>(0,3,6));
        list.add(new Tuple3<>(0,2,5));
        list.add(new Tuple3<>(0,1,6));
        list.add(new Tuple3<>(0,4,3));
        list.add(new Tuple3<>(1,1,9));
        list.add(new Tuple3<>(1,2,8));
        list.add(new Tuple3<>(1,3,10));
        list.add(new Tuple3<>(1,2,9));
        list.add(new Tuple3<>(1,5,7));
        DataStreamSource<Tuple3<Integer, Integer, Integer>> source = env.fromCollection(list);
        KeyedStream<Tuple3<Integer, Integer, Integer>, Integer> tTemp=  source.keyBy(t->t.f0);
        DataStream<Tuple3<Integer, Integer, Integer>> sink =null;

        //按照分区，以第一个Tuple3的元素为基础进行第三列值比较，如果第三列值小于第一个tuple3的第三列值，则进行第三列值替换，其他的不变
//        12> (0,3,6)
//        11> (1,1,9)
//        11> (1,1,8)
//        12> (0,3,5)
//        11> (1,1,8)
//        12> (0,3,5)
//        11> (1,1,8)
//        12> (0,3,3)
//        11> (1,1,7)
        sink =  tTemp.min(2);

//     按照数据分区，以第一个tuple3的元素为基础进行第三列值比较，如果第三列值小于第一个tuple3的第三列值，则进行整个tuple3的替换
//     12> (0,3,6)
//     11> (1,1,9)
//     12> (0,2,5)
//     11> (1,2,8)
//     12> (0,2,5)
//     11> (1,2,8)
//     12> (0,4,3)
//     11> (1,2,8)
//     11> (1,5,7)
        sink = tTemp.minBy(2);

        sink.print();

    }

}


