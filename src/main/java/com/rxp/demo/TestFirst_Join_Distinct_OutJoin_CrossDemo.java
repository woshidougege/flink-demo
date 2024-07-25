package com.rxp.demo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;



/**
 * @author alanchan
 *
 */
public class TestFirst_Join_Distinct_OutJoin_CrossDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        joinFunction(env);
        env.execute();

    }

    public static void unionFunction(StreamExecutionEnvironment env) throws Exception {
        List<String> info1 = new ArrayList<>();
        info1.add("team A");
        info1.add("team B");

        List<String> info2 = new ArrayList<>();
        info2.add("team C");
        info2.add("team D");

        List<String> info3 = new ArrayList<>();
        info3.add("team E");
        info3.add("team F");

        List<String> info4 = new ArrayList<>();
        info4.add("team G");
        info4.add("team H");

        DataStream<String> source1 = env.fromCollection(info1);
        DataStream<String> source2 = env.fromCollection(info2);
        DataStream<String> source3 = env.fromCollection(info3);
        DataStream<String> source4 = env.fromCollection(info4);

        source1.union(source2).union(source3).union(source4).print();
//        team A
//        team C
//        team E
//        team G
//        team B
//        team D
//        team F
//        team H
    }

    public static void crossFunction(ExecutionEnvironment env) throws Exception {
        // cross,求两个集合的笛卡尔积,得到的结果数为：集合1的条数 乘以 集合2的条数
        List<String> info1 = new ArrayList<>();
        info1.add("team A");
        info1.add("team B");

        List<Tuple2<String, Integer>> info2 = new ArrayList<>();
        info2.add(new Tuple2("W", 3));
        info2.add(new Tuple2("D", 1));
        info2.add(new Tuple2("L", 0));

        DataSource<String> data1 = env.fromCollection(info1);
        DataSource<Tuple2<String, Integer>> data2 = env.fromCollection(info2);

        data1.cross(data2).print();
//        (team A,(W,3))
//        (team A,(D,1))
//        (team A,(L,0))
//        (team B,(W,3))
//        (team B,(D,1))
//        (team B,(L,0))
    }

    public static void outerJoinFunction(ExecutionEnvironment env) throws Exception {
        // Outjoin,跟sql语句中的left join,right join,full join意思一样
        // leftOuterJoin,跟join一样，但是左边集合的没有关联上的结果也会取出来,没关联上的右边为null
        // rightOuterJoin,跟join一样,但是右边集合的没有关联上的结果也会取出来,没关联上的左边为null
        // fullOuterJoin,跟join一样,但是两个集合没有关联上的结果也会取出来,没关联上的一边为null
        List<Tuple2<Integer, String>> info1 = new ArrayList<>();
        info1.add(new Tuple2<>(1, "shenzhen"));
        info1.add(new Tuple2<>(2, "guangzhou"));
        info1.add(new Tuple2<>(3, "shanghai"));
        info1.add(new Tuple2<>(4, "chengdu"));

        List<Tuple2<Integer, String>> info2 = new ArrayList<>();
        info2.add(new Tuple2<>(1, "深圳"));
        info2.add(new Tuple2<>(2, "广州"));
        info2.add(new Tuple2<>(3, "上海"));
        info2.add(new Tuple2<>(5, "杭州"));

        DataSource<Tuple2<Integer, String>> data1 = env.fromCollection(info1);
        DataSource<Tuple2<Integer, String>> data2 = env.fromCollection(info2);
        // left join
//        eft join:7> (1,shenzhen,深圳)
//        left join:2> (3,shanghai,上海)
//        left join:8> (4,chengdu,未知)
//        left join:16> (2,guangzhou,广州)
        data1.leftOuterJoin(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {

            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                Tuple3<Integer, String, String> tuple = new Tuple3();
                if (second == null) {
                    tuple.setField(first.f0, 0);
                    tuple.setField(first.f1, 1);
                    tuple.setField("未知", 2);
                } else {
                    // 另外一种赋值方式，和直接用构造函数赋值相同
                    tuple.setField(first.f0, 0);
                    tuple.setField(first.f1, 1);
                    tuple.setField(second.f1, 2);
                }
                return tuple;
            }
        }).print("left join");

        // right join
//        right join:2> (3,shanghai,上海)
//        right join:7> (1,shenzhen,深圳)
//        right join:15> (5,--,杭州)
//        right join:16> (2,guangzhou,广州)
        data1.rightOuterJoin(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {

            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                Tuple3<Integer, String, String> tuple = new Tuple3();
                if (first == null) {
                    tuple.setField(second.f0, 0);
                    tuple.setField("--", 1);
                    tuple.setField(second.f1, 2);
                } else {
                    // 另外一种赋值方式，和直接用构造函数赋值相同
                    tuple.setField(first.f0, 0);
                    tuple.setField(first.f1, 1);
                    tuple.setField(second.f1, 2);
                }
                return tuple;
            }
        }).print("right join");

        // fullOuterJoin
//        fullOuterJoin:2> (3,shanghai,上海)
//        fullOuterJoin:8> (4,chengdu,--)
//        fullOuterJoin:15> (5,--,杭州)
//        fullOuterJoin:16> (2,guangzhou,广州)
//        fullOuterJoin:7> (1,shenzhen,深圳)
        data1.fullOuterJoin(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {

            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                Tuple3<Integer, String, String> tuple = new Tuple3();
                if (second == null) {
                    tuple.setField(first.f0, 0);
                    tuple.setField(first.f1, 1);
                    tuple.setField("--", 2);
                } else if (first == null) {
                    tuple.setField(second.f0, 0);
                    tuple.setField("--", 1);
                    tuple.setField(second.f1, 2);
                } else {
                    // 另外一种赋值方式，和直接用构造函数赋值相同
                    tuple.setField(first.f0, 0);
                    tuple.setField(first.f1, 1);
                    tuple.setField(second.f1, 2);
                }
                return tuple;
            }
        }).print("fullOuterJoin");
    }

    public static void joinFunction(ExecutionEnvironment env) throws Exception {
        List<Tuple2<Integer, String>> info1 = new ArrayList<>();
        info1.add(new Tuple2<>(1, "shenzhen"));
        info1.add(new Tuple2<>(2, "guangzhou"));
        info1.add(new Tuple2<>(3, "shanghai"));
        info1.add(new Tuple2<>(4, "chengdu"));

        List<Tuple2<Integer, String>> info2 = new ArrayList<>();
        info2.add(new Tuple2<>(1, "深圳"));
        info2.add(new Tuple2<>(2, "广州"));
        info2.add(new Tuple2<>(3, "上海"));
        info2.add(new Tuple2<>(5, "杭州"));

        DataSource<Tuple2<Integer, String>> data1 = env.fromCollection(info1);
        DataSource<Tuple2<Integer, String>> data2 = env.fromCollection(info2);

        //

//        join:2> ((3,shanghai),(3,上海))
//        join:16> ((2,guangzhou),(2,广州))
//        join:7> ((1,shenzhen),(1,深圳))
        data1.join(data2).where(0).equalTo(0).print("join");

//        join2:2> (3,上海,shanghai)
//        join2:7> (1,深圳,shenzhen)
//        join2:16> (2,广州,guangzhou)
        DataSet<Tuple3<Integer, String, String>> data3 = data1.join(data2).where(0).equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {

                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        return new Tuple3<Integer, String, String>(first.f0, second.f1, first.f1);
                    }
                });
        data3.print("join2");

    }

    public static void firstFunction(ExecutionEnvironment env) throws Exception {
        List<Tuple2<Integer, String>> info = new ArrayList<>();
        info.add(new Tuple2(1, "Hadoop"));
        info.add(new Tuple2(1, "Spark"));
        info.add(new Tuple2(1, "Flink"));
        info.add(new Tuple2(2, "Scala"));
        info.add(new Tuple2(2, "Java"));
        info.add(new Tuple2(2, "Python"));
        info.add(new Tuple2(3, "Linux"));
        info.add(new Tuple2(3, "Window"));
        info.add(new Tuple2(3, "MacOS"));

        DataSet<Tuple2<Integer, String>> dataSet = env.fromCollection(info);
        // 前几个
//	        dataSet.first(4).print();
//	        (1,Hadoop)
//	        (1,Spark)
//	        (1,Flink)
//	        (2,Scala)

        // 按照tuple2的第一个元素进行分组，查出每组的前2个
//	        dataSet.groupBy(0).first(2).print();
//	        (3,Linux)
//	        (3,Window)
//	        (1,Hadoop)
//	        (1,Spark)
//	        (2,Scala)
//	        (2,Java)

        // 按照tpule2的第一个元素进行分组，并按照倒序排列，查出每组的前2个
        dataSet.groupBy(0).sortGroup(1, Order.DESCENDING).first(2).print();
//	        (3,Window)
//	        (3,MacOS)
//	        (1,Spark)
//	        (1,Hadoop)
//	        (2,Scala)
//	        (2,Python)
    }

    public static void distinctFunction(ExecutionEnvironment env) throws Exception {
        List list = new ArrayList<Tuple3<Integer, Integer, Integer>>();
        list.add(new Tuple3<>(0, 3, 6));
        list.add(new Tuple3<>(0, 2, 5));
        list.add(new Tuple3<>(0, 3, 6));
        list.add(new Tuple3<>(1, 1, 9));
        list.add(new Tuple3<>(1, 2, 8));
        list.add(new Tuple3<>(1, 2, 8));
        list.add(new Tuple3<>(1, 3, 9));

        DataSet<Tuple3<Integer, Integer, Integer>> source = env.fromCollection(list);
        // 去除tuple3中元素完全一样的
        source.distinct().print();
//		(1,3,9)
//		(0,3,6)
//		(1,1,9)
//		(1,2,8)
//		(0,2,5)
        // 去除tuple3中第一个元素一样的，只保留第一个
        // source.distinct(0).print();
//		(1,1,9)
//		(0,3,6)
        // 去除tuple3中第一个和第三个相同的元素，只保留第一个
        // source.distinct(0,2).print();
//		(0,3,6)
//		(1,1,9)
//		(1,2,8)
//		(0,2,5)
    }

    public static void distinctFunction2(ExecutionEnvironment env) throws Exception {
        DataSet<User> source = env.fromCollection(Arrays.asList(new User(1, "alan1", "1", "1@1.com", 18, 3000), new User(2, "alan2", "2", "2@2.com", 19, 200),
                new User(3, "alan1", "3", "3@3.com", 18, 1000), new User(5, "alan1", "5", "5@5.com", 28, 1500), new User(4, "alan2", "4", "4@4.com", 20, 300)));

//		source.distinct("name").print();
//		User(id=2, name=alan2, pwd=2, email=2@2.com, age=19, balance=200.0)
//		User(id=1, name=alan1, pwd=1, email=1@1.com, age=18, balance=3000.0)

        source.distinct("name", "age").print();
//		User(id=1, name=alan1, pwd=1, email=1@1.com, age=18, balance=3000.0)
//		User(id=2, name=alan2, pwd=2, email=2@2.com, age=19, balance=200.0)
//		User(id=5, name=alan1, pwd=5, email=5@5.com, age=28, balance=1500.0)
//		User(id=4, name=alan2, pwd=4, email=4@4.com, age=20, balance=300.0)
    }

    public static void distinctFunction3(ExecutionEnvironment env) throws Exception {
        DataSet<User> source = env.fromCollection(Arrays.asList(new User(1, "alan1", "1", "1@1.com", 18, -1000), new User(2, "alan2", "2", "2@2.com", 19, 200),
                new User(3, "alan1", "3", "3@3.com", 18, -1000), new User(5, "alan1", "5", "5@5.com", 28, 1500), new User(4, "alan2", "4", "4@4.com", 20, -300)));
        // 针对balance增加绝对值去重
        source.distinct(new KeySelector<User, Double>() {
            @Override
            public Double getKey(User value) throws Exception {
                return Math.abs(value.getBalance());
            }
        }).print();
//		User(id=5, name=alan1, pwd=5, email=5@5.com, age=28, balance=1500.0)
//		User(id=2, name=alan2, pwd=2, email=2@2.com, age=19, balance=200.0)
//		User(id=1, name=alan1, pwd=1, email=1@1.com, age=18, balance=-1000.0)
//		User(id=4, name=alan2, pwd=4, email=4@4.com, age=20, balance=-300.0)
    }

    public static void distinctFunction4(ExecutionEnvironment env) throws Exception {
        List<String> info = new ArrayList<>();
        info.add("Hadoop,Spark");
        info.add("Spark,Flink");
        info.add("Hadoop,Flink");
        info.add("Hadoop,Flink");

        DataSet<String> source = env.fromCollection(info);
        source.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                System.err.print("come in ");
                for (String token : value.split(",")) {
                    out.collect(token);
                }
            }
        });
        source.distinct().print();
    }

}

