package com.rxp.demo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author alanchan
 *
 */
public class TestFlatMapDemo {

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        flatMapFunction3(env);

        env.execute();
    }

    // 构造User数据源
    public static DataStreamSource<String> source(StreamExecutionEnvironment env) {
        List<String> info = new ArrayList<>();
        info.add("i am alanchan");
        info.add("i like hadoop");
        info.add("i like flink");
        info.add("and you ?");

        DataStreamSource<String> dataSource = env.fromCollection(info);

        return dataSource;
    }

    // 将句子以空格进行分割-内部匿名类实现
    public static void flatMapFunction1(StreamExecutionEnvironment env) throws Exception {

        DataStreamSource<String> source = source(env);
        SingleOutputStreamOperator<String> sink = source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] splits = value.split(" ");
                for (String split : splits) {
                    out.collect(split);
                }
            }
        });
        sink.print();
//		11> and
//		10> i
//		8> i
//		9> i
//		8> am
//		10> like
//		11> you
//		10> flink
//		8> alanchan
//		9> like
//		11> ?
//		9> hadoop
    }

    // lambda实现
    public static void flatMapFunction2(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<String> source = source(env);
        SingleOutputStreamOperator<String> sink = source.flatMap((FlatMapFunction<String, String>) (input, out) -> {
            String[] splits = input.split(" ");
            for (String split : splits) {
                out.collect(split);
            }
        }).returns(String.class);

        sink.print();
//		6> i
//		8> and
//		8> you
//		8> ?
//		5> i
//		7> i
//		5> am
//		5> alanchan
//		6> like
//		7> like
//		6> hadoop
//		7> flink
    }

    // lambda实现
    public static void flatMapFunction3(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<String> source = source(env);
        SingleOutputStreamOperator<String> sink = source.flatMap((String input, Collector<String> out) -> Arrays.stream(input.split(" ")).forEach(out::collect))
                .returns(String.class);

        sink.print();
//		8> i
//		11> and
//		10> i
//		9> i
//		10> like
//		11> you
//		8> am
//		11> ?
//		10> flink
//		9> like
//		9> hadoop
//		8> alanchan
    }

}

