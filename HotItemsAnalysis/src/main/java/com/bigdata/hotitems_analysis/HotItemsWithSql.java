package com.bigdata.hotitems_analysis;

import com.bigdata.hotitems_analysis.beans.UserBehavior;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class HotItemsWithSql {

    public static void main(String[] args) throws Exception {

        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2.读取数据，创建dataStream
        DataStream<String> inputStream = env.readTextFile("D:\\IdeaProjects\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv");

        // 3.转换为POJO，分配时间戳和watermark
        DataStream<UserBehavior> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(Long.parseLong(fields[0]), Long.parseLong(fields[1]), Integer.parseInt(fields[2]), fields[3], Long.parseLong(fields[4]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior element) {
                return element.getTimestamp() * 1000L;
            }
        });

        // 4. 创建表执行环境
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, environmentSettings);

        // 5.将流转为表
        Table dataTable = tableEnv.fromDataStream(dataStream, "itemId, behavior, timestamp.rowtime as ts");

        // 6.分组开窗
        // table api
        Table windowAggTable = dataTable.filter("behavior = 'pv'")
                .window(Slide.over("1.hours").every("5.minutes").on("ts").as("w"))
                .groupBy("itemId, w")
                .select("itemId, w.end as windowEnd, itemId.count as cnt");

        // 7.利用开窗函数，对count值进行排序并获取row_number，得到Top N
        // SQL
        DataStream<Row> aggStream = tableEnv.toAppendStream(windowAggTable, Row.class);
        tableEnv.createTemporaryView("agg", aggStream, "itemId, windowEnd, cnt");

        Table resultTable = tableEnv.sqlQuery("select * from " +
                " (select *, row_number() over (partition by windowEnd order by cnt desc) as rn " +
                " from agg " +
                " ) where rn <= 5 ");


        // 纯SQL实现
        tableEnv.createTemporaryView("data_table", dataStream, "itemId, behavior, timestamp.rowtime as ts");

        Table resultSqlTable = tableEnv.sqlQuery("select * from " +
                " (select *, row_number() over (partition by windowEnd order by cnt desc) as rn " +
                " from (" +
                " select itemId, count(itemId) as cnt, HOP_END(ts, interval '5' minute, interval '1' hour) as windowEnd" +
                " from data_table " +
                " where behavior = 'pv' " +
                " group by itemId, HOP(ts, interval '5' minute, interval '1' hour)" +
                "   )" +
                " ) where rn <= 5 ");

        // tableEnv.toRetractStream(resultTable, Row.class).print();
        tableEnv.toRetractStream(resultSqlTable, Row.class).print();


        // 执行
        env.execute(" hot item analysis with sql");

    }

}
