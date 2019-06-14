package com.wujianbo.mapreduce.sql;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

import java.io.IOException;
import java.util.Iterator;

public class GroupBy {
    public static class GroupByMapper extends MapperBase{
        private Record key,value;

        @Override
        public void setup(TaskContext context) throws IOException {
            key = context.createMapOutputKeyRecord();
            value = context.createMapOutputValueRecord();
        }

        @Override
        public void map(long recordNum, Record record, TaskContext context) throws IOException {
            key.setBigint("ytid",record.getBigint("ytid"));
            value.setBigint("ytid",record.getBigint("ytid"));
            value.setString("nickname",record.getString("nickname"));
            context.write(key,value);
        }
    }

    public static class GroupByReducer extends ReducerBase {
        private Record result;

        @Override
        public void setup(TaskContext context) throws IOException {
            result = context.createOutputRecord();
        }

        @Override
        public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
            long count = 0;
            while (values.hasNext()){
                count++;
                values.next();
            }
            result.setBigint("ytid",key.getBigint("ytid"));
            result.setBigint("sum",count);
            context.write(result);
            System.out.println("处理结束");
        }
    }

    public static void main(String[] args) throws OdpsException {
        JobConf job = new JobConf();
        job.setMapperClass(GroupByMapper.class);
        job.setReducerClass(GroupByReducer.class);

        job.setMapOutputKeySchema(SchemaUtils.fromString("ytid:bigint"));
        job.setMapOutputValueSchema(SchemaUtils.fromString("ytid:bigint,nickname:string"));

        InputUtils.addTable(TableInfo.builder().tableName("luoxin_test_mr_1").cols(new String[] {"ytid", "nickname"})
                .build(), job);
        OutputUtils.addTable(TableInfo.builder().tableName("luoxin_test_mr_out3").build(), job);

        RunningJob rj = JobClient.runJob(job);
        rj.waitForCompletion();
    }
}
