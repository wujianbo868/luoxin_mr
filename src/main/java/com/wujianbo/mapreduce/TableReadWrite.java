package com.wujianbo.mapreduce;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;

import java.io.IOException;

public class TableReadWrite {
    public static class TokenizerMapper extends MapperBase {
        Record one;

        @Override
        public void map(long recordNum, Record record, TaskContext context) throws IOException {
            one = record;
            context.write(one);
        }
    }

    public static void main(String[] args) throws OdpsException {
        JobConf job = new JobConf();
        job.setMapperClass(TableReadWrite.TokenizerMapper.class);
        job.setNumReduceTasks(0);

        InputUtils.addTable(TableInfo.builder().tableName("luoxin_test_mr_1").cols(new String[] {"ytid", "nickname"})
                .build(), job);
        OutputUtils.addTable(TableInfo.builder().tableName("luoxin_test_mr_out").build(), job);

        RunningJob rj = JobClient.runJob(job);
        rj.waitForCompletion();
    }
}
