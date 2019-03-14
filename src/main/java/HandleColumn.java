import com.aliyun.odps.OdpsException;
import com.aliyun.odps.counter.Counter;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

import java.io.IOException;

public class HandleColumn {
    public static class TokenizerMapper extends MapperBase {
        Record one;
        Counter gCnt;

        @Override
        public void setup(TaskContext context) throws IOException {
            one = context.createMapOutputValueRecord();
            gCnt = context.getCounter("MyCounters", "global_counts");
        }

        @Override
        public void map(long recordNum, Record record, TaskContext context) throws IOException {
            one.setBigint("ytid",record.getBigint("ytid"));
            one.setString("nickname",record.getString("nickname"));
            one.setString("handle_column", record.getBigint("ytid")+record.getString("nickname"));
            context.write(one);
            gCnt.increment(1);
            System.out.println(gCnt.getValue());
        }
    }

    public static void main(String[] args) throws OdpsException {
        JobConf job = new JobConf();
        job.setMapperClass(HandleColumn.TokenizerMapper.class);
        job.setNumReduceTasks(0);

        job.setMapOutputValueSchema(SchemaUtils.fromString("ytid:bigint,nickname:string,handle_column:string"));

        InputUtils.addTable(TableInfo.builder().tableName("luoxin_test_mr_1").cols(new String[] {"ytid", "nickname"})
                .build(), job);
        OutputUtils.addTable(TableInfo.builder().tableName("luoxin_test_mr_out2").build(), job);

        RunningJob rj = JobClient.runJob(job);
        rj.waitForCompletion();
    }
}
