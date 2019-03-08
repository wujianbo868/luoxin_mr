import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;

import java.io.IOException;

/*
 * 该示例展示了MapReduce程序中的基本结构
 */
public class WordCount {

  public static class TokenizerMapper extends MapperBase {

    Record one;

    @Override
    public void map(long recordNum, Record record, TaskContext context) throws IOException {
      one = record;
      context.write(one);
      }
  }

  public static void main(String[] args) throws Exception {

    JobConf job = new JobConf();
    job.setMapperClass(TokenizerMapper.class);
    job.setNumReduceTasks(0);

    InputUtils.addTable(TableInfo.builder().tableName("wc_in").build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName("wc_out").build(), job);

    RunningJob rj = JobClient.runJob(job);
    rj.waitForCompletion();
  }

}
