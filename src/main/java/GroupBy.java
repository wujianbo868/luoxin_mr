import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.conf.JobConf;

import java.io.IOException;

public class GroupBy {
    public static class TokenizerMapper extends MapperBase {

        Record key;
        Record value;

        @Override
        public void setup(TaskContext context) throws IOException {
            key = context.createMapOutputKeyRecord();
            value = context.createMapOutputValueRecord();
        }

        @Override
        public void map(long recordNum, Record record, TaskContext context) throws IOException {
        }
    }

    public static void main(String[] args) {
        JobConf job = new JobConf();
        job.setMapperClass(GroupBy.TokenizerMapper.class);
    }
}
