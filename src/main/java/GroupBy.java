import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;

import java.io.IOException;

public class GroupBy {
    public static class TokenizerMapper extends MapperBase {

        Record one;

        @Override
        public void map(long recordNum, Record record, TaskContext context) throws IOException {
            one = record;
            context.write(one);
        }
    }
}
