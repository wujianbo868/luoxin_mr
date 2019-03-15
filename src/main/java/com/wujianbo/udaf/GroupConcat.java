package com.wujianbo.udaf;

import com.aliyun.odps.io.Text;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.udf.Aggregator;
import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.annotation.Resolve;


@Resolve("string->string")
public class GroupConcat extends Aggregator {


    @Override
    public void setup(ExecutionContext ctx){
    }

    @Override
    public Writable newBuffer() {
        return new Text();
    }

    @Override
    public void iterate(Writable writable, Writable[] writables){
        Text rslt = (Text) writable;
        for(Writable iterm: writables){
            Text text = (Text)iterm;
            rslt.set(new String(rslt.getBytes()) +text.toString());
        }
    }

    @Override
    public Writable terminate(Writable writable){
        return writable;
    }

    @Override
    public void merge(Writable writable, Writable writable1){
        Text rslt = (Text) writable;
        Text partial = (Text) writable1;
        rslt.set(rslt.toString() + partial.toString());
    }
}
