package com.wujianbo.udtf;

import com.aliyun.odps.udf.OdpsType;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;

import java.io.IOException;

public class SimpleUDTF extends UDTF {

    @Override
    public OdpsType[] resolve(OdpsType[] signature){
        return new OdpsType[]{OdpsType.STRING, OdpsType.BIGINT};
    }

    @Override
    public void process(Object[] objects) throws UDFException, IOException {
        String a = (String) objects[0];
        long b = objects[1] == null ? 0 : ((String) objects[1]).length();

        forward(a, b);
    }
}
