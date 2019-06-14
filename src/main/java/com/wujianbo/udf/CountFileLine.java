package com.wujianbo.udf;

import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.UDF;
import com.aliyun.odps.udf.UDFException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class CountFileLine extends UDF {
    ExecutionContext ctx;
    long fileResourceLineCount;

    @Override
    public void setup(ExecutionContext ctx) throws UDFException {
        this.ctx = ctx;
        try {
                InputStream in = ctx.readResourceFileAsStream("file_resource.txt");
                BufferedReader br = new BufferedReader(new InputStreamReader(in));
                String line;
                fileResourceLineCount = 0;
                while ((line = br.readLine()) != null) {
                    fileResourceLineCount++;
                }
                br.close();
        } catch (IOException e) {
            throw new UDFException(e);
        }
    }

    public String evaluate() {
        return "file-count:"+fileResourceLineCount;
    }

}
