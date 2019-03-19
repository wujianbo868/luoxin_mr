package com.wujianbo.udtf;

import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;

import java.io.IOException;

public class Lkeyvalue extends BIUDTF {
    private String spliter1 = ";";
    private String spliter2 = ":";
    private Object[] dump = new Object[]{null, null};

    @Override
    public void process(Object[] objects) throws UDFException, IOException {
        // 要分隔的字符串为null, 则返回null
        if ((objects.length==1 && objects[0]==null) || (objects.length>=3 && objects[objects.length-3]==null)){
            forward(getOutputValues(objects, objects.length-3, dump));
            return;
        }

        // 分隔符为空则使用默认的分隔符
        int strIndex = objects.length - 3;
        int spliter1Index = objects.length - 2;
        int spliter2Index = objects.length - 1;
        if (objects.length >= 3) {
            spliter1 = (objects[spliter1Index] == null || "".equals(objects[spliter1Index].toString())) ? ";"
                    : objects[spliter1Index].toString();
            spliter2 = (objects[spliter2Index] == null || "".equals(objects[spliter2Index].toString())) ? ":"
                    : objects[spliter2Index].toString();
        }

        String input = null;
        if (objects.length == 1){
            input = objects[0].toString();
        }else{
            input = objects[strIndex].toString();
        }

        String[] test = input.split(spliter1, -1);
        for (int i = 0; i < test.length; i++) {
            try {
                int idx = test[i].indexOf(spliter2);
                if (idx == -1){
                    //forward(getOutputValues(objects, objects.length-3, new Object[]{test[i], null}));
                    continue;
                }

                String[] result = test[i].split(spliter2);
                if (result.length == 2) {
                    forward(getOutputValues(objects, objects.length-3, result));
                } else{
                    String[] fResult = new String[2];
                    fResult[0] = result[0];
                    if (result.length >= 2){
                        fResult[1] = result[1];
                    }else{
                        fResult[1] = "";
                    }
                    forward(getOutputValues(objects, objects.length-3, fResult));
                }
            } catch (Exception e) {
                continue;
            }
        }
    }

}
