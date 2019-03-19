package com.wujianbo.udtf;

import com.aliyun.odps.udf.OdpsType;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;

import java.io.IOException;
import java.util.Arrays;

public abstract class BIUDTF extends UDTF {
    private String result;

    public String getResult(){
        return result;
    }

    public void clear(){
        result = null;
    }

    /**
     * 得到该UDTF的输出类型列表
     * @param signature UDTF的输入类型列表
     * @param oldColumns signature中保存原样输出的前oldColumns数
     * @param newColumns 新增输出的类型
     * @return 该UDTF的输出类型列表
     */
    protected OdpsType[] getOutputTypes(OdpsType[] signature, int oldColumns, OdpsType[] newColumns) {
        OdpsType[] result = new OdpsType[oldColumns + newColumns.length];
        for (int i=0; i<oldColumns; i++){
            result[i] = signature[i];
        }

        for (int i=0; i<newColumns.length; i++){
            result[oldColumns + i] = newColumns[i];
        }

        return result;
    }

    /**
     * 得到该UDTF的输出的值
     * @param input 输入数据
     * @param oldColumns input中原样输出的前oldColumns数
     * @param newColumns 新增的输出值
     * @return 该UDTF的输出的值
     */
    protected Object[] getOutputValues(Object[] input, int oldColumns, Object[] newColumns){
        Object[] result = null;
        if (oldColumns >= 0){
            result = new Object[oldColumns + newColumns.length];
        }else{
            oldColumns = 0;
            result = new Object[newColumns.length];
        }

        for (int i=0; i<oldColumns; i++){
            result[i] = input[i];
        }

        for (int i=0; i<newColumns.length; i++){
            result[oldColumns + i] = newColumns[i];
        }

        return result;
    }


    protected void forward(Object objs[])  throws UDFException {
        super.forward(objs);
    }
}
