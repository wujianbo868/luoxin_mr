package com.wujianbo.udaf;

import com.aliyun.odps.io.Text;
import com.aliyun.odps.io.Writable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestSumLenth {

    @Test
    public void testSumLength(){
        SumLenth sumLenth = new SumLenth();
        Writable buffer = sumLenth.newBuffer();
        Writable[] record = new Writable[3];
        record[0] = new Text("wu");
        record[1] = new Text("jian");
        record[2] = new Text("bo");

        sumLenth.iterate(buffer,record);

        Writable nBuffer = sumLenth.newBuffer();
        sumLenth.merge(nBuffer,buffer);
        assertEquals("8",nBuffer.toString());

    }
}
