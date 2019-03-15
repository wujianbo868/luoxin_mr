package com.wujianbo.udaf;

import com.aliyun.odps.io.Text;
import com.aliyun.odps.io.Writable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestGroupConcat {

    @Test
    public void testGroupConcat(){
        GroupConcat groupConcat = new GroupConcat();
        Writable buffer = groupConcat.newBuffer();
        Writable[] record = new Writable[3];
        record[0] = new Text("wu");
        record[1] = new Text("jian");
        record[2] = new Text("bo");

        groupConcat.iterate(buffer,record);

        Writable nBuffer = groupConcat.newBuffer();
        groupConcat.merge(nBuffer,buffer);
        assertEquals("wujianbo", nBuffer.toString());
    }
}
