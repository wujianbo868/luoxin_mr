package com.wujianbo.udaf;

import com.aliyun.odps.udf.UDFException;
import com.wujianbo.udtf.Lkeyvalue;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestKeyValue {

    Lkeyvalue lkeyvalue = new Lkeyvalue();

    @Test
    public void testKeyValue() throws UDFException, IOException {
        lkeyvalue.process(new String[]{"a,b;c,d", ";", ","});
        assertEquals("[a, b][c, d]", lkeyvalue.getResult());
    }
}
