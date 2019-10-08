package com.alibaba.datax.transformer;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;

import java.security.MessageDigest;

/**
 * @author HL
 * @date 2019/9/5 17:11
 */
public class MD5Transformer extends Transformer {

    public MD5Transformer() {
        setTransformerName("md5");
    }

    @Override
    public Record evaluate(Record record, Object... paras) {
        try {
            int columnNumber = record.getColumnNumber();
            StringBuffer stringBuffer = new StringBuffer();
            for (int i = 0; i < columnNumber; i++) {
                Column column = record.getColumn(i);
                Object rawData = column.getRawData();
                if (rawData != null) {
                    stringBuffer.append(column.getRawData());
                }
                stringBuffer.append("-");
            }
            byte[] b2 = stringBuffer.toString().getBytes("UTF8");
            MessageDigest m = MessageDigest.getInstance("MD5");
            byte[] md5Bytes = m.digest(b2);

            StringBuffer hexValue = new StringBuffer();
            for (int i = 0; i < md5Bytes.length; i++) {
                int val = ((int) md5Bytes[i]) & 0xff;
                if (val < 16) {
                    hexValue.append("0");
                }
                hexValue.append(Integer.toHexString(val));
            }
            Column column = new StringColumn(hexValue.toString());
            record.addColumn(column);
        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(), e);
        }
        return record;
    }
}
