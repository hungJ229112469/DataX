package com.alibaba.datax.transformer;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.utils.MappingUtils;

/**
 * @author HL
 * @date 2019/10/9 11:45
 */
public class MappingTransformer extends Transformer {
    public MappingTransformer() {
        setTransformerName("mapping");
    }

    @Override
    public Record evaluate(Record record, Object... paras) {
        int columnIndex = (Integer) paras[0];
        String url = (String) paras[1];
        MappingUtils.putUrl(columnIndex, url);
        Column column = record.getColumn(columnIndex);
        String oriValue = column.asString();
        String newValue = MappingUtils.replace(columnIndex, oriValue);
        // System.out.println("oriValue: " + oriValue + ", newValue: " + newValue);
        record.setColumn(columnIndex, new StringColumn(newValue));
        return record;
    }
}
