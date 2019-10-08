package com.alibaba.datax.transformer;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.transformer.Transformer;

/**
 * @author HL
 * @date 2019/9/25 11:31
 */
public class IDCardTrasformer extends Transformer {

    public IDCardTrasformer() {
        setTransformerName("dx_idcardtrans");
    }

    @Override
    public Record evaluate(Record record, Object... paras) {
        int columnIndex = (Integer) paras[0];
        Column column = record.getColumn(columnIndex);
        String oriValue = column.asString();
        String newValue = transIDCard15to18(oriValue);
        record.setColumn(columnIndex, new StringColumn(newValue));

        return record;
    }

    /**
     * 15位身份证转化为18位标准证件号
     *
     * @param IdCardNO
     * @return String
     */
    public String transIDCard15to18(String IdCardNO) {
        String cardNo = null;
        try {
            if (null != IdCardNO && IdCardNO.trim().length() == 15) {
                IdCardNO = IdCardNO.trim();
                StringBuffer sb = new StringBuffer(IdCardNO);
                sb.insert(6, "19");
                sb.append(transCardLastNo(sb.toString()));
                cardNo = sb.toString();
            }
        } catch (Exception e) {
            cardNo = IdCardNO;
        }

        return cardNo;
    }

    /**
     * 15位补全"18"位后的身份证号码
     *
     * @param newCardId
     * @return
     */
    private String transCardLastNo(String newCardId) {
        char[] ch = newCardId.toCharArray();
        int sum = 0;
        int[] co = {7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2};
        char[] verCode = new char[]{'1', '0', 'X', '9', '8', '7', '6', '5', '4', '3', '2'};
        for (int i = 0; i < newCardId.length(); i++) {
            sum += (ch[i] - '0') * co[i];
        }
        int remainder = sum % 11;
        return String.valueOf(verCode[remainder]);
    }

    public static void main(String[] args) {
        String card = "441422830723001";
        IDCardTrasformer idCardTrasformer = new IDCardTrasformer();
        String s = idCardTrasformer.transIDCard15to18(card);
        System.out.println(s);

    }
}
