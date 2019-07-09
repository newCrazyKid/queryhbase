package com.asiainfo.ctc.queryhbase.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

public class UploadLabel {

    private static Logger logger = LoggerFactory.getLogger(HBaseUtils.class);

    public static void uploadLabelLog(Date date, int qryType, String prodInstId, String value, String resultStatus){
        String monthId = null;
        String prvnceId = "811";
//        String custId = null;
        String servId = prodInstId;
        String lableCode = null;
        String useTime = new SimpleDateFormat("yyyyMMddHHmmss").format(date);
        String isSuccess = resultStatus;
        String lableSource = null;

        if (value != null) {
            String[] fields = value.split(":");
            if(qryType == 1) {
                monthId = fields[5].substring(1, 7);
                lableCode = "10C01003,10C01001,10C01004,10C02004004,10C06,10C02015002,10C02002," +
                        "10C02002012,10C02007001,10C02005,10B02006001001,10C03001,10D03001";
                lableSource = "A";
            }else if(qryType == 2){
//                lableCode = "";
                monthId = value.substring(12, 18);
//                lableSource = "A";
            }else if(qryType == 3){
                monthId = value.substring(12, 18);
                lableCode = "10C02016052,10C02016053,10C02016056";
                lableSource = "A";
            }/*else if(qryType == 4){
                monthId = value.substring(12, 18);
                lableCode = "10C07004001,10C07004002,10C07004003";
                lableType = "C";
                lableSource = "B";
            }*/
        }

        if (lableCode != null) {
            String[] codes = lableCode.split(",");
            for (String code : codes) {
                String line = monthId + "|" + prvnceId + "|" + servId + "|" + code + "|" +
                        code.charAt(2) + "|" + useTime + "|" + isSuccess + "|" + lableSource;
                logger.info(line);
            }
        }
        /*String line = monthId + "|" + prvnceId + "|" + custId + "|" + servId + "|" + lableCode + "|" +
                lableType + "|" + useTime + "|" + isSuccess + "|" + lableSource;*/
    }
}
