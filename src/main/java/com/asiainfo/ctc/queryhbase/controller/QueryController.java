package com.asiainfo.ctc.queryhbase.controller;

import com.asiainfo.ctc.queryhbase.utils.HBaseUtils;
import com.asiainfo.ctc.queryhbase.utils.UploadLabel;
import net.sf.json.JSONArray;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * create by xiejialin on 201906
 */
@Controller
public class QueryController {

    private static Logger logger = LoggerFactory.getLogger(QueryController.class);

    @ResponseBody
    @RequestMapping(value = "/CustomerView", method = {RequestMethod.GET})
    public Map getDatas(int qryType, String prodInstId){
        String resultStatus = "1";
        Table table = null;
        try {
            table = HBaseUtils.getConnection().getTable(TableName.valueOf("interface_eda:testBulkLoad"));
            System.out.println("==============================table:" + table);
        }catch (IOException e){
            resultStatus = "0";
            logger.error(e.getMessage());
            e.printStackTrace();
        }

        int bigCode = qryType * 9999;
        Long hash = Long.parseLong(prodInstId)^bigCode;
        Long prePartition = hash % Integer.parseInt("50");
        DecimalFormat decimalFormat = new DecimalFormat("00");
        String partitionId = decimalFormat.format(prePartition);
        String rowkey = partitionId + "_" + qryType + "_" + prodInstId;
        System.out.println("==============================rowkey:" + rowkey);

        Map resultData = new HashMap();
        Date date = new Date();
        if(qryType == 4){
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            calendar.add(Calendar.DATE, -1);
            String dateNo = sdf.format(calendar.getTime());
            String netHealth = "{\"DATE_NO\":\"" + dateNo + "\",\"YD_HEALTH\":\"100\",\"KD_HEALTH\":\"100\",\"ITV_HEALTH\":\"100\"}";
            resultData.put("resultStatus", "1");
            String[] labelCode = {"10C07004001", "10C07004002", "10C07004003"};
            for (int i = 0; i < 3; i++) {
                String line = dateNo.substring(0, 6) + "|811|" +
                        prodInstId + "|" + labelCode[i] + "|C|" +
                new SimpleDateFormat("yyyyMMddHHmmss").format(date) + "|" + resultStatus + "|B";
                logger.info(line);
            }
            ArrayList<String> valueList = new ArrayList<>();
            valueList.add(netHealth);
            resultData.put("resultInfo", JSONArray.fromObject(valueList));
            return resultData;
        }

        Get get = new Get(Bytes.toBytes(rowkey));
        Result result = null;
        try {
             result = table.get(get);
        } catch (IOException e) {
            resultStatus = "0";
            logger.error(e.getMessage());
            e.printStackTrace();
        }
        String value = Bytes.toString(result.value());

        UploadLabel.uploadLabelLog(date, qryType, prodInstId, value, resultStatus);

        if(value == null){
            value = "";
        }
        ArrayList<String> valueList = new ArrayList<>();
        valueList.add(value);
        JSONArray jsonArray = JSONArray.fromObject(valueList);

        resultData.put("resultInfo", jsonArray);
        resultData.put("resultStatus", resultStatus);

        if(table != null){
            try {
                table.close();
            } catch (IOException e) {
                logger.error(e.getMessage());
                e.printStackTrace();
            }
        }

        return resultData;
    }
}
