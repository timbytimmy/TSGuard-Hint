package com.benchmark.commonUtil;

import com.alibaba.fastjson.JSONObject;
import com.benchmark.constants.DataType;
import com.benchmark.entity.DBVal;
import com.benchmark.fieldName.FieldName;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class GetDBValFromFile {
    public static final Logger logger = Logger.getLogger(GetDBValFromFile.class.getName());
    private static String charSet = "utf-8";

    /**
     * @param filePath
     * @param fieldName 字段值父类，通过传参实例化子类实现通用父类功能
     * @return List<DBVal>
     * @description 将JSON文件数据读取为DBVals
     * @dateTime 2023/2/4 1:05
     */
    public static List<DBVal> getPointDataFromJSON(String filePath, FieldName fieldName, String measurement) {
        List<DBVal> results = new ArrayList<>();
        if (filePath.isEmpty()) {
            logger.info("json file path is null!");
            return results;
        }

        try (InputStream inputStream = new FileInputStream(filePath);
             InputStreamReader inputStreamReader = new InputStreamReader(inputStream, charSet);
             BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {
            String str;
            while ((str = bufferedReader.readLine()) != null) {
                try {
                    // 无_class解析
                    JSONObject res = JSONObject.parseObject(str);
                    DBVal dbVal = DBVal.DBValBuilder.anDBVal().build();

                    //TODO 通过fieldName进行指定，ID、tags、TimeStamp固定映射，除ID和TimeStamp外，其余均为字段
                    // measurement，传参指定
                    dbVal.setTableName(measurement);
                    //TODO 后续将下述单tag扩展至多tags
                    dbVal.setTagName(fieldName.getTagName());
                    dbVal.setTagValue(res.getString(fieldName.getTagName()));

                    if (fieldName.getTimeStampType() == DataType.TEXT) {
                        dbVal.setUtcTimeMilliSeconds(CommonUtil.dateStringToUTCMilliSeconds(
                                res.getString(fieldName.getTimeStamp())));
                    } else if (fieldName.getTimeStampType() == DataType.INT64) {
                        //TODO 时间戳精度要求ms
                        dbVal.setUtcTimeMilliSeconds(res.getLong(fieldName.getTimeStamp()));
                    }

                    // fieldValue 类型
                    List<String> names = fieldName.getNames();
                    List<DataType> types = fieldName.getTypes();
                    for (int i = 0; i < fieldName.getListSize(); i++) {
                        dbVal.addField(names.get(i), types.get(i), res.get(names.get(i)));
                    }

                    // 判断HashTable是否已包含该key
                    results.add(dbVal);
                } catch (Exception e) {
                    logger.severe(String.format("Parse jsonObject error:%s", e));
                }
            }
        } catch (IOException e) {
            logger.severe(String.format("IO异常: %s", e));
        }

        logger.info(String.format("total dbVal size:[%d]", results.size()));
        return results;
    }
}
