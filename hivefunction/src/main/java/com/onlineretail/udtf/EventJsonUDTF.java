package com.onlineretail.udtf;

import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;

import java.util.ArrayList;
import java.util.List;

public class EventJsonUDTF extends GenericUDTF {

    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        // 定义UDTF返回值类型和名称
        List<String> fieldNames = new ArrayList<>();
        List<ObjectInspector> fieldTypes = new ArrayList<>();

        fieldNames.add("event_name");
        fieldNames.add("event_json");

        fieldTypes.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldTypes.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
                fieldTypes);
    }
    @Override
    public void process(Object[] objects) throws HiveException {

        // 根据json =>udf key 为et 而得到objects
        String input=  objects[0].toString();

        if(!StringUtils.isBlank(input)){
            JSONArray jsonArray = new JSONArray(input);

            if(jsonArray == null){
                return;
            }
//            //{
//                "ett":1341234135,
//                "en":"display",
//                "kv":{
//                    "goodsid":"0",
//                  ......
//                }
//            //
            for(int i=0;i<jsonArray.length();i++){
                String[] res = new String[2];

                try{
                    res[0] = jsonArray.getJSONObject(i).getString("en");
                    res[1] = jsonArray.getString(i);
                }catch (Exception e){
                    continue;
                }
                forward(res);
            }
        }
    }

    @Override
    public void close() throws HiveException {

    }

}
