package com.tech.rocketmq.http;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.web.client.RestTemplate;

import java.util.Arrays;
import java.util.List;

/**
 * @author lw
 * @since 2021/11/25
 */
public class RestUtils {
    public static void main(String[] args) {
        String url="http://rs.iag-stg.pingan.com/iserver/services/data-supergis/rest/data/featureResults.json?returnContent=true";
        //设置请求头,可根据接口文档要求设置
        HttpHeaders header = new HttpHeaders();
        header.set("Accept-Charset", "UTF-8");
        header.set("Content-Type", "application/json; charset=utf-8");
        //设置请求参数，此处设置为json
        JSONObject param = getParam("land:land","");
        HttpEntity httpEntity = new HttpEntity(param.toJSONString(), header);

        RestTemplate restTemplate = new RestTemplate();
        JSONObject res = restTemplate.postForObject(url, httpEntity, JSONObject.class);
        System.out.println(res.toJSONString());
    }
    
    
    public static JSONObject getParam(String datasetNames,String attributeFilter){
        JSONObject param = new JSONObject();
        List<String> dsn = Arrays.asList(datasetNames);
        param.put("datasetNames",dsn);
        param.put("getFeatureMode","SQL");

        JSONObject queryParameter = new JSONObject();
        queryParameter.put("name",datasetNames.replace(":","@"));
        queryParameter.put("attributeFilter",attributeFilter);
        param.put("queryParameter",queryParameter);
        
        param.put("hasGeometry",false);
        return param;
    }
}
