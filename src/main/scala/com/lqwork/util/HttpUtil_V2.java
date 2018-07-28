package com.lqwork.util;

import com.sun.javafx.runtime.SystemProperties;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.map.ObjectMapper;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.*;

/***
 * http接口调用辅助类
 * @author qiupeiwei
 * @Date 2015-03-17
 */
public class HttpUtil_V2 {
  private static Map<String,String> httpCfgParamMap = null;

  @SuppressWarnings("unchecked")
  public  Map sendHttpRequest(String targetSys,String targetService,String paramsStr,String sendType){
    if(null == httpCfgParamMap){
      //初始化http配置map
      loadHttpCfgParamMap();
    }
    Map map = new HashMap();
    String responseString = null;
    //获取目标系统和目标服务 组装请求url
    String targetSysStr = httpCfgParamMap.get(targetSys);
    String targetServiceStr = httpCfgParamMap.get(targetService);
    String url = targetSysStr + targetServiceStr;

    DefaultHttpRequestRetryHandler dhr = new DefaultHttpRequestRetryHandler(5,true);//网络故障默认重发5次
    HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
    CloseableHttpClient httpClient = httpClientBuilder.setRetryHandler(dhr).build();

    if(null != sendType && sendType.equals("GET")){
      paramsStr = paramsStr.toString().replaceAll("\"", "%22").replaceAll("\\{", "%7b").replaceAll("\\}", "%7d");
      url = url + "?" + paramsStr;
      System.out.println("http服务接口 urlget:"+url);
      HttpGet httpget = new HttpGet(url);
      httpget.addHeader(new BasicHeader("", ""));
      System.out.println("http服务接口 httpgetURI:"+httpget.getURI());
      CloseableHttpResponse httpResponse = null;
      try {
        httpResponse = httpClient.execute(httpget);
        StatusLine statusLine = httpResponse.getStatusLine();
        HttpEntity entity = httpResponse.getEntity();
        if(entity != null){
          responseString = EntityUtils.toString(entity);
        }
        map.put("respStatus", statusLine.getStatusCode());
        map.put("respContent", responseString);
        System.out.println("http服务接口 statusLine_get:"+statusLine.toString());
        System.out.println("http服务接口 statusLineCode_get:"+statusLine.getStatusCode());
        System.out.println("http服务接口 responseContent_get:"+responseString);

      } catch (ClientProtocolException e) {
        System.err.println("http服务接口 http连接异常！\n"+e);
        e.printStackTrace();
      } catch (IOException e) {
        System.err.println("http服务接口 http连接发生网络异常！\n"+e);
        e.printStackTrace();
      } finally{
        try {
          httpResponse.close();
        } catch (IOException e) {
          System.err.println("http服务接口 断开http连接异常！\n"+e);
          e.printStackTrace();
        }
      }
    }else if(null != sendType && sendType.equals("POST")){
      CloseableHttpResponse httpResponse = null;
      try {
        HttpPost httpPost = new HttpPost(url);

        //设置请求和传输超时时间
        RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(600000).setConnectTimeout(50000).build();
        httpPost.setConfig(requestConfig);
        if(targetSys.equals("shield.gateway")){
          httpPost.addHeader("Content-Type", "application/x-www-form-urlencoded");
        }

        //使用Url编码的报文参数
//				params = URLEncoder.encode(params,"UTF-8");
//				List<NameValuePair> list = new ArrayList<NameValuePair>();
//				list.add(new BasicNameValuePair(paramsName, params));
//				UrlEncodedFormEntity formEntity = new UrlEncodedFormEntity(list,"UTF-8");
//				//  System.out.println("formEntity:"+EntityUtils.toString(formEntity));
//				httpPost.setEntity(formEntity);

        //不使用Url编码的报文参数
        String params_noEncode = paramsStr;
        HttpEntity httpEntity = new StringEntity(params_noEncode,"UTF-8");
        System.out.println("http服务接口 HttpEntityStr:"+EntityUtils.toString(httpEntity));
        httpPost.setEntity(httpEntity);

        httpResponse = httpClient.execute(httpPost);
        StatusLine statusLine = httpResponse.getStatusLine();
        HttpEntity entity = httpResponse.getEntity();
        if(entity != null){
//					byte[] bs = EntityUtils.toString(entity).getBytes();
//					responseString = new String(bs, "UTF-8");
          responseString = EntityUtils.toString(entity);
        }
        map.put("respStatus", statusLine.getStatusCode());
        map.put("respContent", responseString);
        System.out.println("http服务接口 statusLineCode_post:"+statusLine.getStatusCode());
        System.out.println("http服务接口 responseFlag_post:"+String.valueOf(statusLine.getStatusCode()).equals("200"));

      } catch (ClientProtocolException e) {
        System.err.println("http服务接口 http连接异常！\n"+e);
        e.printStackTrace();
      } catch (IOException e) {
        System.err.println("http服务接口 http连接发生网络异常！\n"+e);
        e.printStackTrace();
      } finally{
        try {
          if(httpResponse != null){
            httpResponse.close();
          }
        } catch (IOException e) {
          System.err.println("http服务接口 断开http连接异常！\n"+e);
//          e.printStackTrace();
        }
      }
    }

    return map;
  }

  private  void loadHttpCfgParamMap(){
    Properties prop = new Properties();
    try {
      InputStream in = this.getClass().getClassLoader().getResourceAsStream("config_v2.properties");
      prop.load(in);
      Set set = prop.stringPropertyNames();
      Iterator<String> it = set.iterator();
      httpCfgParamMap = new HashMap();
      while(it.hasNext()){
        String key = it.next();
        //System.out.println("http服务接口 httpCfgParamMap key:"+key);
        httpCfgParamMap.put(key, prop.getProperty(key).trim());
      }
      //System.out.println("http服务接口 httpCfgParamMap:"+httpCfgParamMap.toString());

    } catch (IOException e) {
      System.err.println("http服务接口 初始化http配置map异常！\n"+e);
      e.printStackTrace();
    }

  }


  /***
   * 短信发送方法
   * @return 返回短信发送调用成功或失败的标识
   * @throws Exception
   */
  public  boolean sendMsg(String phone,String msgContent) {

    //1. 是否确实发送短信
    String smsEnable = SystemProperties.getProperty("sms.enable");
    if (smsEnable == null || smsEnable.isEmpty() || !smsEnable.equalsIgnoreCase("true")) {
      System.out.println("SMS发送被关闭，如需打开，请配置sms.enable=true(config_v2.properties)");
      return true;
    }

    boolean flag = true;
    try {
      String parames = "phone=" + phone + "&msgContent=" + msgContent;
      System.out.println("短信发送字符串:"+parames);
      Map<String,Object> result =  sendHttpRequest("sms.server.host","sms.server.url", parames,"GET");
      String returnStatus = result.get("respStatus").toString();
      System.out.println("短信发送状态:"+returnStatus);
      String returnContent = result.get("respContent").toString();
      System.out.println("短信发送的内容主体:"+msgContent);
    } catch (Exception e) {
      flag = false;
      System.err.println("短信发送失败---原因是:"+e.getMessage()+"时间是:"+"手机号是:"+phone);
    }
    return flag;
  }

  /**
   * 采用UrlEncodedFormEntity(post方式)
   * @param targetSys urlhost
   * @param targetService 服务接口
   * @param paramsObject 参数
   * @return
   */
  public  Map<String,Object> sendHttpRequestVersionTwo(String targetSys, String targetService, Object paramsObject){
    if(null == httpCfgParamMap){
      //初始化http配置map
      loadHttpCfgParamMap();
    }
    Map<String,Object> map = new HashMap<String,Object>();
    String responseString = null;
    //获取目标系统和目标服务 组装请求url
    String targetSysStr = httpCfgParamMap.get(targetSys);
    String targetServiceStr = httpCfgParamMap.get(targetService);
    String url = targetSysStr + targetServiceStr;

    DefaultHttpRequestRetryHandler dhr = new DefaultHttpRequestRetryHandler(5,true);//网络故障默认重发5次
    HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
    CloseableHttpClient httpClient = httpClientBuilder.setRetryHandler(dhr).build();

    CloseableHttpResponse httpResponse = null;
    try {
      HttpPost httpPost = new HttpPost(url);
      System.out.println("httpPostURI:"+httpPost.getURI());

      //设置请求和传输超时时间
      RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(600000).setConnectTimeout(50000).build();
      httpPost.setConfig(requestConfig);
      if(targetSys.equals("sms.server.host")){
        httpPost.addHeader("Content-Type", "application/json");
      }else if(targetSys.equals("paycore")){
        httpPost.addHeader("Content-Type", "application/x-www-form-urlencoded");
      }else if(targetSys.equals("pay.gateway")){
        httpPost.addHeader("Content-Type", "application/x-www-form-urlencoded");
      }

      //实体转map
      ObjectMapper objectMapper = new ObjectMapper();
      Map<String, Object> mapParams = objectMapper.convertValue(paramsObject, Map.class);
      Set<String> keySet = mapParams.keySet();
      List<NameValuePair> list = new ArrayList<NameValuePair>();

      for (String key : keySet) {
        Object object = mapParams.get(key);
        list.add(new BasicNameValuePair(key, object == null ? null : object.toString()));
      }

      UrlEncodedFormEntity formEntity = new UrlEncodedFormEntity(list,"UTF-8");
      System.out.println("formEntity:"+EntityUtils.toString(formEntity));
      httpPost.setEntity(formEntity);

      //清空map
      mapParams.clear();
      mapParams = null;

      httpResponse = httpClient.execute(httpPost);
      StatusLine statusLine = httpResponse.getStatusLine();
      HttpEntity entity = httpResponse.getEntity();
      if(entity != null){
        responseString = EntityUtils.toString(entity);
      }

      map.put("respStatus", statusLine.getStatusCode());
      map.put("respContent", responseString);
      System.out.println("http服务接口 statusLine_post:"+statusLine.toString());
      System.out.println("http服务接口 statusLineCode_post:"+statusLine.getStatusCode());
      System.out.println("http服务接口 responseFlag_post:"+String.valueOf(statusLine.getStatusCode()).equals("200"));
      System.out.println("http服务接口 responseContent_post:"+responseString);
    } catch (ClientProtocolException e) {
      System.err.println("http服务接口 http连接异常！\n"+e);
      e.printStackTrace();
    } catch (IOException e) {
      System.err.println("http服务接口 http连接发生网络异常！\n"+e);
      e.printStackTrace();
    } finally{
      try {
        if(httpResponse != null){
          httpResponse.close();
        }
      } catch (IOException e) {
        System.err.println("http服务接口 断开http连接异常！\n"+e);
        e.printStackTrace();
      }
    }
    return map;
  }

  /**
   * 采用UrlEncodedFormEntity(post方式),支持https
   * @param targetSys urlhost
   * @param targetService 服务接口
   * @param paramsObject 参数
   * @return
   */
  public  Map<String,Object> sendHttpRequestVersionThree(String targetSys, String targetService, Object paramsObject){
    if(null == httpCfgParamMap){
      //初始化http配置map
      loadHttpCfgParamMap();
    }
    Map<String,Object> map = new HashMap<String,Object>();
    String responseString = null;
    //获取目标系统和目标服务 组装请求url
    String targetSysStr = httpCfgParamMap.get(targetSys);
    String targetServiceStr = httpCfgParamMap.get(targetService);
    String url = targetSysStr + targetServiceStr;

    CloseableHttpClient httpClient = new SSLClient();
    CloseableHttpResponse httpResponse = null;
    try {
      HttpPost httpPost = new HttpPost(url);
      System.out.println("httpPostURI:"+httpPost.getURI());

      //设置请求和传输超时时间
      RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(600000).setConnectTimeout(50000).build();
      httpPost.setConfig(requestConfig);
      if(targetSys.equals("sms.server.host")){
        httpPost.addHeader("Content-Type", "application/json");
      }else if(targetSys.equals("paycore")){
        httpPost.addHeader("Content-Type", "application/x-www-form-urlencoded");
      }else if(targetSys.equals("pay.gateway")){
        httpPost.addHeader("Content-Type", "application/x-www-form-urlencoded");
      }

      //实体转map
      ObjectMapper objectMapper = new ObjectMapper();
      Map<String, Object> mapParams = objectMapper.convertValue(paramsObject, Map.class);
      Set<String> keySet = mapParams.keySet();
      List<NameValuePair> list = new ArrayList<NameValuePair>();

      for (String key : keySet) {
        Object object = mapParams.get(key);
        list.add(new BasicNameValuePair(key, object == null ? null : object.toString()));
      }

      UrlEncodedFormEntity formEntity = new UrlEncodedFormEntity(list,"UTF-8");
      System.out.println("formEntity:"+EntityUtils.toString(formEntity));
      httpPost.setEntity(formEntity);

      //清空map
      mapParams.clear();
      mapParams = null;

      httpResponse = httpClient.execute(httpPost);
      StatusLine statusLine = httpResponse.getStatusLine();
      HttpEntity entity = httpResponse.getEntity();
      if(entity != null){
        responseString = EntityUtils.toString(entity);
      }

      map.put("respStatus", statusLine.getStatusCode());
      map.put("respContent", responseString);
      System.out.println("http服务接口 statusLine_post:"+statusLine.toString());
      System.out.println("http服务接口 statusLineCode_post:"+statusLine.getStatusCode());
      System.out.println("http服务接口 responseFlag_post:"+String.valueOf(statusLine.getStatusCode()).equals("200"));
    } catch (ClientProtocolException e) {
      System.err.println("http服务接口 http连接异常！\n"+e);
      e.printStackTrace();
    } catch (IOException e) {
      System.err.println("http服务接口 http连接发生网络异常！\n"+e);
      e.printStackTrace();
    } finally{
      try {
        if(httpResponse != null){
          httpResponse.close();
        }
      } catch (IOException e) {
        System.err.println("http服务接口 断开http连接异常！\n"+e);
        e.printStackTrace();
      }
    }
    return map;
  }

  public static void main(String[] args) throws Exception {
//	try {
//		new HttpUtil().sendMsg();
//	} catch (Exception e) {
//		e.printStackTrace();
//	}
  }

  class SSLClient extends DefaultHttpClient {
    public SSLClient() {
      super();
      SSLContext ctx = null;
      try {
        ctx = SSLContext.getInstance("TLS");
      } catch (NoSuchAlgorithmException e) {
        e.printStackTrace();
      }
      X509TrustManager tm = new X509TrustManager() {
        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
          return null;
        }
      };
      try {
        ctx.init(null, new TrustManager[]{tm}, null);
      } catch (KeyManagementException e) {
        e.printStackTrace();
      }
      SSLSocketFactory ssf = new SSLSocketFactory(ctx, SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
      ClientConnectionManager ccm = this.getConnectionManager();
      SchemeRegistry sr = ccm.getSchemeRegistry();
      sr.register(new Scheme("https", 443, ssf));
      setHttpRequestRetryHandler(new DefaultHttpRequestRetryHandler(5,true));
    }
  }
}