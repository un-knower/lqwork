package com.lqwork.demo;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JavaExample {
  private String name;
  private int age;
  private static Object obj;

  public static void main(String[] args) throws Exception {
    System.out.println("hello");


    /*String str = "{\"risk_items\":[{\"risk_level\":\"low\",\"item_detail\":{\"frequency_detail_list\":[{\"data\":[\"236\\u203b\\u203b\\u203b\\u203b804@qq.com\"],\"detail\":\"3\\u4e2a\\u6708\\u8eab\\u4efd\\u8bc1\\u5173\\u8054\\u90ae\\u7bb1\\u6570\\uff1a1\"},{\"data\":[\"15057470549\",\"150\\u203b\\u203b\\u203b\\u203b1184\"],\"detail\":\"3\\u4e2a\\u6708\\u8eab\\u4efd\\u8bc1\\u5173\\u8054\\u624b\\u673a\\u53f7\\u6570\\uff1a2\"},{\"data\":[\"\\u6d59\\u6c5f\\u7701\\u5b81\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b274\\u53f7\",\"\\u5e84\\u6865\\u9547\\u203b\\u203b\\u203b\\u203b74\\u53f7\",\"\\u6d59\\u6c5f\\u7701\\u5b81\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b333\\u53f7\"],\"detail\":\"3\\u4e2a\\u6708\\u8eab\\u4efd\\u8bc1\\u5173\\u8054\\u5bb6\\u5ead\\u5730\\u5740\\u6570\\uff1a3\"}],\"type\":\"frequency_detail\"},\"item_id\":6622421,\"item_name\":\"3\\u4e2a\\u6708\\u5185\\u8eab\\u4efd\\u8bc1\\u5173\\u8054\\u591a\\u4e2a\\u7533\\u8bf7\\u4fe1\\u606f\",\"group\":\"\\u5ba2\\u6237\\u884c\\u4e3a\\u68c0\\u6d4b\"},{\"risk_level\":\"medium\",\"item_detail\":{\"frequency_detail_list\":[{\"data\":[\"467e\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b3caf\",\"f0b7\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b27ee\",\"c094\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b58d8\",\"8214\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b7282\",\"852c\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b84b1\",\"4bc6\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203bef5a\"],\"detail\":\"7\\u5929\\u5185\\u8eab\\u4efd\\u8bc1\\u5173\\u8054\\u8bbe\\u5907\\u6570\\uff1a6\"},{\"detail\":\"1\\u5929\\u5185\\u8eab\\u4efd\\u8bc1\\u5173\\u8054\\u8bbe\\u5907\\u6570\\uff1a0\"}],\"type\":\"frequency_detail\"},\"item_id\":6622591,\"item_name\":\"7\\u5929\\u5185\\u8eab\\u4efd\\u8bc1\\u4f7f\\u7528\\u8fc7\\u591a\\u8bbe\\u5907\\u8fdb\\u884c\\u7533\\u8bf7\",\"group\":\"\\u5ba2\\u6237\\u884c\\u4e3a\\u68c0\\u6d4b\"},{\"risk_level\":\"low\",\"item_detail\":{\"frequency_detail_list\":[{\"data\":[\"\\u6d59\\u6c5f\\u7701 \\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b1-3\\u697c\",\"\\u6d59\\u6c5f\\u7701\\u5b81\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b\\u203b333\\u53f7\",\"\\u6851\\u7530\\u203b\\u203b\\u203b3\\u53f7\"],\"detail\":\"3\\u4e2a\\u6708\\u5185\\u501f\\u6b3e\\u4eba\\u8eab\\u4efd\\u8bc1\\u5173\\u8054\\u5de5\\u4f5c\\u5355\\u4f4d\\u5730\\u5740\\u4e2a\\u6570\\uff1a3\"}],\"type\":\"frequency_detail\"},\"item_id\":6622701,\"item_name\":\"3\\u4e2a\\u6708\\u5185\\u7533\\u8bf7\\u4eba\\u8eab\\u4efd\\u8bc1\\u5173\\u8054\\u5de5\\u4f5c\\u5355\\u4f4d\\u5730\\u5740\\u4e2a\\u6570\\u5927\\u4e8e\\u7b49\\u4e8e2\",\"group\":\"\\u5ba2\\u6237\\u884c\\u4e3a\\u68c0\\u6d4b\"},{\"risk_level\":\"high\",\"item_detail\":{\"platform_detail_dimension\":[{\"count\":25,\"detail\":[\"\\u4e00\\u822c\\u6d88\\u8d39\\u5206\\u671f\\u5e73\\u53f0:7\",\"\\u4e92\\u8054\\u7f51\\u91d1\\u878d\\u95e8\\u6237:1\",\"\\u94f6\\u884c\\u4e2a\\u4eba\\u4e1a\\u52a1:1\",\"\\u94f6\\u884c\\u6d88\\u8d39\\u91d1\\u878d\\u516c\\u53f8:1\",\"\\u5c0f\\u989d\\u8d37\\u6b3e\\u516c\\u53f8:5\",\"P2P\\u7f51\\u8d37:10\"],\"dimension\":\"\\u501f\\u6b3e\\u4eba\\u624b\\u673a\\u8be6\\u60c5\"},{\"count\":25,\"detail\":[\"\\u4e00\\u822c\\u6d88\\u8d39\\u5206\\u671f\\u5e73\\u53f0:7\",\"\\u4e92\\u8054\\u7f51\\u91d1\\u878d\\u95e8\\u6237:1\",\"\\u94f6\\u884c\\u4e2a\\u4eba\\u4e1a\\u52a1:1\",\"\\u94f6\\u884c\\u6d88\\u8d39\\u91d1\\u878d\\u516c\\u53f8:1\",\"\\u5c0f\\u989d\\u8d37\\u6b3e\\u516c\\u53f8:5\",\"P2P\\u7f51\\u8d37:10\"],\"dimension\":\"\\u501f\\u6b3e\\u4eba\\u8eab\\u4efd\\u8bc1\\u8be6\\u60c5\"}],\"platform_detail\":[\"\\u4e00\\u822c\\u6d88\\u8d39\\u5206\\u671f\\u5e73\\u53f0:7\",\"\\u4e92\\u8054\\u7f51\\u91d1\\u878d\\u95e8\\u6237:1\",\"\\u94f6\\u884c\\u4e2a\\u4eba\\u4e1a\\u52a1:1\",\"\\u94f6\\u884c\\u6d88\\u8d39\\u91d1\\u878d\\u516c\\u53f8:1\",\"\\u5c0f\\u989d\\u8d37\\u6b3e\\u516c\\u53f8:5\",\"P2P\\u7f51\\u8d37:12\"],\"platform_count\":27,\"type\":\"platform_detail\"},\"item_id\":6622761,\"item_name\":\"7\\u5929\\u5185\\u7533\\u8bf7\\u4eba\\u5728\\u591a\\u4e2a\\u5e73\\u53f0\\u7533\\u8bf7\\u501f\\u6b3e\",\"group\":\"\\u591a\\u5e73\\u53f0\\u501f\\u8d37\\u7533\\u8bf7\\u68c0\\u6d4b\"},{\"risk_level\":\"medium\",\"item_detail\":{\"platform_detail_dimension\":[{\"count\":60,\"detail\":[\"\\u4e00\\u822c\\u6d88\\u8d39\\u5206\\u671f\\u5e73\\u53f0:13\",\"\\u4e92\\u8054\\u7f51\\u91d1\\u878d\\u95e8\\u6237:1\",\"\\u94f6\\u884c\\u4e2a\\u4eba\\u4e1a\\u52a1:1\",\"\\u94f6\\u884c\\u6d88\\u8d39\\u91d1\\u878d\\u516c\\u53f8:1\",\"\\u4fe1\\u7528\\u5361\\u4e2d\\u5fc3:1\",\"\\u5c0f\\u989d\\u8d37\\u6b3e\\u516c\\u53f8:13\",\"\\u7b2c\\u4e09\\u65b9\\u670d\\u52a1\\u5546:1\",\"P2P\\u7f51\\u8d37:29\"],\"dimension\":\"\\u501f\\u6b3e\\u4eba\\u624b\\u673a\\u8be6\\u60c5\"},{\"count\":62,\"detail\":[\"\\u4e00\\u822c\\u6d88\\u8d39\\u5206\\u671f\\u5e73\\u53f0:14\",\"\\u4e92\\u8054\\u7f51\\u91d1\\u878d\\u95e8\\u6237:1\",\"\\u94f6\\u884c\\u4e2a\\u4eba\\u4e1a\\u52a1:1\",\"\\u94f6\\u884c\\u6d88\\u8d39\\u91d1\\u878d\\u516c\\u53f8:1\",\"\\u4fe1\\u7528\\u5361\\u4e2d\\u5fc3:1\",\"\\u5c0f\\u989d\\u8d37\\u6b3e\\u516c\\u53f8:13\",\"\\u7b2c\\u4e09\\u65b9\\u670d\\u52a1\\u5546:1\",\"P2P\\u7f51\\u8d37:30\"],\"dimension\":\"\\u501f\\u6b3e\\u4eba\\u8eab\\u4efd\\u8bc1\\u8be6\\u60c5\"}],\"platform_detail\":[\"\\u4e00\\u822c\\u6d88\\u8d39\\u5206\\u671f\\u5e73\\u53f0:14\",\"\\u4e92\\u8054\\u7f51\\u91d1\\u878d\\u95e8\\u6237:1\",\"\\u94f6\\u884c\\u4e2a\\u4eba\\u4e1a\\u52a1:1\",\"\\u94f6\\u884c\\u6d88\\u8d39\\u91d1\\u878d\\u516c\\u53f8:1\",\"\\u4fe1\\u7528\\u5361\\u4e2d\\u5fc3:1\",\"\\u5c0f\\u989d\\u8d37\\u6b3e\\u516c\\u53f8:13\",\"\\u7b2c\\u4e09\\u65b9\\u670d\\u52a1\\u5546:1\",\"P2P\\u7f51\\u8d37:32\"],\"platform_count\":64,\"type\":\"platform_detail\"},\"item_id\":6622771,\"item_name\":\"1\\u4e2a\\u6708\\u5185\\u7533\\u8bf7\\u4eba\\u5728\\u591a\\u4e2a\\u5e73\\u53f0\\u7533\\u8bf7\\u501f\\u6b3e\",\"group\":\"\\u591a\\u5e73\\u53f0\\u501f\\u8d37\\u7533\\u8bf7\\u68c0\\u6d4b\"},{\"risk_level\":\"medium\",\"item_detail\":{\"platform_detail_dimension\":[{\"count\":71,\"detail\":[\"\\u4e00\\u822c\\u6d88\\u8d39\\u5206\\u671f\\u5e73\\u53f0:18\",\"\\u4e92\\u8054\\u7f51\\u91d1\\u878d\\u95e8\\u6237:1\",\"\\u94f6\\u884c\\u4e2a\\u4eba\\u4e1a\\u52a1:1\",\"\\u94f6\\u884c\\u6d88\\u8d39\\u91d1\\u878d\\u516c\\u53f8:1\",\"\\u4fe1\\u7528\\u5361\\u4e2d\\u5fc3:1\",\"\\u5c0f\\u989d\\u8d37\\u6b3e\\u516c\\u53f8:13\",\"\\u7b2c\\u4e09\\u65b9\\u670d\\u52a1\\u5546:1\",\"P2P\\u7f51\\u8d37:34\",\"\\u5927\\u578b\\u6d88\\u8d39\\u91d1\\u878d\\u516c\\u53f8:1\"],\"dimension\":\"\\u501f\\u6b3e\\u4eba\\u624b\\u673a\\u8be6\\u60c5\"},{\"count\":72,\"detail\":[\"\\u4e00\\u822c\\u6d88\\u8d39\\u5206\\u671f\\u5e73\\u53f0:18\",\"\\u4e92\\u8054\\u7f51\\u91d1\\u878d\\u95e8\\u6237:1\",\"\\u94f6\\u884c\\u4e2a\\u4eba\\u4e1a\\u52a1:1\",\"\\u94f6\\u884c\\u6d88\\u8d39\\u91d1\\u878d\\u516c\\u53f8:1\",\"\\u4fe1\\u7528\\u5361\\u4e2d\\u5fc3:1\",\"\\u5c0f\\u989d\\u8d37\\u6b3e\\u516c\\u53f8:13\",\"\\u7b2c\\u4e09\\u65b9\\u670d\\u52a1\\u5546:1\",\"P2P\\u7f51\\u8d37:35\",\"\\u5927\\u578b\\u6d88\\u8d39\\u91d1\\u878d\\u516c\\u53f8:1\"],\"dimension\":\"\\u501f\\u6b3e\\u4eba\\u8eab\\u4efd\\u8bc1\\u8be6\\u60c5\"}],\"platform_detail\":[\"\\u4e00\\u822c\\u6d88\\u8d39\\u5206\\u671f\\u5e73\\u53f0:19\",\"\\u4e92\\u8054\\u7f51\\u91d1\\u878d\\u95e8\\u6237:1\",\"\\u94f6\\u884c\\u4e2a\\u4eba\\u4e1a\\u52a1:1\",\"\\u94f6\\u884c\\u6d88\\u8d39\\u91d1\\u878d\\u516c\\u53f8:1\",\"\\u4fe1\\u7528\\u5361\\u4e2d\\u5fc3:1\",\"\\u5c0f\\u989d\\u8d37\\u6b3e\\u516c\\u53f8:13\",\"\\u7b2c\\u4e09\\u65b9\\u670d\\u52a1\\u5546:1\",\"P2P\\u7f51\\u8d37:37\",\"\\u5927\\u578b\\u6d88\\u8d39\\u91d1\\u878d\\u516c\\u53f8:1\"],\"platform_count\":75,\"type\":\"platform_detail\"},\"item_id\":6622781,\"item_name\":\"3\\u4e2a\\u6708\\u5185\\u7533\\u8bf7\\u4eba\\u5728\\u591a\\u4e2a\\u5e73\\u53f0\\u7533\\u8bf7\\u501f\\u6b3e\",\"group\":\"\\u591a\\u5e73\\u53f0\\u501f\\u8d37\\u7533\\u8bf7\\u68c0\\u6d4b\"}],\"final_score\":100,\"final_decision\":\"Reject\",\"application_id\":\"170802144438248DAD9968CCECC55878\",\"apply_time\":1501656278482,\"report_time\":1501656278489,\"address_detect\":{\"field_address\":[{\"address\":\"\\u56db\\u5ddd\\u7701\\u5df4\\u4e2d\\u5730\\u533a\\u5df4\\u4e2d\\u5e02\",\"field_path\":\"id_number\",\"value_format\":\"ID_NUMBER\"},{\"address\":\"\\u6d59\\u6c5f\\u7701\\u5b81\\u6ce2\\u5e02\",\"field_path\":\"account_mobile\",\"value_format\":\"MOBILE\"}],\"mobile_address\":\"\\u6d59\\u6c5f\\u7701\\u5b81\\u6ce2\\u5e02\",\"id_card_address\":\"\\u56db\\u5ddd\\u7701\\u5df4\\u4e2d\\u5730\\u533a\\u5df4\\u4e2d\\u5e02\"}}";
    System.out.println(JavaExample.unicodeToString(str));*/


    /*System.out.println(15 ^ 6);
    System.out.println(2 >> 1);
    System.out.println(2 << 1);
    System.out.println(2 >>> 1);
    System.out.println(2 % 3);
    System.out.println(2 & 3);*/
    /*HashMap<String, String> map = new HashMap<String, String>();
    map.put("k1", "v1");
    map.put("k2", "v2");
    System.out.println(map.values());
    System.out.println(map.get(""));
    System.out.println(map.entrySet());*/


    /*HashSet<String> set = new HashSet<String>();
    set.add("s1");
    set.add("s1");
    System.out.println(set);*/


    /*JavaExample oc = new JavaExample();
    System.out.println(oc.hashCode());
    System.out.println("hello".hashCode());
    System.out.println(Integer.valueOf(10).hashCode());*/


    /*ArrayList<String> list = new ArrayList<String>(20);
    System.out.println(Integer.MAX_VALUE - 8);*/

    /*LinkedList<String> linkedList = new LinkedList<String>();
    linkedList.add("a");
    linkedList.add("b");
    linkedList.add("c");
    linkedList.add("d");
    linkedList.add("e");
    linkedList.add(0, "cc");
    System.out.println(linkedList.get(0));
    linkedList.addFirst("aa");
    linkedList.addLast("bb");
    linkedList.removeFirst();
    linkedList.remove(1);
    linkedList.remove("a");
    linkedList.get(1);
    System.out.println(linkedList);*/


    /*int[] i = new int[10];
    System.out.println(i[0]);

    int num = 10000;
    ArrayList<Integer> list2 = new ArrayList<Integer>();
    list2.add(1);
    list2.add(2);
    list2.add(1, 11);
    System.out.println(list2);*/
//    System.out.println(list2.remove(0));
//    for (int i = 0; i < num; i++) {
//      list2.add(1);
//    }
//    System.out.println(list2.size());


    /*ArrayList<String> list = new ArrayList<String>();
    list.add("s1");
    list.add("s2");

    Class c = list.getClass();
    Method m = c.getMethod("add", Object.class);
    m.invoke(list, 10);
    for (Object j : list) {
      System.out.println(j);
    }*/


    /*Class c = Class.forName("com.lqwork.demo.JavaExample");
    obj = c.newInstance();

    System.out.println(c.getName());
    System.out.println(c.newInstance());

    System.out.println(c.getConstructors()[0]);

    System.out.println(c.getMethods()[1]);
    System.out.println(c.getMethod("method1", String.class));
    System.out.println(c.getMethod("main", String[].class));

    System.out.println(c.getDeclaredFields()[0]);
    System.out.println(c.getDeclaredField("name"));*/


    /*String s1 = new String("s1");
    String s2 = new String("s2");
    String s3 = s1;

    System.out.println(s1.equals(s2));
    System.out.println(s1.equals(s3));

    System.out.println(s1.hashCode() == (s2.hashCode()));
    System.out.println(s1.hashCode() == (s3.hashCode()));*/
  }

  public void method1(String s){

  }





  /**
   * \\uhhhh               带有十六进制值0x的字符hhhh
   * \\p{XDigit}           十六进制数字:[0-9a-fA-F]
   * {4}                  恰好匹配4个
   * find()               相当于一个迭代器
   * Integer.parseInt()   16进制转10进制
   * group()              例:(a(b)(c(d))), 第0组是(a(b)(c(d))), 第一组是a(b)(c(d)), 第二组是(b)和(c(d)), 第三组是d
   * @param str
   * @return
   */
  public static String unicodeToString(String str) {

    Pattern pattern = Pattern.compile("(\\\\u(\\p{XDigit}{4}))");
    Matcher matcher = pattern.matcher(str);
    char ch;
    while (matcher.find()) {
      ch = (char) Integer.parseInt(matcher.group(2), 16);//十进制通过char强转为中文
//      System.out.println(ch);
      str = str.replace(matcher.group(1), ch+"" );//将原字符串中的16进制替换成相应的中文字符串
    }
    return str;
  }
}
