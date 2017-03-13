package com.maqiang.dataprocess;

import com.hankcs.hanlp.HanLP;
import com.maqiang.hbaseutil.HBaseUtil;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apdplat.word.WordSegmenter;
import org.apdplat.word.segmentation.Word;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by maqiang on 23/02/2017.
 */
public class GenAttribute {

    public List<Word> partition(String src) {
        return WordSegmenter.seg(src);
    }

    public Set<String> genAttr(String questions) {
        Set<String> result = new HashSet<>();
        try {
            JSONArray jsonArray = new JSONArray(unicodeToString(questions).replace("u",""));
            StringBuilder sb = new StringBuilder();
            for (int i = 0;i<jsonArray.length();i++) {
                JSONObject jsonObject = jsonArray.getJSONObject(i);
                String question = jsonObject.getString("title");
//                List<Word> words = partition(question);
//                for(Word word:words) {
//                    result.add(word.getText());
//                }
//                List<String> words = HanLP.extractKeyword(question,5);
//
//                //如何筛选出最具代表性的关键词,排序?
//                for(String word : words) {
//                    if(result.size()<50) {
//                        result.add(word);
//                    }else {
//                        break;
//                    }
//                }
                sb.append(question);
                sb.append(",");
            }
            List<String> words = HanLP.extractKeyword(sb.toString(),5);

            //如何筛选出最具代表性的关键词,排序?
            for(String word : words) {
                if(result.size()<50) {
                    result.add(word);
                }else {
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            if(questions!=null) {
                System.out.println(unicodeToString(questions).replace("u",""));
            } else {
                System.out.println("null questions");
            }
            result = Collections.emptySet();
        }
        return result;
    }

    public void distributeAttrs(String tableName) {
        try {
            HTable hTable = HBaseUtil.getTable(tableName);
            ResultScanner scanner = HBaseUtil.getAllRows(hTable);
            for (Result result : scanner) {
                String questions = Bytes.toString(result.getValue(Bytes.toBytes("info"),
                        Bytes.toBytes("following_questions")));
//                List<Cell> cells = result.getColumnCells(Bytes.toBytes("info"),Bytes.toBytes("following_questions"));
                StringBuffer br = new StringBuffer();
//                for(Cell cell : cells) {
//                    String questions = Bytes.toString(CellUtil.cloneValue(cell));
//                    br.append(questions);
//                }
                Set<String> set =genAttr(questions);
                System.out.println(set.size());
                for(String str : set) {
                    br.append(str);
                    br.append(",");
                }
                String str = "";
                if(br.length()>0) {
                    str = br.toString().substring(0, br.toString().length() - 1);
                }
                HBaseUtil.addRow(hTable,Bytes.toString(result.getRow()),"attrs","keywords",str);
                String name = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")));
                System.out.println(name+" "+br.length()+" ");
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void main(String [] args) {
        GenAttribute gen = new GenAttribute();
        gen.distributeAttrs("zhihu_user");
    }

    public static String unicodeToString(String str) {

        Pattern pattern = Pattern.compile("(\\\\u(\\p{XDigit}{4}))");
        Matcher matcher = pattern.matcher(str);
        char ch;
        while (matcher.find()) {
            ch = (char) Integer.parseInt(matcher.group(2), 16);
            str = str.replace(matcher.group(1), ch + "");
        }
        return str;
    }
}
