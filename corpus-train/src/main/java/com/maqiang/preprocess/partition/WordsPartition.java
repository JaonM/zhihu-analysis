package com.maqiang.preprocess.partition;

import org.apdplat.word.WordSegmenter;
import org.apdplat.word.segmentation.Word;

import java.io.*;
import java.util.List;

/**
 * Created by maqiang on 20/02/2017.
 *
 * 基于Word的中文分词
 *github url:https://github.com/ysc/word
 */
public class WordsPartition {

    public static void partition(String src,String dest) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(src));
        BufferedWriter bw = new BufferedWriter(new FileWriter(dest));
        String readLine;
        while((readLine=br.readLine())!=null) {
            List<Word> words = WordSegmenter.seg(readLine);
            StringBuffer sb = new StringBuffer();
            for(Word word:words) {
                sb.append(word.getText()+" ");
            }
            sb.replace(sb.length()-1,sb.length(),"\n");
            bw.write(sb.toString());
        }
        br.close();
        bw.close();
    }

    public static void main(String [] args) {
        try {
            WordsPartition.partition(WordsPartition.class.getClassLoader().getResource("wiki_simplified.txt").getFile(),
                    "wiki_partition.txt");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
