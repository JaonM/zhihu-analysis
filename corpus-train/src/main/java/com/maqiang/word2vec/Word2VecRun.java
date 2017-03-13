package com.maqiang.word2vec;

import com.ansj.vec.Word2VEC;

import java.util.ArrayList;
import java.util.List;

/**
 * 将分词训练成词向量
 *
 * Java版本Word2Vec https://github.com/NLPchina/Word2VEC_java
 *
 * Created by maqiang on 22/02/2017.
 */
public class Word2VecRun {

    public static void main(String [] args) {
        try {
//            File corpusFile = new File(Word2VecRun.class.getClassLoader().getResource("wiki_partition.txt").getFile());
//            Learn learn = new Learn();
//            learn.learnFile(corpusFile);
//            learn.saveModel(new File("wiki_vector.mod"));

            Word2VEC word2VEC = new Word2VEC();
            word2VEC.loadJavaModel(Word2VecRun.class.getClassLoader().getResource("wiki_vector.mod").getPath());
//            System.out.println(word2VEC.distance("程序员"));
            List<String> wordList = new ArrayList<>();
            wordList.add("程序员");
            wordList.add("互联网");
            System.out.println(word2VEC.getWordVector("互联网").length);
            System.out.println(word2VEC.getWordVector("姚明").length);
//            System.out.println(word2VEC.distance(wordList));
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
