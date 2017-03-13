package com.maqiang.preprocess.tra2sim;

import org.nlpcn.commons.lang.jianfan.JianFan;

import java.io.*;

/**
 * Created by maqiang on 20/02/2017.
 *
 * 繁体转换成简体
 */
public class TradToSim {

    public static void process(String srcFile,String destFile) throws IOException {
        File src = new File(srcFile);
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(src)));
        File outputFile = new File(destFile);
        outputFile.createNewFile();
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile)));
        String readLine;
        while((readLine=br.readLine())!=null) {
            String result = JianFan.f2j(readLine);
            bw.write(result+"\n");
        }
        br.close();
        bw.close();
    }

    public static void main(String [] args) {
        try {
            TradToSim.process(TradToSim.class.getClassLoader().getResource("wiki.zh.txt").getFile(),
                    "wiki_simplified.txt");
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}
