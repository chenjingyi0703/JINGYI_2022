package ik;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;
import org.wltea.analyzer.lucene.IKAnalyzer;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class IKAnalyzerClient {
    public static Map<String, String> segMore(String text) {
        Map<String, String> map = new HashMap<>();

        map.put("智能切分", segText(text, true));
        map.put("细粒度切分", segText(text, false));

        return map;
    }

    public static String segText(String text, boolean useSmart) {
        StringBuilder result = new StringBuilder();
        //独立java的分词
        //参数：true 智能切分 false 最细粒度切分
        IKSegmenter ik = new IKSegmenter(new StringReader(text), useSmart);
        try {
            Lexeme word = null;
            while ((word = ik.next()) != null) {
                result.append(word.getLexemeText()).append(" ");
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        System.out.println(result.toString());
        return result.toString();
    }

//    public static void iksegementatio(){
//        String t = "一件红色西装";
//        System.out.println(t);
//        IKSegmentation ikSeg = new IKSegmentation(new StringReader(t) ,true);
//        try {
//            Lexeme l = null;
//            while( (l = ikSeg.next()) != null){
//                System.out.println(l);
//            }
//        } catch (IOException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
//    }

    public static void stream() {
        String text = "lxw的大数据田地 -- lxw1234.com 专注Hadoop、Spark、Hive等大数据技术博客。 北京优衣库";
        //基于lucence的分词
        //实例化分词器
        //参数：true 智能切分 false 最细粒度切分
        Analyzer analyzer = new IKAnalyzer(false);
        StringReader reader = new StringReader(text);

        try {
            TokenStream ts = analyzer.tokenStream("", reader);
            CharTermAttribute term = ts.getAttribute(CharTermAttribute.class);
            while (ts.incrementToken()) {
                System.out.print(term.toString() + "|");
            }
        } catch (Exception e) {

        }

        analyzer.close();
        reader.close();
    }

    //分词结果与标准分词对比
    private static EvaluationResult evaluation(String resultText, String standardText) {
        int perfectLineCount=0;
        int wrongLineCount=0;
        int perfectCharCount=0;
        int wrongCharCount=0;
        try(BufferedReader resultReader = new BufferedReader(new InputStreamReader(new FileInputStream(resultText),"utf-8"));
            BufferedReader standardReader = new BufferedReader(new InputStreamReader(new FileInputStream(standardText),"utf-8"))){
            String result;
            while( (result = resultReader.readLine()) != null ){
                result = result.trim();
                String standard = standardReader.readLine().trim();
                if(result.equals("")){
                    continue;
                }
                if(result.equals(standard)){
                    //分词结果和标准一模一样
                    perfectLineCount++;
                    perfectCharCount+=standard.replaceAll("\\s+", "").length();
                }else{
                    //分词结果和标准不一样
                    wrongLineCount++;
                    wrongCharCount+=standard.replaceAll("\\s+", "").length();
                }
            }
        } catch (IOException ex) {
            System.err.println("分词效果评估失败：" + ex.getMessage());
        }
        int totalLineCount = perfectLineCount+wrongLineCount;
        int totalCharCount = perfectCharCount+wrongCharCount;
        EvaluationResult er = new EvaluationResult();
        er.setPerfectCharCount(perfectCharCount);
        er.setPerfectLineCount(perfectLineCount);
        er.setTotalCharCount(totalCharCount);
        er.setTotalLineCount(totalLineCount);
        er.setWrongCharCount(wrongCharCount);
        er.setWrongLineCount(wrongLineCount);
        return er;
    }

    private static class EvaluationResult implements Comparable{
        private String analyzer;
        private float segSpeed;
        private int totalLineCount;
        private int perfectLineCount;
        private int wrongLineCount;
        private int totalCharCount;
        private int perfectCharCount;
        private int wrongCharCount;

        public String getAnalyzer() {
            return analyzer;
        }
        public void setAnalyzer(String analyzer) {
            this.analyzer = analyzer;
        }
        public float getSegSpeed() {
            return segSpeed;
        }
        public void setSegSpeed(float segSpeed) {
            this.segSpeed = segSpeed;
        }
        public float getLinePerfectRate(){
            return perfectLineCount/(float)totalLineCount*100;
        }
        public float getLineWrongRate(){
            return wrongLineCount/(float)totalLineCount*100;
        }
        public float getCharPerfectRate(){
            return perfectCharCount/(float)totalCharCount*100;
        }
        public float getCharWrongRate(){
            return wrongCharCount/(float)totalCharCount*100;
        }
        public int getTotalLineCount() {
            return totalLineCount;
        }
        public void setTotalLineCount(int totalLineCount) {
            this.totalLineCount = totalLineCount;
        }
        public int getPerfectLineCount() {
            return perfectLineCount;
        }
        public void setPerfectLineCount(int perfectLineCount) {
            this.perfectLineCount = perfectLineCount;
        }
        public int getWrongLineCount() {
            return wrongLineCount;
        }
        public void setWrongLineCount(int wrongLineCount) {
            this.wrongLineCount = wrongLineCount;
        }
        public int getTotalCharCount() {
            return totalCharCount;
        }
        public void setTotalCharCount(int totalCharCount) {
            this.totalCharCount = totalCharCount;
        }
        public int getPerfectCharCount() {
            return perfectCharCount;
        }
        public void setPerfectCharCount(int perfectCharCount) {
            this.perfectCharCount = perfectCharCount;
        }
        public int getWrongCharCount() {
            return wrongCharCount;
        }
        public void setWrongCharCount(int wrongCharCount) {
            this.wrongCharCount = wrongCharCount;
        }
        @Override
        public String toString(){
            return analyzer+"："
                    +"\n"
                    +"分词速度："+segSpeed+" 字符/毫秒"
                    +"\n"
                    +"行数完美率："+getLinePerfectRate()+"%"
                    +"  行数错误率："+getLineWrongRate()+"%"
                    +"  总的行数："+totalLineCount
                    +"  完美行数："+perfectLineCount
                    +"  错误行数："+wrongLineCount
                    +"\n"
                    +"字数完美率："+getCharPerfectRate()+"%"
                    +" 字数错误率："+getCharWrongRate()+"%"
                    +" 总的字数："+totalCharCount
                    +" 完美字数："+perfectCharCount
                    +" 错误字数："+wrongCharCount;
        }
        @Override
        public int compareTo(Object o) {
            EvaluationResult other = (EvaluationResult)o;
            if(other.getLinePerfectRate() - getLinePerfectRate() > 0){
                return 1;
            }
            if(other.getLinePerfectRate() - getLinePerfectRate() < 0){
                return -1;
            }
            return 0;
        }
    }
    //测试
    public static void main(String[] args) throws IOException {
        String text = "lxw的大数据田地 -- lxw1234.com 专注Hadoop、Spark、Hive等大数据技术博客。 北京优衣库";
//        //独立Lucene实现
//        StringReader re = new StringReader(text);
//        IKSegmenter ik = new IKSegmenter(re, true);
//        Lexeme lex = null;
//        try {
//            while ((lex = ik.next()) != null) {
//                System.out.print(lex.getLexemeText() + "|");
//            }
//        } catch (Exception e) {
//
//        }
//        segText(text, true);


        InputStream in = IKAnalyzerClient.class.getClassLoader().getResourceAsStream("test-text.txt");
        InputStreamReader isr = new InputStreamReader(in);
        BufferedReader br = new BufferedReader(isr);
        String line2 = "";
        while ((line2 = br.readLine()) != null) {
            segText(line2, true);
        }

//        EvaluationResult result = evaluation("standard-text.txt", "test-text.txt");
//        System.out.println(result.perfectLineCount);

//        segMore(text);

//        stream();


    }


}
