package cn.edu.bjtu.weibo.service.serviceImpl.searchContentServiceImplHelper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.util.IOUtils;

import java.io.Reader;
import java.io.StringReader;
/**
 * Created by lovejoy on 2016/12/24.
 */
public class MyIkAnalyzer extends Analyzer{
    @Override
    protected TokenStreamComponents createComponents(String arg0) {
        Reader reader=null;
        try{
            reader=new StringReader(arg0);
            MyIKTokenizer it = new MyIKTokenizer(reader);
            return new Analyzer.TokenStreamComponents(it);
        }finally {
            IOUtils.closeWhileHandlingException(reader);
        }
    }
}
