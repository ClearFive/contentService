package cn.edu.bjtu.weibo.service.serviceImpl;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import cn.edu.bjtu.weibo.dao.WeiboDAO;
import cn.edu.bjtu.weibo.dao.WeiboDAOImpl;
import cn.edu.bjtu.weibo.model.BaseContent;
import cn.edu.bjtu.weibo.model.BaseContentSR;
import cn.edu.bjtu.weibo.model.Weibo;
import cn.edu.bjtu.weibo.service.SearchContentService;
import cn.edu.bjtu.weibo.service.serviceImpl.searchContentServiceImplHelper.MyIkAnalyzer;
/**
 * 
 * 
 * @author 方成龙 14301005 王天然 
 *	Use： lucene-core-6.3.0.jar 
 *		 IKAnalyzer2012_FF.jar
 */
public class SearchContentServiceImpl implements SearchContentService {

	public static final String CONTENT_INDEX_PATH = "content_index";
	
	
	/**
	 * return search List of BaseContentSR
	 * something about "int[] highlighIndex" in BaseContentSR :
	 * The first value in the array is the number of times keyword appears.
	 * The second value in the array is the length of the keyword.
	 * Other values are the indexes where the keyword appears
	 */
	@Override
	public List<BaseContentSR> getSearchedWeiboList(String keyword, int pageIndex, int numberPerPage) {
		// TODO Auto-generated method stub
		return getSearchList(keyword, pageIndex, numberPerPage);
	}
	
	@Override
	public void updateSearchIndex() {
		// TODO Auto-generated method stub
		creatIndex();
	}
	

	public static int[] getHighlighIndex(String keyword, String content){
		List<Integer> index = new ArrayList();
		int per = -1;
		for (int i = 0; i < content.length(); i++) {
            if (content.indexOf(keyword, i) != -1) {
                if (content.indexOf(keyword, i) != per) {
                    per = content.indexOf(keyword, i);
                    index.add(per);
                }
            }
		}
		int[] highlighIndex = new int[index.size()+2];
		highlighIndex[0]=index.size();
		highlighIndex[1]=keyword.length();
		for(int i=0;i<index.size();i++){
			highlighIndex[i+2]=index.get(i);
		}
		
		return highlighIndex;
	}

	 public static String listToString(List<String> stringList){
	        if (stringList==null) {
	            return null;
	        }
	        StringBuilder result=new StringBuilder();
	        boolean flag=false;
	        for (String string : stringList) {
	            if (flag) {
	                result.append("*");
	            }else {
	                flag=true;
	            }
	            result.append(string);
	        }
	        return result.toString();
	    }
	
	 public static List<String> stringToList(String str){
		 //System.out.println(str);
		 List<String> list = new ArrayList();
		 if(str==null)
		 {
			 return list;
		 }
		 else if(str.contains("*"))
		 {
			 String[] str1 = str.split("\\*");  
			 for(String s : str1){  
		           list.add(s);
		        }  
		 }
		 else
		 {
			 list.add(str);
		 }
		 return list;
	 }
	 
	 
	private static synchronized void creatIndex(){
		WeiboDAO weibodao = new WeiboDAOImpl();
		List<String> totalWeibo = weibodao.getTotalWeibo();
		//System.out.println(totalWeibo.size());
		try{
			Directory directory = FSDirectory.open(Paths.get(CONTENT_INDEX_PATH));
			//use IK analyzer
			Analyzer analyzer = new MyIkAnalyzer();
	    	IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
	    	iwc.setOpenMode(OpenMode.CREATE);
	    	IndexWriter writer = new IndexWriter(directory, iwc);
			for(String weiboId : totalWeibo){
				//System.out.println("GET ID: " + weiboId);
				Document doc = new Document();
				String str = null;
				Weibo weibo = weibodao.getWeibo(weiboId);
				doc.add(new StringField("CONTENT", 
						((str = weibo.getContent()) == null) ? "" : str, Field.Store.YES));
				doc.add(new StringField("LIKE", 
						((str = String.valueOf(weibo.getLike())) == null) ? "" : str, Field.Store.YES));
				doc.add(new StringField("DATE", 
						((str = weibo.getDate()) == null) ? "" : str, Field.Store.YES));
				doc.add(new StringField("COMMENT_NUMBER", 
						((str = String.valueOf(weibo.getCommentNumber())) == null) ? "" : str, Field.Store.YES));
				doc.add(new StringField("USERID", 
						((str = weibo.getUserId()) == null) ? "" : str, Field.Store.YES));
				doc.add(new StringField("AT_USER_LIST", 
						((str = listToString(weibo.getAtUserIdList())) == null) ? "" : str, Field.Store.YES));
				doc.add(new StringField("TOPIC_ID_LIST", 
						((str = listToString(weibo.getTopicIdList())) == null) ? "" : str, Field.Store.YES));
				
				//System.out.println("content:"+weibo.getContent()+" like:"+weibo.getLike());
				
				writer.addDocument(doc);
			}
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
	}

	public List<BaseContentSR> getSearchList(String keyword, int pageIndex, int numberPerPage){
		List<BaseContentSR> result = new ArrayList();
		DirectoryReader dr = null;
		int max = pageIndex * numberPerPage;
		try {
			dr = DirectoryReader.open(FSDirectory.open(Paths.get(CONTENT_INDEX_PATH)));
			IndexSearcher searcher = new IndexSearcher(dr);
			String wildcardPatern = "*" + keyword + "*";
			Term contentTerm = new Term("CONTENT", wildcardPatern);			
			WildcardQuery contentQuery = new WildcardQuery(contentTerm);
			BooleanQuery.Builder builder = new BooleanQuery.Builder();
		    builder.add(contentQuery, Occur.SHOULD);
		   // ScoreDoc[] hits = searcher.search(builder.build(),max).scoreDocs;
		    TopDocs docs = searcher.search(builder.build(), 1000);
		    ScoreDoc[] hits = docs.scoreDocs;
		    
		    ArrayList<Document> sortedList =mixSort(searcher,hits,keyword);
		    
		   // System.out.println("KeyWord: "+keyword+" records:"+hits.length);
		    
		    int beginIndex = (pageIndex - 1) * numberPerPage;
		    if(beginIndex>hits.length||beginIndex>max){
		    	return null;
		    }
//		    int numPage = 0;
//		    if(0==hits.length%numberPerPage){numPage=numberPerPage;	    	
//		    }else{numPage=hits.length-beginIndex;}
		    //getResults(searcher, hits, beginIndex, numPage, result);
		    int length = (max<hits.length)?max:hits.length;
		    for(int i=beginIndex;i<length;i++){
		    	//Document doc = searcher.doc(hits[i].doc);
		    	BaseContent basecontent = new BaseContent();
		    	BaseContentSR basecontentSR = new BaseContentSR();
		    	
		    	basecontent.setContent(sortedList.get(i).get("CONTENT"));
		    	basecontent.setLike(Integer.valueOf(sortedList.get(i).get("LIKE")).intValue());
		    	basecontent.setDate(sortedList.get(i).get("DATE"));
		    	basecontent.setCommentNumber(Integer.valueOf(sortedList.get(i).get("COMMENT_NUMBER")).intValue());
		    	basecontent.setUserId(sortedList.get(i).get("USERID"));
		    	
		    	// System.out.println(doc.get("AT_USER_LIST"));
		    	basecontent.setAtUserIdList(stringToList(sortedList.get(i).get("AT_USER_LIST")));
		    	// System.out.println(doc.get("TOPIC_ID_LIST"));
		    	basecontent.setTopicIdList(stringToList(sortedList.get(i).get("TOPIC_ID_LIST")));
		    	
		    	basecontentSR.setBaseContent(basecontent);
		    	
		    	int[] highlighIndex =getHighlighIndex(keyword,basecontent.getContent());
		    	basecontentSR.setHighlighIndex(highlighIndex);
		    	result.add(basecontentSR);
		    }
		   
		    return result;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		return result;
	}
	
	 //对搜索到的结果按照时间、点赞和评论数、关键词是否出现在话题中三项评分，按照得分从高到低返回ArrayList<Document>
    private ArrayList<Document> mixSort(IndexSearcher searcher, ScoreDoc[] hits, String keyword) {
        ArrayList<Document> docsList = new ArrayList<>();
        ArrayList<Integer> originScoreList = new ArrayList<>();
        try {
            for (int i = 0; i < hits.length; i++) {
                docsList.add(searcher.doc(hits[i].doc));
                //Lucene自己的排序作为初始的排序标准 索引号为doc号 值为分数 最高为length 最低分为1
                originScoreList.add(hits.length - i);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        ArrayList<Long> dateList = new ArrayList<>();
        ArrayList<Integer> commentAndLikeNumList = new ArrayList<>();
        ArrayList<Boolean> hasSearchedTopicList = new ArrayList<>();
        for (int i = 0; i < docsList.size(); i++) {
            Document doc = docsList.get(i);
            //dateList 索引号为doc号 值为日期的数字形式
            dateList.add(i, date2Long(doc.get("DATE")));
            //commentAndLikeNumList 索引号为doc号 值为点赞数和评论数的总和
            commentAndLikeNumList.add(i, Integer.valueOf(doc.get("LIKE")).intValue() + Integer.valueOf(doc.get("COMMENT_NUMBER")).intValue());
            //hasSearchedTopicList 索引号为doc号 值为内容里是否含有与搜索关键词相关的话题
            hasSearchedTopicList.add(i, hasSearchedTopic(doc.get("CONTENT"),keyword));
        }
        //给date和commentAndLikeNum排序打分 索引号为doc号 值为分数 最高为length 最低为1
        ArrayList<Integer> dateScoreList=sortDateByBubbleSort(dateList);
        ArrayList<Integer> commentAndLikeNumScoreList=sortNumByBubbleSort(commentAndLikeNumList);

        //排序的话 以下条件会让权重会被提高：
        //时间更新
        //评论和点赞数更高
        //关键词出现在话题里
        //姑且三者权重一样吧
        Float[] weight={1.0f,1.0f,1.0f};
        ArrayList<Float> totalScore=new ArrayList<>();
        int size=dateScoreList.size();
        for(int i=0;i<size;i++)
        {
            float score=0;
            //如果关键词出现在话题里 就给size分 否则0分
            if(hasSearchedTopicList.get(i))
                score=size*weight[2];//当然还要乘以它的权重
            //三个因素加权得到总分数
            score+=weight[0]*dateScoreList.get(i)+weight[1]*commentAndLikeNumScoreList.get(i);
            totalScore.add(i,score);
        }

        //对总分进行降序排序 同时对docsList降序排序 冒泡排序
        for(int i=0;i<size-1;i++)
        {
            for(int j=0;j<size-1-i;j++)
            {
                if(totalScore.get(j)<totalScore.get(j+1))
                {
                    float temp=totalScore.get(j);
                    totalScore.set(j,totalScore.get(j+1));
                    totalScore.set(j+1,temp);

                    Document tempDoc=docsList.get(j);
                    docsList.set(j,docsList.get(j+1));
                    docsList.set(j+1,tempDoc);
                }
            }
        }
        return docsList;
    }

    //mixSort()调用的方法
    private long date2Long(String date) {
        //2016/12/23 10:11:19
        String[] twoParts = date.split(" ");
        String[] firstParts = twoParts[0].split("/");
        long result = Integer.parseInt(firstParts[0]) * 10000000000l;
        result += Integer.parseInt(firstParts[1]) * 100000000l;
        result += Integer.parseInt(firstParts[2]) * 1000000;
        String[] secondParts = twoParts[1].split(":");
        result += Integer.parseInt(secondParts[0]) * 10000;
        result += Integer.parseInt(secondParts[1]) * 100;
        result += Integer.parseInt(secondParts[2]);
        return result;
    }

    //mixSort()调用的方法
    private static boolean hasSearchedTopic(String content, String keyword) {
        //判断搜索的关键字是否出现在了内容的话题里
        String reg="#(.+)#";
        Pattern pattern=Pattern.compile(reg);
        Matcher matcher=pattern.matcher(content);
        if(matcher.find()) {
            if (matcher.group().contains(keyword))
                return true;
            return false;
        }
        else
            return false;
    }

    //mixSort()调用的方法
    private ArrayList<Integer> sortDateByBubbleSort(ArrayList<Long> inputList)
    {
        ArrayList<Long> myList=inputList;
        //冒泡升序排列 得到myList
        long temp=0;
        int size=myList.size();
        for(int i=0;i<size-1;i++)
        {
            for(int j=0;j<size-1-i;j++)
            {
                if(myList.get(j)>myList.get(j+1))
                {
                    temp=myList.get(j);
                    myList.set(j,myList.get(j+1));
                    myList.set(j+1,temp);
                }
            }
        }

        //根据myList的顺序 给每项打分 最高size 最低1
        ArrayList<Integer> scoreList=new ArrayList<>();
        for(int i=0;i<size;i++)
        {
            scoreList.add(i,1+myList.indexOf(inputList.get(i)));
        }
        return scoreList;
    }

    //mixSort()调用的方法
    private ArrayList<Integer> sortNumByBubbleSort(ArrayList<Integer> inputList)
    {
        ArrayList<Integer> myList=inputList;
        //冒泡升序排列 得到myList
        int temp=0;
        int size=myList.size();
        for(int i=0;i<size-1;i++)
        {
            for(int j=0;j<size-1-i;j++)
            {
                if(myList.get(j)>myList.get(j+1))
                {
                    temp=myList.get(j);
                    myList.set(j,myList.get(j+1));
                    myList.set(j+1,temp);
                }
            }
        }

        //根据myList的顺序 给每项打分 最高size 最低1
        ArrayList<Integer> scoreList=new ArrayList<>();
        for(int i=0;i<size;i++)
        {
            scoreList.add(i,1+myList.indexOf(inputList.get(i)));
        }
        return scoreList;
    }
}
