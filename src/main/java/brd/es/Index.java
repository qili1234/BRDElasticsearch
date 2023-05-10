package brd.es;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.Properties;
import java.util.HashMap;
import java.util.Set;
import java.util.Date;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.http.HttpHost;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.BodyContentHandler;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.rest.RestStatus;




public class Index {
	
	String _indexSop = "sop";
	String _indexStudy = "study";
	String _indexSpecimen = "specimen";
	String _indexPaperSub = "papersub";
	String _indexSopSub = "sopsub";
	int _bulkSize=100;
	Connection _con = null;
	RestHighLevelClient _client;

	
   
    public Index(String dbUrl, String dbUser, String dbPassword, String host, int port, String scheme,  int bulkSize) throws Exception{
    	
    	Class.forName("oracle.jdbc.driver.OracleDriver"); 
	   //  System.out.println("make connection dbUrl: " + dbUrl+ " dbUser: " + dbUser + " dbPassword: " + dbPassword);
    	
	     _con = DriverManager.getConnection(dbUrl, dbUser, dbPassword);

	     _bulkSize=bulkSize;
	     
	     _client= new RestHighLevelClient(
                RestClient.builder(new HttpHost(host, port, scheme)));
	  }
    
    public RestHighLevelClient getClient(){
		return _client;
    }
	
    public Connection getConnection(){
		return _con;
    }
   
    public void closeClient() throws Exception {
	   _client.close();;
    }
	
    public void closeConnection() throws Exception {
	   _con.close();
    }
    
    public static void main(String[] args) {
    	System.out.println("start: " + (new Date()).toString());
    	Options options = new Options();
		 options.addOption("o", "operation", true, "operation.");
		 options.addOption("r", "record", true, "Record.");
		 options.addOption("i", "id", true, "Study ID or SOP ID.");
		 options.addOption("bo", "brd-only", false, "Only index study and sop");
		 
		 CommandLineParser parser = new BasicParser();
		 CommandLine cmd = null;
    	
    	
    	
    	 Connection con = null;
		 RestHighLevelClient client=null;
    	 String propPath="/var/storage/conf/brd/BRDElasticsearch7.properties";
    	 Index index =null;
        try {
        	
        	 cmd = parser.parse(options, args);
			 String operation="";
			  if (cmd.hasOption("o") ){
				  operation=cmd.getOptionValue("o");
			  }
			  
			  
			  String record="";
			  if (cmd.hasOption("r")){
				  record = cmd.getOptionValue("r");
			  }
			  
			  
			  String recordId="";
			  if (cmd.hasOption("i")){
				  recordId = cmd.getOptionValue("i");
			  }
			  
			  
			  if((cmd.hasOption("o") && cmd.hasOption("r") && cmd.hasOption("i")&&!cmd.hasOption("bo")) || (!cmd.hasOption("o") && !cmd.hasOption("r") && !cmd.hasOption("i") && !cmd.hasOption("bo")) || (!cmd.hasOption("o") && !cmd.hasOption("r") && !cmd.hasOption("i") && cmd.hasOption("bo"))  ){
				 
			  }else{
				  throw (new ParseException("wrong parameters")); 
			  }
			  
        	
			  if( !operation.equals("") && !operation.equals("index") && !operation.equals("delete")){
				  throw new ParseException("wrong parameters");
			  }
        	
			  
			  if( !record.equals("") && !record.equals("study") && !record.equals("sop") && !record.equals("papersub") && !record.equals("sopsub")){
				  throw new ParseException("wrong parameters");
			  }
        	
        	 InputStream input = new FileInputStream(propPath);
			 Properties prop=new Properties();	
			 prop.load(input);
				
		     String dbUrl=prop.getProperty("db_url");
			 String dbUser=prop.getProperty("db_user");
			 //System.out.println("dbUser: " + dbUser);
			 String dbPassword = prop.getProperty("db_password");
			 //System.out.println("dbPassword:" + dbPassword );
			 String host = prop.getProperty("host");
			 String port = prop.getProperty("port");
			 String scheme = prop.getProperty("scheme");
			 String bulkSizeStr = prop.getProperty("bulk_size");
				
			 int bulkSize = Integer.parseInt(bulkSizeStr);
			 int iport = Integer.parseInt(port);
        	
			 index = new Index(dbUrl, dbUser, dbPassword, host, iport, scheme, bulkSize);
			 
			 if(operation.equals("index") && record.equals("study")){
				 // logger.info("start index case " + recordId);
				 index.indexStudy(recordId);
				  
			  }else if(operation.equals("delete") && record.equals("study")){
					 // logger.info("start index case " + recordId);
					 index.deleteStudy(recordId);
					  
		      }else if (operation.equals("index") && record.equals("sop")) {
				  index.indexSop(recordId);
			  }else if (operation.equals("delete") && record.equals("sop")) {
				  index.deleteSop(recordId);
			  }else if (operation.equals("index") && record.equals("papersub")) {
			       index.indexPaperSubmission(recordId);
		      }else if (operation.equals("index") && record.equals("sopsub")) {
			       index.indexSopSubmission(recordId);
		      }else if(cmd.hasOption("bo")) {
				  //System.out.println("this is bo");
				 index.indexStudy("ALL");
				 index.indexSop("ALL");
				 index.indexPaperSubmission("ALL");
				 index.indexSopSubmission("ALL");
			  }else {
			 
			     index.indexSpecimen();
			     index.indexStudy("ALL");
			     index.indexSop("ALL");
				  index.indexPaperSubmission("ALL");
				  index.indexSopSubmission("ALL");
			  }
        	
	      System.out.println("end: " + (new Date()).toString());
	      index.closeClient();
	      index.closeConnection();
         }catch(ParseException e2){
	    	   //e2.printStackTrace();
	    	   System.out.println("usage: java -jar brd-index.jar -o [index|delete] -r [study|sop|papersub|sopsub] -i [studyId]|[sopId]|[papersubId][sopsub]");	 
	    	   System.exit(1);
			 
		 }catch (Exception e) {
			  StackTraceElement[] stack = e.getStackTrace();
			    String exception = e.toString()+"\n\t\t";
			    for (StackTraceElement s : stack) {
			        exception = exception + s.toString() + "\n\t\t";
			    }
			    
			    System.out.println(exception);
			   
        }
    }
    
    public void indexStudy(String studyId)  throws Exception{
    	Study study=new Study(_con);
    	if(!studyId.equals("ALL")) {
    		 int id = study.getIdFromStudyId(studyId);
			  if(id== -1){
				  throw new Exception("no such study id: " + studyId);
			  }
			  
    	}
    	Map<String, Map> studyMap =study.getStudyMap(studyId);
    	System.out.println("study size: " + studyMap.size());
    	startIndex("study", studyMap);
    }
    
    public void indexSopSubmission(String sopSubId)  throws Exception{
    	SopSubmission sopSub=new SopSubmission (_con);
    	if(!sopSubId.equals("ALL")) {
    		 int id = sopSub.getIdFromSopSubmissionId(sopSubId);
			  if(id== -1){
				  throw new Exception("no such paper submission id: " + sopSubId);
			  }
			  
    	}
    	Map<String, Map> sopSubMap =sopSub.getSopSubmissionMap(sopSubId);
    	System.out.println("sop submission size:  " + sopSubMap.size());
    	startIndex("sopsub", sopSubMap);
    }
    
    public void indexPaperSubmission(String paperSubId)  throws Exception{
    	PaperSubmission paperSub=new PaperSubmission (_con);
    	if(!paperSubId.equals("ALL")) {
    		 int id = paperSub.getIdFromPaperSubmissionId(paperSubId);
			  if(id== -1){
				  throw new Exception("no such paper submission id: " + paperSubId);
			  }
			  
    	}
    	Map<String, Map> paperSubMap =paperSub.getPaperSubmissionMap(paperSubId);
    	System.out.println("paper submission size: " + paperSubMap.size());
    	startIndex("papersub", paperSubMap);
    }
    
    public void indexSpecimen()  throws Exception{
    	Specimen specimen =new Specimen(_con);
    	Map<String, Map> specimenMap =specimen.getSpecimenMap();
    	System.out.println("speimen size: " + specimenMap.size());
    	startIndex("specimen", specimenMap);
    }
    
    public void startIndex(String type, Map<String, Map> data)  throws Exception{
		 
    	//	 logger.info("type:" + type);
    			
    	      String indexName=null;
    	     // String typeName=null;
    	      if(type=="study"){
    	    	  indexName=_indexStudy;
    	    	 // typeName=_typeCase;
    	      
    	      }else if(type=="specimen"){
    	    	  indexName=_indexSpecimen;
    	      }else if(type=="papersub") {
    	    	  indexName=_indexPaperSub;
    	      }else if(type=="sopsub") {
    	    	  indexName=_indexSopSub;
    	      }
    	     
    	      //System.out.println("url: " + url);
    	      
    	      Set<String> keys = data.keySet();
    	      Object[] caseArray=keys.toArray();
    	      int arraySize=caseArray.length;
    	    
    	      
    	      int j = 1;
    	      BulkRequest request=new BulkRequest();
    	      for(int i=0; i<arraySize; i++){
    	    	  
    	    	 
    	           if(i == (j*_bulkSize) ){   
    	        	 
    	        	   _client.bulk(request, RequestOptions.DEFAULT);
    	        	
    	        	     j++;
    	        	   request=null;
    	        	   request=new BulkRequest();
    	        	      
    	           }
    	    	  String idStr =(String)caseArray[i];
    	    	  Map element = data.get(idStr);
    	    	  request.add(new IndexRequest(indexName).id(idStr)
    		                .source(element));
    	    	  
    	           
    	      }
    	     
    	      _client.bulk(request, RequestOptions.DEFAULT);
    	  
    	     
    	    
    		}
    
    public void indexSop(String sopId)  throws Exception{
    	String indexName="sop";
        Sop sop = new Sop(_con);
        Map<String, Map> sopMap =sop.getSopMap(sopId);
        System.out.println("sop size: " + sopMap.size());
        
        Set<String> keys = sopMap.keySet();
	      Object[] caseArray=keys.toArray();
	      int arraySize=caseArray.length;
	      
	      for(int i = 0; i < arraySize ; i++) {
	          String idStr =(String)caseArray[i];
  	      Map element = sopMap.get(idStr);
	          String contents = sop.getContents(idStr);
  	     // System.out.println("idStr: " + idStr);
  	      //System.out.println("contents: " + contents);
  	     // Map<String, String> element = new HashMap();
  	      element.put("body", contents);
  	      BulkRequest request=new BulkRequest();
  	      request.add(new IndexRequest(indexName).id(idStr)
		                .source(element));
  	      _client.bulk(request, RequestOptions.DEFAULT);
	      }
    	
    	
    }
    
    public boolean deleteStudy(String id) throws Exception {
        try {
            DeleteRequest request = new DeleteRequest("study",  id);
            DeleteResponse deleteResponse = _client.delete(request, RequestOptions.DEFAULT);
            return deleteResponse.isFragment();
        } catch (ElasticsearchException exception) {
            if (exception.status() == RestStatus.NOT_FOUND) {

            }
        }
        return false;
    }
    
    
    public boolean deleteSop(String id) throws Exception {
        try {
            DeleteRequest request = new DeleteRequest("sop",  id);
            DeleteResponse deleteResponse = _client.delete(request, RequestOptions.DEFAULT);
            return deleteResponse.isFragment();
        } catch (ElasticsearchException exception) {
            if (exception.status() == RestStatus.NOT_FOUND) {

            }
        }
        return false;
    }
}
