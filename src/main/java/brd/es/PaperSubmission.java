package brd.es;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.BodyContentHandler;

public class PaperSubmission {
	
		Connection _con;
			
			public PaperSubmission(Connection con){
				_con=con;
			}

			
		public int getIdFromPaperSubmissionId(String paperSubId)  throws Exception{
				
				PreparedStatement stmt=null;
				 ResultSet rs = null;

				  int id = -1;

				String sql="select id from paper_submission where id ='" + paperSubId + "'";
				try{

					   stmt=_con.prepareStatement(sql);
					 
					    rs = stmt.executeQuery();
		              
					    while(rs.next()){
					    	 id = rs.getInt("id");
					      
					     

					       
					     //   System.out.println("id : " + id + " caseId: " + caseId);

					    }
					   }catch(Exception e){
					      throw e;
					   }finally{

					       if(stmt != null)
					          stmt.close();

					       if(rs != null)
					          rs.close();

					   }

		       return id;
				
			}

		
		
		
		
		public Map<String, Map> getPaperSubmissionMap(String paperSubId)  throws Exception{
			
			PreparedStatement stmt=null;
			 ResultSet rs = null;
			Map<String, Map> paperSubMap= new HashMap<String, Map>();

			

			String sql=" select ps.id, to_char(ps.date_created,  'yyyy-MM-dd') ||'T'|| to_char(ps.date_created,'HH24:MI:SS')||'.000Z' as ps_date_created, "+
			          " ps.pub_med_id, ps.title, ps.publication_name, ps.publication_year, ps.page_number, ps.is_review_paper, "+
					" ps.is_published_paper,  to_char(ps.unpublished_paper_date,  'yyyy-MM-dd') ||'T'|| to_char(ps.unpublished_paper_date,'HH24:MI:SS')||'.000Z' as unpublished_paper_date, " +
			          " ps.volume, ps.comments, ps.curator_comments, "+
					" ps.authorslink, ps.is_archived, ps.submitter_name, ps.submitter_email, ps.submitter_org, ps.display_option, "+
					" p.id as paper_id, p.pub_med_id as paper_pub_med_id, p.title as paper_title, p.publication_name as paper_publication_name, "+
					" p.publication_year as paper_publication_year, p.curator_purpose as paper_curator_purpose, p.curator_conclusion as paper_curator_conclusion, "+
					" p.authorslink as paper_authorslink, p.is_private as paper_is_private, p.suggested_by as paper_suggested_by   from paper_submission ps, "+
					" (select paper.id, paper.pub_med_id, paper.title, paper.publication_name, paper.publication_year, paper.curator_purpose, paper.curator_conclusion, "+
					" paper.authorslink, ds.is_private, paper.suggested_by  from paper, document_status ds where paper.document_status_id=ds.id) p "+
					" where p.id (+)= ps.paper_id";
					
			if (paperSubId.equals("ALL")){
				
			}else{
				sql += " and ps.id ="+paperSubId+"";
			}
					
		   	
			try{

				   stmt=_con.prepareStatement(sql);
				 
				    rs = stmt.executeQuery();
				    
				
				    

				    while(rs.next()){
				    	
				    	 int id = rs.getInt("id");
				    	 String ps_date_created = rs.getString("ps_date_created");
				    	 int pub_med_id = rs.getInt("pub_med_id");
				    	 String title = rs.getString("title");
				    	 String publication_name = rs.getString("publication_name");
				    	 String publication_year = rs.getString("publication_year");
				    	
				         String page_number=rs.getString("page_number");
				         int is_review_paper = rs.getInt("is_review_paper");
				         int is_published_paper = rs.getInt("is_published_paper");
				         String unpublished_paper_date = rs.getString("unpublished_paper_date");
				         String volume = rs.getString("volume");
				         String comments=rs.getString("comments");
				         String curator_comments=rs.getString("curator_comments");
				         String authorslink=rs.getString("authorslink");
				         int is_archived = rs.getInt("is_archived");
				        // System.out.println("is_archived: " + is_archived);
				         String submitter_name=rs.getString("submitter_name");
				         String submitter_email=rs.getString("submitter_email");
				         String submitter_org=rs.getString("submitter_org");
				         String display_option=rs.getString("display_option");
				         int paper_id = rs.getInt("paper_id");
				         String paper_pub_med_id =rs.getString("paper_pub_med_id");
				         String paper_title =rs.getString("paper_title");
				         String paper_publication_name =rs.getString("paper_publication_name");
				         String paper_publication_year =rs.getString("paper_publication_year");
				         String paper_curator_purpose =rs.getString("paper_curator_purpose");
				         String paper_curator_conclusion=rs.getString("paper_curator_conclusion");
				         String paper_authorslink=rs.getString("paper_authorslink");
				         int paper_is_private=rs.getInt("paper_is_private");
				         String paper_suggested_by=rs.getString("paper_suggested_by");
				       
				         
				         Map  map= new HashMap();
				         map.put("ps_date_created", ps_date_created);
				         map.put("pub_med_id", pub_med_id);
				         map.put("title", title);
				         map.put("publication_name", publication_name);
				         int year_int=0;
				         try {
				        	 year_int=Integer.parseInt(publication_year);
				         }catch (NumberFormatException e) {
				        	    
				         }
				         map.put("publication_year", year_int);
				         map.put("page_number", page_number);
				         if(is_review_paper==1) {
				            map.put("is_review_paper", true);
				         }else {
				        	 map.put("is_review_paper", false); 
				         }
				         
				         if(is_published_paper==1) {
				        	 map.put("is_published_paper", true);
				         }else {
				        	 map.put("is_published_paper", false);
				         }				         
				         
				         if(unpublished_paper_date.equals("T.000Z")) {
				              map.put("unpublished_paper_date", null);
				         }else {
				        	 map.put("unpublished_paper_date", unpublished_paper_date);
				         }
				         map.put("publication_year", year_int);
				         map.put("volume", volume);
				         map.put("comments", comments);
				         map.put("curator_comments", curator_comments);
				         map.put("authorslink", authorslink);
				         if(is_archived==1) {
				        	// System.out.println("is_archived: true");
				             map.put("is_archived", true);
				         }else {
				        	 map.put("is_archived", false);
				        	// System.out.println("is_archived: false");
				         }
				         
				         map.put("submitter_name", submitter_name);
				         map.put("submitter_email", submitter_email);
				         map.put("submitter_org", submitter_org);
				         map.put("display_option", display_option);
				         map.put("paper_id", paper_id);
				         
				         map.put("paper_pub_med_id", paper_pub_med_id);
				         map.put("paper_title", paper_title);
				         map.put("paper_publication_name", paper_publication_name);
				         int paper_year_int=0;
				         try {
				        	 paper_year_int=Integer.parseInt(paper_publication_year);
				         }catch (NumberFormatException e) {
				        	    
				         }
				         map.put("paper_publication_year", paper_year_int);
				         map.put("paper_curator_purpose", paper_curator_purpose);
				         map.put("paper_curator_conclusion", paper_curator_conclusion);
				         map.put("paper_authorslink", paper_authorslink);
				         if(paper_is_private==1) {
				        	 map.put("paper_is_private", true);
				         }else {
				        	 map.put("paper_is_private", false);
				         }
				         
				         map.put("paper_suggested_by", paper_suggested_by);
				    	
				         
				      
				         
				         String idStr = Integer.toString(id);
				       
				      
				       
				         paperSubMap.put(idStr, map);
				    }
				   }catch(Exception e){
				      throw e;
				   }finally{

				       if(stmt != null)
				          stmt.close();

				       if(rs != null)
				          rs.close();

				   }


			return paperSubMap;
		}
			
			
			

}
