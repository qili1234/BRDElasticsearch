package brd.es;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

public class SopSubmission {
	

	Connection _con;
		
		public SopSubmission(Connection con){
			_con=con;
		}

		
	public int getIdFromSopSubmissionId(String sopSubId)  throws Exception{
			
			PreparedStatement stmt=null;
			 ResultSet rs = null;

			  int id = -1;

			String sql="select id from sop_submission where id ='" + sopSubId + "'";
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

	
	
	
	
	public Map<String, Map> getSopSubmissionMap(String sopSubId)  throws Exception{
		
		PreparedStatement stmt=null;
		 ResultSet rs = null;
		Map<String, Map> sopSubMap= new HashMap<String, Map>();

		

		String sql=" select ss.id, to_char(ss.date_created,  'yyyy-MM-dd') ||'T'|| to_char(ss.date_created,'HH24:MI:SS')||'.000Z' as ss_date_created, "+
				" ss.submission_id, ss.submitter_name, ss.submitter_tel, "+
				" ss.submitter_email, ss.title, ss.ver, ss.description, ss.body_file_name, "+
				" ss.source, ss.note, ss.previous_sub_id, ss.biospecimens, ss.diagnoses, ss.topics, ss.curator_comments, "+
				" ss.is_archived,  ss.is_new, ss.sop_id from sop_submission ss";
				
		if (sopSubId.equals("ALL")){
			
		}else{
			sql += " where ss.id ="+sopSubId+"";
		}
				
	   	
		try{

			   stmt=_con.prepareStatement(sql);
			 
			    rs = stmt.executeQuery();
			    
			
			    

			    while(rs.next()){
			    	
			    	 int id = rs.getInt("id");
			    	 String ss_date_created = rs.getString("ss_date_created");
			    	 String submission_id = rs.getString("submission_id");
			    	 String submitter_name=rs.getString("submitter_name");
			    	 String submitter_tel=rs.getString("submitter_tel");
			         String submitter_email=rs.getString("submitter_email");
			    	 String title = rs.getString("title");
			    	
			    	 String ver = rs.getString("ver");
			    	
			         String description=rs.getString("description");
			         String body_file_name=rs.getString("body_file_name");
			         String source=rs.getString("source");
			         String note=rs.getString("note");
			         String previous_sub_id =rs.getString("previous_sub_id");
			         String biospecimens =rs.getString("biospecimens");
			         String diagnoses =rs.getString("diagnoses");
			         String topics =rs.getString("topics");
			         String curator_comments =rs.getString("curator_comments");
			         int is_archived = rs.getInt("is_archived");
			         int is_new = rs.getInt("is_new");
			         
			      
			         
			         Map  map= new HashMap();
			         map.put("ss_date_created", ss_date_created);
			         map.put("submission_id", submission_id);
			         map.put("submitter_name", submitter_name);
			         map.put("submitter_tel", submitter_tel);
			         map.put("submitter_email", submitter_email);
			         map.put("title", title);
			         map.put("ver", ver);
			         map.put("description", description );
			         map.put("body_file_name", body_file_name );
			         map.put("source", source);
			         map.put("note", note);
			         map.put("previous_sub_id", previous_sub_id);
			         map.put("biospecimens", biospecimens);
			         map.put("diagnoses", diagnoses);
			         map.put("topics", topics);
			         map.put("curator_comments", curator_comments);
			         if(is_archived==1) {
			             map.put("is_archived", true);
			         }else {
			        	 map.put("is_archived", false);
			         }
			         
			         if(is_new==1) {
			             map.put("is_new", true);
			         }else {
			        	 map.put("is_new", false);
			         }
			         
			     
			      
			         
			         String idStr = Integer.toString(id);
			       
			      
			       
			         sopSubMap.put(idStr, map);
			    }
			   }catch(Exception e){
			      throw e;
			   }finally{

			       if(stmt != null)
			          stmt.close();

			       if(rs != null)
			          rs.close();

			   }


		return sopSubMap;
	}
		
		

}
