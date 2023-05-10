package brd.es;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.io.InputStream;




import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.RecursiveParserWrapper;
import org.apache.tika.sax.BasicContentHandlerFactory;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.ContentHandlerFactory;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class Sop {
Connection _con;
	
	public Sop(Connection con){
		_con=con;
	}

	
public int getIdFromSopId(String sopId)  throws Exception{
		
		PreparedStatement stmt=null;
		 ResultSet rs = null;

		  int id = -1;

		String sql="select id from sop where id ='" + sopId + "'";
		try{

			   stmt=_con.prepareStatement(sql);
			 
			    rs = stmt.executeQuery();
              
			    while(rs.next()){
			    	 id = rs.getInt("id");
			      
			     

			       
			     //   System.out.println("id  : " + id + " caseId: " + caseId);

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

	public String getContents(String sopId)  throws Exception{
		
		PreparedStatement stmt=null;
		 ResultSet rs = null;

		 

		String sql="select id, body from sop  where id = " + sopId;
		  AutoDetectParser parser = new AutoDetectParser();
 		  BodyContentHandler handler = new BodyContentHandler(-1);
 	      Metadata metadata = new Metadata();
 	      String contents="";
          
		try{

			   stmt=_con.prepareStatement(sql);
			 
			    rs = stmt.executeQuery();
              
			    while(rs.next()){
			    	
			    	 InputStream is = rs.getBinaryStream("body");
			    	 if(is != null) {
		             parser.parse(is, handler, metadata);
			         contents = handler.toString();
			    	 }

			       
			     // System.out.println("id : " + id + " caseId: " + caseId);

			    }
			   }catch(Exception e){
			      throw e;
			   }finally{

			       if(stmt != null)
			          stmt.close();

			       if(rs != null)
			          rs.close();

			   }

       return contents;
		
	}
	
	
	public Map<String, Map> getSopMap()  throws Exception{
		return getSopMap("ALL");
	}
	
	public Map<String, Map> getSopMap(String sopId)  throws Exception{
		Map<String, List<String>> expFactors=null;
	  if (sopId.equals("ALL")){
		 expFactors=getExpFactors("ALL");	
	   }
		
		Map<String, List<String>> types=getTypes(sopId);
		Map<String, List<String>> diagnoses=getDiagnoses(sopId);
		PreparedStatement stmt=null;
		 ResultSet rs = null;
		Map<String, Map> sopMap= new HashMap<String, Map>();

		

		String sql= "select sop.id as sop_id, sop.description, sop.body_file_name, sop.status, sop.metadata_id, meta.tier, ss.name as source, meta.title from sop, sop_metadata meta, " 
				+ "sop_source ss " 
				+ "where sop.metadata_id = meta.id and sop.source_id=ss.id";
		if (sopId.equals("ALL")){
			
		}else{
			sql += " and sop.id ="+sopId+"";
		}
				
				
		try{

			   stmt=_con.prepareStatement(sql);
			 
			    rs = stmt.executeQuery();
			    
			    while(rs.next()){
			    	
			    	 int id = rs.getInt("sop_id");
			         String description = rs.getString("description");
			         String body_file_name = rs.getString("body_file_name");
			        
			         String tier = rs.getString("tier");
			         String source = rs.getString("source");
			         String title = rs.getString("title");
			         int mid=rs.getInt("metadata_id");
			         
			       
			      
			         
			         Map  map= new HashMap();
			        // map.put("id",id);
			         map.put("description", description);
			         map.put("body_file_name", body_file_name);
                    map.put("tier", tier );
                  
                    map.put("source", source);
                    map.put("title", title);
                    //map.put("metadata_id", metadata_id);
                    String midStr=Integer.toString(mid);
                    if(!sopId.equals("ALL")) {
                    	expFactors=getExpFactors(midStr);	
                    }
                    
                    String idStr = Integer.toString(id);
                    map.put("topics", expFactors.get(midStr));
                    map.put("biospecimens", types.get(idStr));
                    map.put("diagnoses", diagnoses.get(idStr));
                  
                    
			    
                  
			        sopMap.put(idStr, map);
			    }
			   }catch(Exception e){
			      throw e;
			   }finally{

			       if(stmt != null)
			          stmt.close();

			       if(rs != null)
			          rs.close();

			   }


		return sopMap;
	}
	
	
	public Map<String, List<String>> getExpFactors(String metaId)  throws Exception{
		PreparedStatement stmt=null;
		 ResultSet rs = null;
		 Map<String,List<String>> factors = new HashMap<String, List<String>>();

		String sql="select  sf.sop_metadata_id, ex.name as factor  from sop_factor_class sf, experimental_fctr_class ex "
				+ "where sf.experimental_fctr_class_id=ex.id";
		
	   if (metaId.equals("ALL")){
			
		}else{
			sql += " and sop_metadata_id ="+metaId+"";
		}
				
		
		try{

			   stmt=_con.prepareStatement(sql);
			 
			    rs = stmt.executeQuery();

			    while(rs.next()){
			    	 int metadataId = rs.getInt("sop_metadata_id");
			         String factor = rs.getString("factor");
                     String metadataIdStr = Integer.toString(metadataId);
                     List<String> factorList = factors.get( metadataIdStr);
                     if(factorList==null){
                    	 factorList = new LinkedList<String>();
                    	 factors.put(metadataIdStr, factorList);
                     }
                     factorList.add(factor);
			     

			       
			     // System.out.println("id : " + id + " caseId: " + caseId);

			    }
			   }catch(Exception e){
			      throw e;
			   }finally{

			       if(stmt != null)
			          stmt.close();

			       if(rs != null)
			          rs.close();

			   }


		return factors;
	}
	
	
	public Map<String, List<String>> getTypes(String sopId)  throws Exception{
		PreparedStatement stmt=null;
		 ResultSet rs = null;
		 Map<String,List<String>> types = new HashMap<String, List<String>>();

		String sql=" select stl.sop_id, t.name as btype, l.name as blocation from sop_type_location stl, biospecimen_type_location tl, biospecimen_type t, biospecimen_location l " +
				"where stl.biospecimen_type_location_id=tl.id and tl.biospecimen_type_id=t.id and tl.biospecimen_location_id=l.id";
		
		 if (sopId.equals("ALL")){
				
			}else{
				sql += " and stl.sop_id ="+sopId+"";
			}
					
		
		try{

			   stmt=_con.prepareStatement(sql);
			 
			    rs = stmt.executeQuery();

			    while(rs.next()){
			    	 int id = rs.getInt("sop_id");
			         String type = rs.getString("btype");
			    	 String location = rs.getString("blocation");
                     String idStr = Integer.toString(id);
                     List<String> typeList = types.get(idStr);
                     if(typeList==null){
                    	 typeList = new LinkedList<String>();
                    	 types.put(idStr, typeList);
                     }
                     typeList.add(type + " - " + location);
			     

			       
			     // System.out.println("id : " + id + " caseId: " + caseId);

			    }
			   }catch(Exception e){
			      throw e;
			   }finally{

			       if(stmt != null)
			          stmt.close();

			       if(rs != null)
			          rs.close();

			   }


		return types;
	}
	
	public Map<String, List<String>> getDiagnoses(String sopId)  throws Exception{
		PreparedStatement stmt=null;
		 ResultSet rs = null;
		 Map<String,List<String>> diagnoses = new HashMap<String, List<String>>();

		String sql="select sd.sop_id, di.name as diagnosis, sudi.name as sub_diagnosis from sop_diagnosis sd, diagnosis di, sub_diagnosis sudi "
				+ "where sd.diagnosis_id=di.id and sudi.id (+)= sd.sub_diagnosis_id ";
		
		 if (sopId.equals("ALL")){
				
			}else{
				sql += " and sd.sop_id ="+sopId+"";
			}
					
				
		try{

			   stmt=_con.prepareStatement(sql);
			 
			    rs = stmt.executeQuery();

			    while(rs.next()){
			    	 int id = rs.getInt("sop_id");
			         String diagnosis = rs.getString("diagnosis");
			    	 String sub_diagnosis = rs.getString("sub_diagnosis");
			    	 //System.out.println("sub_diagnosis: " + sub_diagnosis );
			    	 
                     String idStr = Integer.toString(id);
                     List<String> diagnosisList = diagnoses.get(idStr);
                     if(diagnosisList==null){
                    	 diagnosisList = new LinkedList<String>();
                    	 diagnoses.put(idStr, diagnosisList);
                     }
                     if(sub_diagnosis!=null){
                    	 diagnosisList.add(diagnosis + " - " + sub_diagnosis);
			    	 }else {
			    		 diagnosisList.add(diagnosis);
			    	 }
                    
			     

			       
			     // System.out.println("id : " + id + " caseId: " + caseId);

			    }
			   }catch(Exception e){
			      throw e;
			   }finally{

			       if(stmt != null)
			          stmt.close();

			       if(rs != null)
			          rs.close();

			   }


		return diagnoses;
	}
}
