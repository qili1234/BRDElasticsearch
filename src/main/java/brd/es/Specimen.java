package brd.es;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Specimen {
Connection _con;
	
	public Specimen(Connection con){
		_con=con;
	}
	
	public Map<String, Map> getSpecimenMap()  throws Exception{
		
		PreparedStatement stmt=null;
		 ResultSet rs = null;
		Map<String, Map> specimenMap= new HashMap<String, Map>();

		   

		String sql="select s.id, s.display_order, s.public_id as specimenPublicId, c.public_id as casePublicId,\r\n"
				+ "c.age_range, c.gender, st.name as caseStatus, st.code as  caseStatusCode, at.name as tissue, at.code as tissueCode,\r\n"
				+ "fi.name as fixative, fi.code as fixativeCode, ps.comments as prcComments, ps.autolysis, ins.name as acceptability, ins.code as acceptabilityCode\r\n"
				+ " from dr_specimen s, dr_case c, st_case_status st,\r\n"
				+ "st_acquis_type at, st_fixative fi, prc_specimen ps, st_inventory_status ins\r\n"
				+ "where s.case_record_id = c.id and c.study_id=1 and s.public_id is not null and  s.public_id <> 'ZZZZZZZZ'\r\n"
				+ "and c.case_status_id=st.id and s.tissue_type_id=at.id and s.fixative_id=fi.id and s.prc_specimen_id = ps.id\r\n"
				+ "and ps.inventory_status_id=ins.id";
	
				
		try{

			   stmt=_con.prepareStatement(sql);
			 
			    rs = stmt.executeQuery();
			    
			    while(rs.next()){
			    	
			    	 int id = rs.getInt("id");
			    	 int displayOrder = rs.getInt("display_order");
			         String specimenPublicId = rs.getString("specimenPublicId");
			         String casePublicId = rs.getString("casePublicId");
			         String ageRange = rs.getString("age_range");
			         String caseStatus = rs.getString("caseStatus");
			         String caseStatusCode = rs.getString("caseStatusCode");
			         String gender = rs.getString("gender");
			         String tissue = rs.getString("tissue");
			         String tissueCode = rs.getString("tissueCode");
			         String fixative = rs.getString("fixative");
			         String fixativeCode = rs.getString("fixativeCode");
			         String prcComments= rs.getString("prcComments");
			         String autolysis= rs.getString("autolysis");
			         String acceptability= rs.getString("acceptability");
			         String acceptabilityCode= rs.getString("acceptabilityCode");
			         
			         
			       
			      
			         
			         Map  map= new HashMap();
			       
			         map.put("displayOrder", displayOrder);
			         map.put("specimenPublicId", specimenPublicId);
                     map.put("casePublicId", casePublicId );
                     map.put("ageRange", ageRange );
                     map.put("caseStatus", caseStatus);
                     map.put("caseStatusCode", caseStatusCode);
                     map.put("gender", gender);
                     map.put("tissue", tissue);
                     map.put("tissueCode", tissueCode);
                     map.put("fixative", fixative);
                     map.put("fixativeCode", fixativeCode);
                     map.put("prcComments", prcComments);
                     map.put("autolysis", autolysis);
                     map.put("acceptability", acceptability);
                     map.put("acceptabilityCode", acceptabilityCode);
                  
                   
                    String idStr = Integer.toString(id);
                   
                  
                    
			    
                  
			        specimenMap.put(idStr, map);
			    }
			   }catch(Exception e){
			      throw e;
			   }finally{

			       if(stmt != null)
			          stmt.close();

			       if(rs != null)
			          rs.close();

			   }


		return specimenMap;
	}


}
