package brd.es;

import java.sql.*;
import java.util.*;

public class Study {
	
   Connection _con;
	
	public Study(Connection con){
		_con=con;
	}
	
public int getIdFromStudyId(String studyId)  throws Exception{
		
		PreparedStatement stmt=null;
		 ResultSet rs = null;

		  int id = -1;

		String sql="select id from study where id ='" + studyId + "'";
		try{

			   stmt=_con.prepareStatement(sql);
			 
			    rs = stmt.executeQuery();
              
			    while(rs.next()){
			    	 id = rs.getInt("id");
			      
			     

			       
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

       return id;
		
	}

	
	public Map<String, Map> getStudyMap(String studyId)  throws Exception{
		Map<String, List<String>> diagnoses=getDiagnoses(studyId);
		Map<String, List<Map>> platforms=getPlatforms(studyId);
		Map<String, List<String>> biospecimens =getBiospecimenTypes(studyId);
		Map<String, List<String>> preservativeTypes=getPreservativeTypes(studyId);
		Map<String, List<Map>> preAnalyticalFactors= getValues(studyId);
		
		PreparedStatement stmt=null;
		 ResultSet rs = null;
		Map<String, Map> studyMap= new HashMap<String, Map>();

		

		String sql="select stu.id, stu.purpose, stu.summary_of_findings, p.pub_med_id, p.title, p.publication_name, p.publication_year, "+
				" p.curator_purpose, p.curator_conclusion, p.authorslink,  p.suggested_by, ds.is_private from study stu, paper p, document_status ds "+
				" where stu.paper_id=p.id and p.document_status_id=ds.id";
				
		if (studyId.equals("ALL")){
			
		}else{
			sql += " and stu.id ="+studyId+"";
		}
				
	   	
		try{

			   stmt=_con.prepareStatement(sql);
			 
			    rs = stmt.executeQuery();
			    
			
			    

			    while(rs.next()){
			    	
			    	 int id = rs.getInt("id");
			    	 //  System.out.println("problem???");
			         String purpose  = rs.getString("purpose");
			         String summary_of_findings  = rs.getString("summary_of_findings");
			         //System.out.println("case id fine");
			       
			         int pub_med_id = rs.getInt("pub_med_id");
			         //System.out.println("i am good");
			         String title = rs.getString("title");
			         String publication_name = rs.getString("publication_name");
			         //System.out.println("i am not good");
			        // String studyKey = rs.getString("study_key");
			        
			         String publication_year = rs.getString("publication_year");
			         String curator_purpose = rs.getString("curator_purpose");
			         String curator_conclusion = rs.getString("curator_conclusion");
			         
			         String authorslink = rs.getString("authorslink");
			         
			         String suggested_by=rs.getString("suggested_by");
			         int is_private = rs.getInt("is_private");
			         
			      
			         
			         Map  map= new HashMap();
			        // map.put("id",id);
			         map.put("purpose", purpose);
			         
                    
                    map.put("summary_of_findings", summary_of_findings);
                    map.put("pub_med_id", pub_med_id);
                    
			       
			         map.put("title", title);
			         map.put("publication_name", publication_name);
			       
			         int year_int=0;
			         try {
			        	 year_int=Integer.parseInt(publication_year);
			         }catch (NumberFormatException e) {
			        	    
			         }
			         
			         map.put("publication_year", year_int);
			         map.put("curator_purpose", curator_purpose);
			        
			         
			       
			         map.put("curator_conclusion", curator_conclusion);
			         map.put("authorslink", authorslink);
			         map.put("suggested_by", suggested_by);
			         
			        
			         if(is_private == 1)
			              map.put("is_private", true);
			         else
			        	 map.put("is_private", false);
			     
			         
			         
			         String idStr = Integer.toString(id);
			       
			         map.put("biospecimens", biospecimens.get(idStr));
			         map.put("preservativeTypes", preservativeTypes.get(idStr));
			         map.put("diagnoses", diagnoses.get(idStr));
			         map.put("platforms", platforms.get(idStr));
			         map.put("preAnalyticalFactors", preAnalyticalFactors.get(idStr));
			         
			        // List<String> sampleTypes = types.get(idStr);
			       //  map.put("sampleType", sampleTypes);
			         
			       
			         studyMap.put(idStr, map);
			    }
			   }catch(Exception e){
			      throw e;
			   }finally{

			       if(stmt != null)
			          stmt.close();

			       if(rs != null)
			          rs.close();

			   }


		return studyMap;
	}
	

	public Map<String, List<String>> getDiagnoses(String studyId)  throws Exception{
		PreparedStatement stmt=null;
		 ResultSet rs = null;
		 Map<String,List<String>> diagnoses = new HashMap<String, List<String>>();

		String sql=" select std.study_id, d.name as diagnosis, sud.name as sub_diagnosis from study_diagnosis std, diagnosis d, sub_diagnosis sud "+
				" where std.diagnosis_id = d.id and sud.id (+)=std.sub_diagnosis_id";
		if(!studyId.equals("ALL")) {
			sql = sql + " and std.study_id=" + studyId;
		}
		try{

			   stmt=_con.prepareStatement(sql);
			 
			    rs = stmt.executeQuery();

			    while(rs.next()){
			    	 int id = rs.getInt("study_id");
			         String diagnosis = rs.getString("diagnosis");
			    	 String sub_diagnosis = rs.getString("sub_diagnosis");
			    	// System.out.println("sub_diagnosis: " + sub_diagnosis );
			    	 
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
	
	
	public Map<String, List<Map>> getPlatforms(String studyId)  throws Exception{
		PreparedStatement stmt=null;
		 ResultSet rs = null;
		 Map<String,List<Map>> platforms = new HashMap<String, List<Map>>();

		String sql=" select stb.study_id, tp.name as technology_platform, bt.name as analyte from study_platform_biomolecule stb, "+
				" platform_biomolecule_type pb, technology_platform tp, biomolecule_type bt "+
				" where stb.platform_biomolecule_type_id = pb.id and pb.technology_platform_id=tp.id and pb.biomolecul_type_id=bt.id";
		if(!studyId.equals("ALL")) {
			sql = sql + " and stb.study_id=" + studyId;
		}
		try{

			   stmt=_con.prepareStatement(sql);
			 
			    rs = stmt.executeQuery();

			    while(rs.next()){
			    	 int id = rs.getInt("study_id");
			         String technology_platform = rs.getString("technology_platform");
			    	 String analyte = rs.getString("analyte");
			    	// System.out.println("sub_diagnosis: " + sub_diagnosis );
			    	 
                     String idStr = Integer.toString(id);
                     List<Map> platformList = platforms.get(idStr);
                     if(platformList==null){
                    	 platformList = new LinkedList<Map>();
                    	 platforms.put(idStr, platformList);
                     }
                     Map<String, String> element = new HashMap<String, String>();
                     element.put("technology_platform", technology_platform);
                     element.put("analyte", analyte);
                     platformList.add(element);
                    
			     

			       
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


		return platforms;
	}
	
	
	public Map<String, List<String>> getBiospecimenTypes(String studyId)  throws Exception{
		PreparedStatement stmt=null;
		 ResultSet rs = null;
		 Map<String,List<String>> types = new HashMap<String, List<String>>();

		String sql=" select stl.study_id, bt.name as btype, bl.name as blocation  from study_type_location stl, biospecimen_type_location btl, biospecimen_type bt, biospecimen_location bl "+
				" where stl.biospecimen_type_location_id=btl.id and btl.biospecimen_type_id = bt.id and btl.biospecimen_location_id=bl.id";
		if(!studyId.equals("ALL")) {
			sql = sql + " and stl.study_id=" + studyId;
		}
		try{

			   stmt=_con.prepareStatement(sql);
			 
			    rs = stmt.executeQuery();

			    while(rs.next()){
			    	 int id = rs.getInt("study_id");
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
	
	
	
	public Map<String, List<String>> getPreservativeTypes(String studyId)  throws Exception{
		PreparedStatement stmt=null;
		 ResultSet rs = null;
		 Map<String,List<String>> types = new HashMap<String, List<String>>();

		String sql="select spt.study_id, pt.name as preservative_type from study_preservative_type spt, preservative_type pt "
				+ "where spt.preservative_type_id = pt.id";
		if(!studyId.equals("ALL")) {
			sql = sql + " and spt.study_id=" + studyId;
		}
		try{

			   stmt=_con.prepareStatement(sql);
			 
			    rs = stmt.executeQuery();

			    while(rs.next()){
			    	 int id = rs.getInt("study_id");
			         String type = rs.getString("preservative_type");
                     String idStr = Integer.toString(id);
                     List<String> typeList = types.get(idStr);
                     if(typeList==null){
                    	 typeList = new LinkedList<String>();
                    	 types.put(idStr, typeList);
                     }
                     typeList.add(type);
			     

			       
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
	
	
	public Map<String, List<Map>> getValues(String studyId)  throws Exception{
		PreparedStatement stmt=null;
		 ResultSet rs = null;
		 Map<String,List<Map>> values = new HashMap<String, List<Map>>();
		 
		 String sql="";
        if(studyId.equals("ALL") ){
		
		
		 sql="select  sef.study_id, efc.name as classification, ef.name as pre_analytical_factor,  val.factor_value from study_experimental_factor sef, \r\n"
				+ "experimental_factor ef, experimental_fctr_class efc, experiment_fctr_perm_val val, study_exprmntl_fctr_value sval\r\n"
				+ "where sef.experimental_factor_id=ef.id and ef.experimental_factors_class_id=efc.id\r\n"
				+ " and sval.experimental_factor_per_val_id=val.id and sval.study_experimental_factor_id =sef.id\r\n"
				+ "and val.experimental_factor_id=ef.id and efc.name <> 'Platform-specific Methodology'\r\n"
				+ "union\r\n"
				+ "select sef.study_id, efc.name as classification, ef.name as pre_analytical_factor,   sval.factor_value  from study_experimental_factor sef, \r\n"
				+ "experimental_factor ef, experimental_fctr_class efc,  study_exprmntl_fctr_value sval\r\n"
				+ "where sef.experimental_factor_id=ef.id and ef.experimental_factors_class_id=efc.id\r\n"
				+ " and sval.study_experimental_factor_id =sef.id and factor_value is not null and efc.name <> 'Platform-specific Methodology'\r\n"
				+ "union\r\n"
				+ "select sef.study_id, tp.name || ' Specific' as classification, ef.name as pre_analytical_factor, sval.factor_value from study_experimental_factor sef, experimental_factor ef, experimental_fctr_class efc, technology_platform tp,\r\n"
				+ " platform_experimental_factor pef, study_exprmntl_fctr_value sval\r\n"
				+ "where sef.experimental_factor_id = ef.id and ef.experimental_factors_class_id = efc.id and efc.name= 'Platform-specific Methodology' \r\n"
				+ "and tp.id = sef.technology_platform_id and tp.id = pef.technology_platform_id and pef.experimental_factor_id=ef.id\r\n"
				+ "and sef.id = sval.study_experimental_factor_id and sval.factor_value is not null\r\n"
				+ "union\r\n"
				+ "select sef.study_id, tp.name || ' Specific' as classification, ef.name as pre_analytical_factor, val.factor_value from study_experimental_factor sef, experimental_factor ef, experimental_fctr_class efc, technology_platform tp,\r\n"
				+ " platform_experimental_factor pef, study_exprmntl_fctr_value sval, experiment_fctr_perm_val val\r\n"
				+ "where sef.experimental_factor_id = ef.id and ef.experimental_factors_class_id = efc.id and efc.name= 'Platform-specific Methodology' \r\n"
				+ "and tp.id = sef.technology_platform_id and tp.id = pef.technology_platform_id and pef.experimental_factor_id=ef.id\r\n"
				+ "and sef.id = sval.study_experimental_factor_id and val.experimental_factor_id=ef.id and val.id = sval.experimental_factor_per_val_id";
        }else {
        	 sql="select  sef.study_id, efc.name as classification, ef.name as pre_analytical_factor,  val.factor_value from study_experimental_factor sef, \r\n"
     				+ "experimental_factor ef, experimental_fctr_class efc, experiment_fctr_perm_val val, study_exprmntl_fctr_value sval\r\n"
     				+ "where sef.experimental_factor_id=ef.id and ef.experimental_factors_class_id=efc.id\r\n"
     				+ " and sval.experimental_factor_per_val_id=val.id and sval.study_experimental_factor_id =sef.id\r\n"
     				+ "and val.experimental_factor_id=ef.id and efc.name <> 'Platform-specific Methodology' and sef.study_id=" + studyId + "\r\n"
     				+ "union\r\n"
     				+ "select sef.study_id, efc.name as classification, ef.name as pre_analytical_factor,   sval.factor_value  from study_experimental_factor sef, \r\n"
     				+ "experimental_factor ef, experimental_fctr_class efc,  study_exprmntl_fctr_value sval\r\n"
     				+ "where sef.experimental_factor_id=ef.id and ef.experimental_factors_class_id=efc.id\r\n"
     				+ " and sval.study_experimental_factor_id =sef.id and factor_value is not null and efc.name <> 'Platform-specific Methodology' and sef.study_id=" + studyId + "\r\n"
     				+ "union\r\n"
     				+ "select sef.study_id, tp.name || ' Specific' as classification, ef.name as pre_analytical_factor, sval.factor_value from study_experimental_factor sef, experimental_factor ef, experimental_fctr_class efc, technology_platform tp,\r\n"
     				+ " platform_experimental_factor pef, study_exprmntl_fctr_value sval\r\n"
     				+ "where sef.experimental_factor_id = ef.id and ef.experimental_factors_class_id = efc.id and efc.name= 'Platform-specific Methodology' \r\n"
     				+ "and tp.id = sef.technology_platform_id and tp.id = pef.technology_platform_id and pef.experimental_factor_id=ef.id\r\n"
     				+ "and sef.id = sval.study_experimental_factor_id and sval.factor_value is not null  and sef.study_id=" + studyId + "\r\n"
     				+ "union\r\n"
     				+ "select sef.study_id, tp.name || ' Specific' as classification, ef.name as pre_analytical_factor, val.factor_value from study_experimental_factor sef, experimental_factor ef, experimental_fctr_class efc, technology_platform tp,\r\n"
     				+ " platform_experimental_factor pef, study_exprmntl_fctr_value sval, experiment_fctr_perm_val val\r\n"
     				+ "where sef.experimental_factor_id = ef.id and ef.experimental_factors_class_id = efc.id and efc.name= 'Platform-specific Methodology' \r\n"
     				+ "and tp.id = sef.technology_platform_id and tp.id = pef.technology_platform_id and pef.experimental_factor_id=ef.id\r\n"
     				+ "and sef.id = sval.study_experimental_factor_id and val.experimental_factor_id=ef.id and val.id = sval.experimental_factor_per_val_id and sef.study_id=" + studyId;
        }
		try{

			   stmt=_con.prepareStatement(sql);
			 
			    rs = stmt.executeQuery();

			    while(rs.next()){
			    	 int id = rs.getInt("study_id");
			         String classification = rs.getString("classification");
			         String pre_analytical_factor = rs.getString("pre_analytical_factor");
			         String factor_value= rs.getString("factor_value");
                     String idStr = Integer.toString(id);
                     List<Map> valueList = values.get(idStr);
                     if(valueList==null){
                    	 valueList = new LinkedList<Map>();
                    	 values.put(idStr, valueList);
                     }
                     
                     boolean found=false;
                     for(int i = 0; i<valueList.size(); i++) {
                    	 Map element = valueList.get(i);
                    	 String ec = (String) element.get("classification");
                    	 String ef = (String) element.get("pre_analytical_factor");
                    	 if(classification.equals(ec) && pre_analytical_factor.equals(ef)) {
                    		// System.out.println("found");
                    		 List list =(List)element.get("values");
                             list.add(factor_value);
                    		 found=true;
                    	 }
                     }
                     if(!found) {
                    	// System.out.println("not found");
                    	 Map element = new HashMap();
                    	 element.put("classification", classification);
                    	 element.put("pre_analytical_factor", pre_analytical_factor);
                    	 List<String> list = new LinkedList<String>();
                    	 list.add(factor_value);
                    	 element.put("values", list);
                    	 valueList.add(element);
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


		return values;
	}
	
	
}
