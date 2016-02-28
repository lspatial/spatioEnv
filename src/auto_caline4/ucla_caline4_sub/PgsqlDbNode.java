package ucla_caline4_sub;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Vector;

class PgsqlDbNode{
	String m_driver ="org.postgresql.Driver";
    String m_url = "jdbc:postgresql://localhost:5432/cs_dis"; 
    String m_user = "postgres"; 
    String m_pwd = "lil"; 
    static Connection m_pcon=null; 
    String m_basicpath="/home/postgres/wkspace/output/dis_ca";

    public PgsqlDbNode(){}

    public Connection getDbCon(){ return m_pcon; }
    
	public void initDBLink(String inURL, String user, String pwd){
	   if(inURL!=null){
		   m_url=inURL;
	   }
	   if(user!=null){
		   m_user =user ;
	   }
	   if(pwd!=null){
		   m_pwd=pwd;
	   }
	   try {
	      Class.forName(m_driver); //trying to load driver
	    }catch (ClassNotFoundException e) {
	        System.err.println("Can't load driver "+ e.getMessage());
	    }
	    try { 
	    	m_pcon = DriverManager.getConnection(m_url, m_user, m_pwd);
	       System.err.println("Conection OK");
	        if (m_pcon != null)
	            System.out.println("Successfully connected to Postgres Database");
	        else
	            System.out.println("We should never get here.");
	    }catch(Exception e) {       
		        System.err.println("Connection Attempt failed");
		        System.err.println(e.getMessage());
	    }
	}
	        
	public void closeDbLink(){
		try{
			m_pcon.close(); 
			m_pcon=null; 
		}catch(Exception e) {
	        System.err.println("Connection Attempt failed");
            System.err.println(e.getMessage());
        }
	}
	
	public void resetDbLink(){
		 try { 
			    if(m_pcon!=null){
			    	m_pcon.close(); 
			    }
		    	m_pcon = DriverManager.getConnection(m_url, m_user, m_pwd);
		       System.err.println("Conection OK");
		        if (m_pcon != null)
		            System.out.println("Successfully connected to Postgres Database");
		        else
		            System.out.println("We should never get here.");
		    }catch(Exception e) {       
			        System.err.println("Connection Attempt failed");
			        System.err.println(e.getMessage());
		    }
	}
}