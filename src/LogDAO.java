package com.ef;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import com.ef.Parser.logrecord;

public class LogDAO {
	
	 private String username;
	 private String password;
	 private String URL;
	 
	 
	public LogDAO(String address, String user, String pass)
	
	{
		/*
		 * Constructor for initializing URL, username and password
		 * required to access the MYSQL Server
		 */
		
		this.URL = address+"?useSSL=true";
		this.username = user;
		this.password = pass;
	}
	
/*	
	private byte[] getBytes(String ip) throws UnknownHostException
	{
		
		
		InetAddress address = InetAddress.getByName(ip);
		byte[] arr = address.getAddress();
		return arr;
		
		    
			
	}

*/
	
	public boolean insertData(HashMap<String,ArrayList<logrecord>> logs) {
		
		/*
		 *  Stores the Log Data to DataBase
		 */
		
		try (Connection con = DriverManager.getConnection(URL,username,password)) 
		{	
		    Set<String> keys = logs.keySet();
		    ArrayList<logrecord> records = new ArrayList<logrecord>();
		    int batchcount = 0;
		    
			Statement st = con.createStatement();
			st.executeUpdate("create database logs");
			st.executeUpdate("use logs");
			st.executeUpdate("create table LogRecords( logtime datetime(3),ip varbinary(16),request varchar(30),statusvalue smallint,useragent varchar(500),primary key(ip,logtime))");
		    st.close();
		    
		    
			PreparedStatement ps = con.prepareStatement("insert into LogRecords (logtime,ip,request,statusvalue,useragent) values(?,INET6_ATON(?),?,?,?)");
			
			con.setAutoCommit(false);
			
			System.out.println("Inserting into Database:"+LocalDateTime.now());
			
			for(String key:keys)
			{
				records = logs.get(key);
				
				for(logrecord t:records)
				{
					if(batchcount < 1000)
					{
						ps.setTimestamp(1,Timestamp.valueOf(t.getDt()));
						ps.setString(2,key);
						ps.setString(3,t.getRequest());
						ps.setInt(4,t.getStatus());
						ps.setString(5,t.getInfo());
						
						ps.addBatch();
						batchcount++;
							
					}
					
					else
					{
						ps.executeBatch();
						con.commit();
						batchcount = 0;
						
						ps.setTimestamp(1,Timestamp.valueOf(t.getDt()));
						ps.setString(2,key);
						ps.setString(3,t.getRequest());
						ps.setInt(4,t.getStatus());
						ps.setString(5,t.getInfo());
						
						ps.addBatch();
						batchcount++;
						
						
												
					}
					
					
				}	
				
			}
			
			if(batchcount > 0)
			{
				
				ps.executeBatch();
				con.commit();
			}	
			
			ps.close();
			
			System.out.println("Finished Inserting into Database:"+LocalDateTime.now());
			
		
		} 
		
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		} 
		
		/*
		catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		
		*/
	
		return true;
	}
	

	
	public boolean insertIPs(HashMap<String,Long> logs) {
		
		/*
		 * Stores the IP's satisfying the given criteria into the DataBase 
		 */
		
		try (Connection con = DriverManager.getConnection(URL,username,password)) 
		{	
		
			    Set<String> keys = logs.keySet();
			    int batchcount = 0;
				
			    Statement st = con.createStatement();
				st.executeUpdate("use logs");
				st.executeUpdate("create table IPLOG( ip varbinary(16), reason varchar(35), primary key(ip))");
				st.close();
				
				PreparedStatement ps = con.prepareStatement("insert into IPLOG(ip,reason) values(INET6_ATON(?),?)");

				con.setAutoCommit(false);
				
				System.out.println("Inserting IPS into Database:"+LocalDateTime.now());
				for(String key:keys)
				{
					
					if(batchcount < 10)
					{
						ps.setString(1,key);
						ps.setString(2,"This IP made "+logs.get(key)+" requests");
						ps.addBatch();
						batchcount++;
						
					}
					
					else
					{
						ps.executeBatch();
						con.commit();
						batchcount = 0;
						
						ps.setString(1,key);
						ps.setString(2,"This IP made "+logs.get(key)+" requests");
						ps.addBatch();
						batchcount++;
						
					}
					
				}
				
				if(batchcount > 0)
				{	
					ps.executeBatch();
					con.commit();
				}
				
				ps.close();
			    System.out.println("Finished Inserting IPS into Database:"+LocalDateTime.now());
				
		
		} 
		
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		} 
		/*
		catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	*/
		return true;
	}

	
	
}