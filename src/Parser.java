package com.ef;

import com.ef.LogDAO;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;
import java.util.Set;

public class Parser {
	
	
	private static LocalDateTime startdate;
	
	private static LocalDateTime enddate;
	
	private static String duration;
	
	private static int threshold;
	
	private static HashMap<String,ArrayList<logrecord>> logmap; 
	
	private static HashMap<String,Long> matchedip ; 
	
	private static String URL;
	
	private static String username;
	
	private static String password;
	
	private static LogDAO db;
	
	private static int linecount = 0;
	
	
	

	
	
	public static class logrecord
	{
		/*
		 * This class is used to store records mapping to each IP.
		 * For each IP, a corresponding ArrayList of logrecord is stored
		 * in the HashMap.
		 * 
		 */
		
	private LocalDateTime dt;
	
	private String request;
	
	private int status;
	
	private String info;
	
	public LocalDateTime getDt() {
		return dt;
	}

	public void setDt(LocalDateTime dt) {
		this.dt = dt;
	}

	public String getRequest() {
		return request;
	}

	public void setRequest(String request) {
		this.request = request;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public String getInfo() {
		return info;
	}

	public void setInfo(String info) {
		this.info = info;
	}
	
		
	}
	
	
	/*
	 	
	public static String[] splitarguments(String[] temp)
	{
		
		
		//  This function splits the command line arguments passed and returns their corresponding values. 
		 
		   
		String[] ret = new String[4];
		String[] keyvalues;
		
		for(int i = 0; i<4; i++)
		{
			keyvalues = temp[i].split("=");
			
			if(keyvalues[0].equals("--accesslog"))
			{
					ret[0] = keyvalues[1]; 
			}
			else if(keyvalues[0].equals("--startDate"))
			{
					ret[1] = keyvalues[1]; 
			}
			else if(keyvalues[0].equals("--duration"))
			{
					ret[2] = keyvalues[1]; 
			}
			else
			{
					ret[3] = keyvalues[1]; 
			}
			
		}
		
		
		
		
		return ret;
	}
	 
	 
	 */

	
	
	public static LocalDateTime setDate(String s)
	{
	  /*
	   * This function sets the date in each log to a LocalDateTime Object.
	   */
		
	String[] dt = s.split(" ");
    
	String date = dt[0];
	String time = dt[1];
	
	String[] ymd = date.split("-");
	String[] hms = time.split(":");
	
	LocalDateTime datetime = LocalDateTime.of(Integer.parseInt(ymd[0]),Integer.parseInt(ymd[1]),Integer.parseInt(ymd[2]),Integer.parseInt(hms[0]),Integer.parseInt(hms[1]),Integer.parseInt(hms[2].substring(0,2)),Integer.parseInt(hms[2].substring(3,hms[2].length())+"000000" ));
	
	return datetime;
			
	}
	
	
	private static void addrecord(String log) {
		
		/*
		 * This function takes each line from the file and stores the log
		 * to the HashMap
		 */
	
	logrecord record = new logrecord();
	
	String[] arr = log.split("\\|");

	record.setDt(setDate(arr[0]));
	record.setRequest(arr[2]);
	record.setStatus(Integer.parseInt(arr[3]));
	record.setInfo(arr[4]);
	
	
	if(logmap.containsKey(arr[1]))
	{
		
		logmap.get(arr[1]).add(record);
		
	}
	
	else
	{
		ArrayList<logrecord> t = new ArrayList<logrecord>();
		t.add(record);
		logmap.put(arr[1],t);
		
	}
		
		
	}
	
	
	private static LocalDateTime setstartDate(String dt) {
		
		/*
		 * This function sets the StartDate as given in the CommandLine argument.
		 */
	
		LocalDateTime ldt = null;
		String[] datetime = dt.split("\\.");
		
		String[] ymd = datetime[0].split("-");
		String[] hms = datetime[1].split(":");
		
		ldt = LocalDateTime.of(Integer.parseInt(ymd[0]),Integer.parseInt(ymd[1]),Integer.parseInt(ymd[2]),Integer.parseInt(hms[0]),Integer.parseInt(hms[1]),Integer.parseInt(hms[2])); 
			
		return ldt;
	}
	
	
	private static void readDatafromFile(String[] param) {
		
		/*
		 * This function opens the file, adds the record to HashMap,
		 * sets the startDate, EndDate required for processing the logs.
		 */
		
		//String[] param = splitarguments(parameters);
		
		try{
			
			File filepath = new File(param[0]);
			startdate = setstartDate(param[1]);
			duration = param[2];
			threshold = Integer.parseInt(param[3]);
			
		
			if(filepath.isFile())
			{
               
			   logmap = new HashMap<String,ArrayList<logrecord>>();
			   String log;
			   BufferedReader br = new BufferedReader(new FileReader(filepath));
			    
			   System.out.println("Start reading from file: "+LocalDateTime.now().toString());
				
			   while((log = br.readLine()) != null)
				{
			        
				    linecount++;
					addrecord(log);
					
				}
			   br.close();
			   
			}
			
			else
			{
				System.out.println("File doesn't exist");
				System.exit(0);
				
			}
				
			System.out.println("End of reading from file: "+LocalDateTime.now().toString());
			System.out.println("No of logs: "+linecount);
				
				 
				if(duration.toLowerCase().equals("hourly"))
				{
					
					enddate = startdate.plusHours(1);
							
				}
				
				if(duration.toLowerCase().equals("daily")) 
				{
					enddate = startdate.plusDays(1);
					
				}
					
			}
			
		    catch (NullPointerException nullp) {
			
		    	System.out.println("Enter file name");
		    	System.exit(1);
		    
		    } 
			catch (FileNotFoundException e) {
				
				System.out.println("Enter existing file name");
				System.exit(0);
			    
			} 
			catch (IOException e) {
				// TODO Auto-generated catch block
				System.out.println("An IO Exception Occured");
				System.exit(1);
				
				//e.printStackTrace();
			}
			
			
	}
	
	
	
	private static void storeDatatoDB() {
		
		/*
		 * This function calls the corresponding method of DAO to store
		 * given logs to DataBase.
		 */
		
		
		if(logmap.isEmpty())
		{
			System.out.println("No data found in the file");
			System.exit(0);
		}	
		
		else
		{
			
			if(db.insertData(logmap))
				System.out.println("Insertion to DataBase Successfull");
			
			else
			{
				System.out.println("Insertion to DataBase UnSuccessfull");
				System.exit(1);
			}	

	    }
		
   
	}

	
	
	private static void findIPs() {
		
		/*
		 *  This function selects the IP's based on the given StartDate and Duration
		 */
		
			if(logmap.isEmpty())
			{
				System.out.println("No data found in the file");
				System.exit(0);
			}

			else
			{
				matchedip = new HashMap<String,Long>();
				Set<String> keys = logmap.keySet();
				ArrayList<logrecord> temp = new ArrayList<logrecord>();
				long count;
				
				System.out.println("Start finding IP's: "+LocalDateTime.now().toString());
				for(String i : keys)
				{
				
					temp = logmap.get(i);
					
					if( (count = temp.stream().map(m -> m.getDt()).filter(l -> {return  (l.isAfter(startdate) || l.isEqual(startdate)) && (l.isBefore(enddate) || l.isEqual(enddate));} ).count()) >= threshold)
					{	
						matchedip.put(i,count);
					//	System.out.println("IP: "+i+" made "+count+" requests");
					    
					}
				}
				System.out.println("End of finding IP's: "+LocalDateTime.now().toString());
				
				if(!matchedip.isEmpty())
				{
					//System.out.println("No IP's found matching the given criteria");
					
					for(String ip:matchedip.keySet())
					{
						System.out.println("/-------------  IP: "+ip+" made "+matchedip.get(ip)+" Requests ");
					}
				}	
				
			}
	}
	
	
	
	private static void storeIPstoDB() {
		
		/*
		 *  This function calls the corresponding DAO object's method to store
		 *  IP's satisfying the condition to DataBase.
		 */
		
		if(matchedip.isEmpty())
		System.out.println("No IPs found matching the given condition");
		
		else
		{
			
			if(db.insertIPs(matchedip))
				System.out.println("Matched IPs loaded into DataBase Successfully");
			
			else
			{
				System.out.println("Loading Matched IPs UnSuccessfull");
				System.exit(1);
			}	

	    }
			
		
		
	}
	
	

	/*
	public static void main(String[] args)
	{
		method(new String[]{"--accesslog=C:/Users/kgopu/Desktop/test/access.log","--startDate=2017-01-01.15:00:00", "--duration=hourly", "--threshold=200"});
	}
	
	*/
	
	public static void main(String[] execParameters)
	
	{
		/*
		 *  This is the entry point of the application. It does all the processing
		 *  by calling other methods in the class.
		 */
		
		if(execParameters.length != 4)
		{
			System.out.println("Enter required Parameters for execution");
			return;
			
		}
		
		else
		{	
		    
			System.out.println("Do You want to Store Data to your MySQl DataBase ?");
			System.out.println("Press 'Y' or 'N' ");
			Scanner s = new Scanner(System.in);
			String line;
			
			while(!( (line = s.nextLine()).toLowerCase().equals("y") || line.toLowerCase().equals("n")))
				System.out.println("Press 'Y' or 'N' ");	
			
		
			
			switch(line.toLowerCase())
			{
			 
			case "y": System.out.println("Enter DataBase URL. For MySQL - jdbc:mysql://localhost:3306 ");
					  URL = s.nextLine().trim();
					  System.out.println("Enter Username");
					  username = s.nextLine().trim();
					  System.out.println("Enter Password");
					  password = s.nextLine().trim();
					  readDatafromFile(execParameters);
					  db = new LogDAO(URL,username,password);
					  storeDatatoDB();
					  findIPs();
					  storeIPstoDB();
					  break;
					  
			
			case "n": readDatafromFile(execParameters);
			          findIPs();
			          if(matchedip.isEmpty())
			        	  System.out.println("No IPs found matching the given condition");
			          break;
			
			}
			
			s.close();
			
		}
		
	}
	


	}



	/**
	 * Serial Parsing 
	 * 
	 Start reading from file: 2017-11-04T11:50:35.563
End of reading from file: 2017-11-04T11:50:36.142
Start finding IP's: 2017-11-04T11:50:36.143
End of finding IP's: 2017-11-04T11:50:36.183
	 */
	
	/*
	 *Parallel stream (300 MB) 
	 * Start reading from file: 2017-11-04T12:27:31.162
End of reading from file: 2017-11-04T12:27:41.978
Start finding IP's: 2017-11-04T12:27:41.978
End of finding IP's: 2017-11-04T12:27:42.178

	 */
	
	/*
	 * 
	 * Serial Stream (300 MB)
	Start reading from file: 2017-11-04T12:36:22.815
	End of reading from file: 2017-11-04T12:36:34.159
	Start finding IP's: 2017-11-04T12:36:34.160
	End of finding IP's: 2017-11-04T12:36:34.334
	
	*/

	




	
