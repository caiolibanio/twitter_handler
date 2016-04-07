//package twitter_handler;
//import java.sql.DriverManager;
//import java.sql.ResultSet;
//import java.io.FileNotFoundException;
//import java.io.PrintWriter;
//import java.io.UnsupportedEncodingException;
//import java.math.BigInteger;
//import java.sql.Connection;
//import java.sql.SQLException;
//import java.sql.Statement;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.Map;
//
//import org.json.JSONArray;
//import org.json.JSONException;
//import org.json.JSONObject;
//
//public class Main {
//	
//	static Connection connection = null;
//	
//	static PrintWriter writer = null;
//
//	public static void main(String[] argv) {
//
//		System.out.println("-------- PostgreSQL "
//				+ "JDBC Connection Testing ------------");
//
//		try {
//
//			Class.forName("org.postgresql.Driver");
//
//		} catch (ClassNotFoundException e) {
//
//			System.out.println("Where is your PostgreSQL JDBC Driver? "
//					+ "Include in your library path!");
//			e.printStackTrace();
//			return;
//
//		}
//
//		System.out.println("PostgreSQL JDBC Driver Registered!");
//			
//			
//			try {
//				filteringRows();
//				connection.close();
//			} catch (SQLException e) {
//				e.printStackTrace();
//				System.err.println("Erro fatal!");
//			}
//		}
//	
//
//	private static void filteringRows() throws SQLException {
//		
//		Statement st = null;
//		ResultSet rs = null;
//		int countOK = 0;
//		int countFail = 0;
//		int countStep = 1000000;
//		boolean lastLine = false;
//		int step1 = 1;
//		int step2 = 1000;
//		
//		while(!lastLine){
//			try 
//		    {
//				connection = DriverManager.getConnection(
//				"jdbc:postgresql://127.0.0.1:5432/tweets", "postgres",
//				"admin");	
//				connection.setAutoCommit(false);
//		      st = connection.createStatement();
//		      rs = st.executeQuery("select * from geo_tweets where tid between " + step1 + " and " + step2 + " order by tid");
//		      step1 = step1 + 1000;
//		      step2 = step2 + 1000;
//		      while ( rs.next() )
//		      {
//		        
//		        Long tid = rs.getLong("tid");
//		        String json   = rs.getString("json");
//		        Tweet tweet = new Tweet(tid, json);
//		        if(tid.longValue() == 31185709){
//		        	lastLine = true;
//		        }
//		        String[] coords = extractCoords(tweet);
//		        Long user_id = extractUserId(tweet);
//		        String text = extractMessage(tweet);
//		        tweet.setLongitude(Double.parseDouble(coords[0]));
//		        tweet.setLatitude(Double.parseDouble(coords[0]));
//		        tweet.setUser_id(user_id);
//		        tweet.setText(text);
//
//		        
////		        insert(tweet);
//		        countOK++;
//		        	
//		        
//		      }
//		      
//		    }
//		    catch (SQLException se) {
//		      System.err.println("Erro em uma linha... Continuando com a proxima.");
//		      System.err.println(se.getMessage());
//		      countFail++;
//		    }finally{
//		    	rs.close();
//			    st.close();
//			    connection.commit();
//		      	connection.close();
//		      	
//		      	if(countStep == countOK){
//		      		countStep = countStep + 1000000;
//		      		System.out.println("CountOK esta em: " + countOK + " -- CountFAIL esta em: " + countFail);
//		      		
//		      	}
//		      	
//		    }
//			
//		}
//		System.out.println("Total aceito:" + countOK + "---- Total de erros: " + countFail);
//	}
//
//	private static String[] extractCoords(Tweet tweet) {
//		String[] out = new String[2];
//		try {
//			JSONArray jsonArray = new JSONArray("[" + tweet.getJson() + "]");
//			int count = jsonArray.getJSONObject(0).length(); // get totalCount of all jsonObjects
//			JSONObject jsonObject = jsonArray.getJSONObject(0).getJSONObject("coordinates");  // get jsonObject @ i position 
//			String coords = jsonObject.get("coordinates").toString();
//			String[] coordsFormated = coords.replace("[", "").replace("]", "").replace(",", " ").split(" ");
//			String longitude = coordsFormated[0];
//			String latitude = coordsFormated[1];
//			out[0] = longitude;
//			out[1] = latitude;
//			
//		} catch (JSONException e) {
//			e.printStackTrace();
//		}
//		return out;
//	}
//	
//	private static Long extractUserId(Tweet tweet){
//		Long user_id = null;
//		try {
//			JSONArray jsonArray = new JSONArray("[" + tweet.getJson() + "]");
//			JSONObject jsonObject = jsonArray.getJSONObject(0).getJSONObject("user");  // get jsonObject @ i position 
//			String id_str = jsonObject.get("id_str").toString();
//			user_id = new Long(id_str);
//			
//		} catch (JSONException e) {
//			e.printStackTrace();
//		}
//		return user_id;
//	}
//	
//	private static String extractMessage(Tweet tweet){
//		String text = null;
//		try {
//			JSONArray jsonArray = new JSONArray("[" + tweet.getJson() + "]");
//			text = jsonArray.getJSONObject(0).getString("text");  // get jsonObject @ i position 
//			
//		} catch (JSONException e) {
//			e.printStackTrace();
//		}
//		return text;
//	}
//	
//	public static void insert(Tweet tweet) throws SQLException{
//		
//		Statement stmt = connection.createStatement();
//		String sql = "INSERT INTO geo_tweets" + "(" + "tid" + ", " + "json" + ", " + "user_id" + ", " + "longitude" + ", " + 
//		"latitude" + ", " + "text" + ") "
//	               + "VALUES (" + "'" + tweet.getTid() + "'" + ", " +"$token$" + tweet.getJson() + "$token$" + ", "
//		+ "'" + tweet.getUser_id() + "'" + ", " + "'" + tweet.getLongitude() + "'" + ", " + "'" + tweet.getLatitude() + 
//		"'" + ", " + "$token$" + tweet.getText() + "$token$" + ");";
//		
//		stmt.executeUpdate(sql);
//		stmt.close();
//
//		
//	}
//	
//	public static Map<String,String> parse(JSONObject json , Map<String,String> out) throws JSONException{
//	    Iterator<String> keys = json.keys();
//	    while(keys.hasNext()){
//	        String key = keys.next();
//	        String val = null;
//	        try{
//	             JSONObject value = json.getJSONObject(key);
//	             parse(value,out);
//	        }catch(Exception e){
//	            val = json.getString(key);
//	        }
//
//	        if(val != null){
//	            out.put(key,val);
//	        }
//	    }
//	    return out;
//	}
//	
//		
//}
//
