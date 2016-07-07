package twitter_handler;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class SelectGeoRefTweets {
	
	static Connection connection = null;
	
	static PrintWriter writer = null;

	public static void main(String[] argv) {

		System.out.println("-------- PostgreSQL "
				+ "JDBC Connection Testing ------------");

		try {

			Class.forName("org.postgresql.Driver");

		} catch (ClassNotFoundException e) {

			System.out.println("Where is your PostgreSQL JDBC Driver? "
					+ "Include in your library path!");
			e.printStackTrace();
			return;

		}

		System.out.println("PostgreSQL JDBC Driver Registered!");
			
			
			try {
				filteringRows();
				connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
				System.err.println("Erro fatal!");
			}
		}
	

	private static void filteringRows() throws SQLException {
		
		Statement st = null;
		ResultSet rs = null;
		int countOK = 0;
		int countFail = 0;
		boolean lastLine = false;
		int step1 = 1;
		int step2 = 1000;
		
		while(!lastLine){
			try 
		    {
				connection = DriverManager.getConnection(
				"jdbc:postgresql://127.0.0.1:5432/tweets", "postgres",
				"admin");	
				connection.setAutoCommit(false);
		      st = connection.createStatement();
		      rs = st.executeQuery("select * from tweets_london_raw_original where tid between " + step1 + " and " + step2 + " order by tid");
		      step1 = step1 + 1000;
		      step2 = step2 + 1000;
		      while ( rs.next() )
		      {
		        
		        Long tid = rs.getLong("tid");
		        String json   = rs.getString("json");
		        Tweet tweet = new Tweet(tid, json);
		        if(tid.longValue() == 31185709){
		        	lastLine = true;
		        }
		        boolean haveCoords = checkTweet(tweet);
		        
		        
		       
		        

		        if(haveCoords){
		        	String[] coords = extractCoords(tweet);
			        Long user_id = extractUserId(tweet);
			        String text = extractMessage(tweet);
			        Timestamp date = getTwitterDate(tweet);
			        tweet.setLongitude(Double.parseDouble(coords[0]));
			        tweet.setLatitude(Double.parseDouble(coords[1]));
			        tweet.setUser_id(user_id);
			        tweet.setMessage(text);
			        tweet.setDate(date);
			        boolean insideGeom = isInsideGeom(tweet);
			        if(insideGeom){
			        	insert(tweet);
			        	countOK++;
			        }
		        	
		        }
		      }
		      
		    }
		    catch (SQLException se) {
		      System.err.println("Erro em uma linha... Continuando com a proxima.");
		      System.err.println(se.getMessage());
		      countFail++;
		    }finally{
		    	rs.close();
			    st.close();
			    connection.commit();
		      	connection.close();
		      	
		    }
			
		}
		System.out.println("Total aceito:" + countOK + "---- Total de erros: " + countFail);
	}

	private static boolean isInsideGeom(Tweet tweet) throws SQLException {
		Statement stmt = connection.createStatement();
		boolean result = false;
		String sql = "SELECT code FROM social_data WHERE st_contains(social_data.geom, ST_SetSRID(ST_MakePoint("
				+ tweet.getLongitude() + ", " + tweet.getLatitude() + "), 4326)) = TRUE";
		ResultSet rs = stmt.executeQuery(sql);
		if (rs.next()) {
			result = true;
		}
		stmt.close();
		rs.close();
		return result;
	}


	private static boolean checkTweet(Tweet tweet) {
		try {
			JSONArray jsonArray = new JSONArray("[" + tweet.getJson() + "]");
 
			int count = jsonArray.getJSONObject(0).length(); // get totalCount of all jsonObjects
			JSONObject jsonObject = jsonArray.getJSONObject(0).getJSONObject("coordinates");  // get jsonObject @ i position 
			return true;
		} catch (JSONException e) {
//			System.out.println("Esta linha nao possui coordenadas geograficas!");
		}
		return false;
	}
	
	private static String[] extractCoords(Tweet tweet) {
		String[] out = new String[2];
		try {
			JSONArray jsonArray = new JSONArray("[" + tweet.getJson() + "]");
			JSONObject jsonObject = jsonArray.getJSONObject(0).getJSONObject("coordinates");  // get jsonObject @ i position 
			String coords = jsonObject.get("coordinates").toString();
			String[] coordsFormated = coords.replace("[", "").replace("]", "").replace(",", " ").split(" ");
			String longitude = coordsFormated[0];
			String latitude = coordsFormated[1];
			out[0] = longitude;
			out[1] = latitude;
			
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return out;
	}
	
	private static Long extractUserId(Tweet tweet){
		Long user_id = null;
		try {
			JSONArray jsonArray = new JSONArray("[" + tweet.getJson() + "]");
			JSONObject jsonObject = jsonArray.getJSONObject(0).getJSONObject("user");  // get jsonObject @ i position 
			String id_str = jsonObject.get("id_str").toString();
			user_id = new Long(id_str);
			
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return user_id;
	}
	
	private static String extractMessage(Tweet tweet){
		String text = null;
		try {
			JSONArray jsonArray = new JSONArray("[" + tweet.getJson() + "]");
			text = jsonArray.getJSONObject(0).getString("text");  // get jsonObject @ i position 
			
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return text;
	}
	
	public static Timestamp getTwitterDate(Tweet tweet) {
		String date = null;
		final String TWITTER = "EEE MMM dd HH:mm:ss ZZZZZ yyyy";
		
		TimeZone tz = TimeZone.getTimeZone("UK");
		TimeZone.setDefault(tz);
		
		
		SimpleDateFormat sf = new SimpleDateFormat(TWITTER, Locale.UK);
//		sf.setTimeZone(TimeZone.getTimeZone("UTC"));
		Date dateObj = null;
		Timestamp timeStamp = null;

		try {
			JSONArray jsonArray = new JSONArray("[" + tweet.getJson() + "]");
			date = jsonArray.getJSONObject(0).getString("created_at");
			
			// SimpleDateFormat sf = new SimpleDateFormat(TWITTER);
			sf.setLenient(true);
			dateObj = sf.parse(date);
			
			
			timeStamp = new java.sql.Timestamp((dateObj).getTime());

		} catch (JSONException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return timeStamp;
	}

	public static void insert(Tweet tweet) throws SQLException{
		
		Statement stmt = connection.createStatement();
		String sql = "INSERT INTO geo_tweets" + "(" + "tid" + ", " + "json" + ", " + "user_id" + ", " + "longitude" + ", " + 
				"latitude" + ", " + "message" + ", " + "date" + ") "
			               + "VALUES (" + "'" + tweet.getTid() + "'" + ", " +"$token$" + tweet.getJson() + "$token$" + ", "
				+ "'" + tweet.getUser_id() + "'" + ", " + "'" + tweet.getLongitude() + "'" + ", " + "'" + tweet.getLatitude() + 
				"'" + ", " + "$token$" + tweet.getMessage() + "$token$" + ", " + "'" + tweet.getDate() + "'" + ");";
		
		stmt.executeUpdate(sql);
		stmt.close();

		
	}
	
		
}

