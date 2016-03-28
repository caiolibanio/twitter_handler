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

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class JDBCExample {
	
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
		int countOK = 89778;
		int countFail = 0;
		int countStep = 1000000;
		
		for(int i = 89778; i < 19456798; i = i + 1000){
			try 
		    {
				connection = DriverManager.getConnection(
				"jdbc:postgresql://127.0.0.1:5432/tweets", "postgres",
				"admin");	
				connection.setAutoCommit(false);
		      st = connection.createStatement();
		      rs = st.executeQuery("SELECT tid, json FROM tweets_london_raw_original ORDER BY tid LIMIT 1000 OFFSET " + i);
		      while ( rs.next() )
		      {
		        
		        Long tid = rs.getLong("tid");
		        String json   = rs.getString("json");
		        Tweet tweet = new Tweet(tid, json);
		        boolean key = checkTweet(tweet);
		        
		        if(key){
		        	insert(tweet);
		        	countOK++;
		        	
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
		      	
		      	if(countStep == countOK){
		      		countStep = countStep + 1000000;
		      		System.out.println("CountOK esta em: " + countOK);
		      		
		      	}
		      	
		    }
			
		}
		System.out.println("Total aceito:" + countOK + "---- Total de erros: " + countFail);
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
	
	public static void insert(Tweet tweet) throws SQLException{
		
		Statement stmt = connection.createStatement();
		String sql = "INSERT INTO geo_tweets" + "(" + "tid" + ", " + "json" + ") "
	               + "VALUES (" + "'" + tweet.getTid() + "'" + ", " +"$token$" + tweet.getJson() + "$token$" + ");";
		
		stmt.executeUpdate(sql);
		stmt.close();

		
	}
	
	public static void openFile() throws FileNotFoundException, UnsupportedEncodingException{
		writer = new PrintWriter("the-file-name.txt", "UTF-8");
	}
		
}

