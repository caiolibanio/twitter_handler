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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class UsersExtractor {

	static Connection connection = null;

	static PrintWriter writer = null;

	public static void main(String[] argv) {

		System.out.println("-------- PostgreSQL " + "JDBC Connection Testing ------------");

		try {

			Class.forName("org.postgresql.Driver");

		} catch (ClassNotFoundException e) {

			System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
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
		int countStep = 1000000;
		boolean lastLine = false;
		int step1 = 1;
		int step2 = 1000;

		try {
			openConn();
			st = connection.createStatement();
			rs = st.executeQuery("select distinct user_id from geo_tweets order by user_id");
			System.out.println("Consulta terminada... Inserindo dados...");

			while (rs.next()) {
				Long user_id = rs.getLong("user_id");
				insert(user_id);
			}

		} catch (SQLException se) {
			System.err.println("Erro fatal!");
			System.err.println(se.getMessage());
			countFail++;
		} finally {
			rs.close();
			st.close();
			closeConn();

		}

		System.out.println("---- Total de erros: " + countFail);
	}

	private static String[] extractCoords(Tweet tweet) {
		String[] out = new String[2];
		try {
			JSONArray jsonArray = new JSONArray("[" + tweet.getJson() + "]");
			int count = jsonArray.getJSONObject(0).length(); // get totalCount
																// of all
																// jsonObjects
			JSONObject jsonObject = jsonArray.getJSONObject(0).getJSONObject("coordinates"); // get
																								// jsonObject
																								// @
																								// i
																								// position
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

	private static Long extractUserId(Tweet tweet) {
		Long user_id = null;
		try {
			JSONArray jsonArray = new JSONArray("[" + tweet.getJson() + "]");
			JSONObject jsonObject = jsonArray.getJSONObject(0).getJSONObject("user"); // get
																						// jsonObject
																						// @
																						// i
																						// position
			String id_str = jsonObject.get("id_str").toString();
			user_id = new Long(id_str);

		} catch (JSONException e) {
			e.printStackTrace();
		}
		return user_id;
	}

	private static String extractMessage(Tweet tweet) {
		String text = null;
		try {
			JSONArray jsonArray = new JSONArray("[" + tweet.getJson() + "]");
			text = jsonArray.getJSONObject(0).getString("text"); // get
																	// jsonObject
																	// @ i
																	// position

		} catch (JSONException e) {
			e.printStackTrace();
		}
		return text;
	}

	public static void insert(Long user_id) throws SQLException {

		Statement stmt = connection.createStatement();
		String sql = "INSERT INTO geo_tweets_users" + "(" +  "user_id" + ") " + "VALUES (" + "'" + user_id + "'" + ");";

		stmt.executeUpdate(sql);
		stmt.close();
	}


	private static void openConn() throws SQLException {
		connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/tweets", "postgres", "admin");
		connection.setAutoCommit(false);
	}

	private static void closeConn() throws SQLException {
		connection.commit();
		connection.close();
	}

}
