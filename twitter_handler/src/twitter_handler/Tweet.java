package twitter_handler;

import java.sql.Date;
import java.sql.Timestamp;

public class Tweet {
	Long tid;
	String json;
	Long user_id;
	Double longitude;
	Double latitude;
	String message;
	Timestamp date;
	
	public Tweet(Long tid, String jsoon) {
		super();
		this.tid = tid;
		this.json = jsoon;
	}

	public Long getTid() {
		return tid;
	}

	public void setTid(Long tid) {
		this.tid = tid;
	}

	public String getJson() {
		return json;
	}

	public void setJson(String jsoon) {
		this.json = jsoon;
	}

	public Long getUser_id() {
		return user_id;
	}

	public void setUser_id(Long user_id) {
		this.user_id = user_id;
	}

	public Double getLongitude() {
		return longitude;
	}

	public void setLongitude(Double longitude) {
		this.longitude = longitude;
	}

	public Double getLatitude() {
		return latitude;
	}

	public void setLatitude(Double latitude) {
		this.latitude = latitude;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public Timestamp getDate() {
		return date;
	}

	public void setDate(Timestamp date) {
		this.date = date;
	}
	
	
	

}
