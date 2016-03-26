package twitter_handler;


public class Tweet {
	Long tid;
	String json;
	
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
	
	

}
