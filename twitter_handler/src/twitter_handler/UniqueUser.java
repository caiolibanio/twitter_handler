package twitter_handler;

public class UniqueUser {
	
	private Long tid;
	private Long user_id;
	
	public UniqueUser(Long tid, Long user_id) {
		super();
		this.tid = tid;
		this.user_id = user_id;
	}

	public Long getTid() {
		return tid;
	}

	public void setTid(Long tid) {
		this.tid = tid;
	}

	public Long getUser_id() {
		return user_id;
	}

	public void setUser_id(Long user_id) {
		this.user_id = user_id;
	}
	
	
	

}
