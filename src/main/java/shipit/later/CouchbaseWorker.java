package shipit.later;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public abstract class CouchbaseWorker {
	protected String host;
	protected String bucket;
	protected String password;
	protected CouchbaseClient client;

	public CouchbaseWorker(String host, String bucket, String password) {
		this.host = host;
		this.bucket = bucket;
		this.password = password;
	}

	public void init() throws IOException {
		List<String> hosts = Arrays.asList(host);

		// Connect to the Cluster
		client = new CouchbaseClient(hosts, bucket, password);

		client.init();
	}

	public void shutdown() {
		client.getCluster().disconnect();
	}
}
