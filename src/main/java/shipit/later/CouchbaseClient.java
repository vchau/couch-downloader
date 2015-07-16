package shipit.later;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import rx.Observable;
import rx.functions.Func1;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

public class CouchbaseClient {
	private List<String> hostList;
	private String bucketName;
	private String password;
	private CouchbaseCluster cluster;
	private Bucket bucket;

	public CouchbaseClient(List<String> hostList, String bucketName, String pwd) {
		this.hostList = hostList;
		this.bucketName = bucketName;
		this.password = pwd;
	}

	@PostConstruct
	public void init() throws IOException {
		CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
				.kvTimeout(5000).build();
		cluster = CouchbaseCluster.create(env, hostList);

		bucket = cluster.openBucket(bucketName, password);

	}

	@PreDestroy
	public void close() {
		cluster.disconnect();
	}

	public Bucket getBucket() {
		return bucket;
	}

	public CouchbaseCluster getCluster() {
		return cluster;
	}

	public JsonDocument get(String id) {
		return bucket.get(id);
	}

	public RawJsonDocument insert(String id, String document) {
		return bucket.insert(RawJsonDocument.create(id, document));
	}

	public RawJsonDocument insert(String id, Integer ttlInSec, String document) {
		return bucket.insert(RawJsonDocument.create(id, ttlInSec, document));
	}

	public RawJsonDocument upsert(String id, String document) {
		return bucket.upsert(RawJsonDocument.create(id, document));
	}

	public RawJsonDocument upsert(String id, Integer ttlInSec, String document) {
		return bucket.upsert(RawJsonDocument.create(id, ttlInSec, document));
	}

	public List<JsonDocument> getBulk(String[] ids) {
		return Observable.from(ids)
				.flatMap(new Func1<String, Observable<JsonDocument>>() {
					@Override
					public Observable<JsonDocument> call(String id) {
						return bucket.async().get(id);
					}
				}).toList().toBlocking().single();
	}

	public Map<String, JsonDocument> getBulkAsMap(String[] ids) {
		return Observable.from(ids)
				.flatMap(new Func1<String, Observable<JsonDocument>>() {
					@Override
					public Observable<JsonDocument> call(String id) {
						return bucket.async().get(id);
					}
				}).toMap(new Func1<JsonDocument, String>() {
					@Override
					public String call(JsonDocument d) {
						return d.id();
					}
				}).toBlocking().single();
	}
}
