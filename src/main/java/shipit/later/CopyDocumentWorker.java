package shipit.later;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.RawJsonDocument;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CopyDocumentWorker extends CouchbaseWorker {
	public CopyDocumentWorker(String host, String bucket, String password) {
		super(host, bucket, password);
	}

	public CouchbaseClient initSrcClient(String srcHost, String bucket, String password) throws IOException {
		List<String> hosts = Arrays.asList(srcHost);

		// Connect to the Cluster
		CouchbaseClient srcClient = new CouchbaseClient(hosts, bucket, password);

		srcClient.init();
		return srcClient;
	}

	public String fetchById(String id, CouchbaseClient srcClient) {
		JsonDocument jd = srcClient.get(id);
		if (jd == null)
			return null;

		return jd.content().toString();
	}

	/**
	 * Note that `srcClient` is the source Couch client that will be initialized.
	 * The member `client` is the destination Couch client that is managed by the
	 * parent class.
	 * 
	 * @param docId
	 * @param srcHost
	 * @param srcBucket
	 * @param srcPassword
	 * @throws IOException
	 */
	public void copy(String docId, String srcHost, String srcBucket, String srcPassword) throws IOException {
		CouchbaseClient srcClient = null;
		try {
			srcClient = initSrcClient(srcHost, srcBucket, srcPassword);
			String content = fetchById(docId, srcClient);
			if (content == null) {
				throw new RuntimeException("Document not found.");
			}

			ObjectMapper om = new ObjectMapper();
			JsonNode doc = om.readValue(content, JsonNode.class);

			JsonNode idNode = doc.get("id");
			if (idNode != null && docId != null && !docId.equals(idNode.asText())) {
				throw new RuntimeException(
						"Provided id " + docId + " is not equal to 'id' found in document: " + idNode.asText());
			}
			String key = null;
			if (docId != null) {
				key = docId;
			} else if (idNode != null) {
				key = idNode.asText();
			}
			if (key == null) {
				throw new IllegalArgumentException("Could not determine document id.");
			} else {
				System.out.println("Creating document with id: " + key);
			}

			client.getBucket().upsert(RawJsonDocument.create(key, content));

		} finally {
			if (srcClient != null) {
				client.getCluster().disconnect();
			}
		}
	}

}
