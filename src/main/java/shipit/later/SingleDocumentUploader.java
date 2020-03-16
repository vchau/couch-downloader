package shipit.later;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;

import com.couchbase.client.java.document.RawJsonDocument;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SingleDocumentUploader extends CouchbaseWorker {
	public SingleDocumentUploader(String host, String bucket, String password) {
		super(host, bucket, password);
	}

	public void upload(String docId, String inputPath) throws IOException {
		String jsonDocFilename = inputPath;
		File jsonDocFile = new File(jsonDocFilename);
		if (jsonDocFile.exists()) {
			try (InputStream is = new FileInputStream(jsonDocFile)) {
				String content = IOUtils.toString(is, "UTF-8");
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
			}
		}
	}
}
