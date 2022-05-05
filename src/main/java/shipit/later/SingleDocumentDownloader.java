package shipit.later;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import com.couchbase.client.java.document.JsonDocument;
import org.apache.commons.io.IOUtils;

import com.couchbase.client.java.document.RawJsonDocument;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SingleDocumentDownloader extends CouchbaseWorker {
	public SingleDocumentDownloader(String host, String bucket, String password) {
		super(host, bucket, password);
	}

	public String fetchById(String id, CouchbaseClient srcClient) {
		JsonDocument jd = srcClient.get(id);
		if (jd == null)
			return null;

		return jd.content().toString();
	}

	public void download(String docId, String outputPath) throws IOException {
		String documentFileName = outputPath + "/"
				+ Main.OUT_JSON_DOCS_FILENAME;
		FileOutputStream fos = new FileOutputStream(documentFileName);
		System.out.println("Writing documents: " + documentFileName);

		try (BufferedOutputStream bos = new BufferedOutputStream(fos)) {

			String content = fetchById(docId, client);
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
			bos.write(content.getBytes());
		}
	}
}
