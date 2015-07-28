package shipit.later;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import com.couchbase.client.java.document.RawJsonDocument;

public class JsonDocumentUploader extends CouchbaseWorker {

	public JsonDocumentUploader(String host, String bucket, String password) {
		super(host, bucket, password);
	}

	public void upload(String inputPath) throws IOException {
		String jsonDocFilename = inputPath + "/json_docs.json";
		File jsonDocFile = new File(jsonDocFilename);
		if (jsonDocFile.exists()) {
			try (BufferedReader br = new BufferedReader(new InputStreamReader(
					new FileInputStream(jsonDocFile), "UTF-8"))) {
				String line = null;
				int count = 0;
				int total = 0;
				while ((line = br.readLine()) != null) {
					int commaIndex = line.indexOf(',');
					String key = line.substring(0, commaIndex);
					String json = line.substring(commaIndex + 1, line.length());
					client.getBucket()
							.upsert(RawJsonDocument.create(key, json));
					total++;
					count++;
					if (count > 10) {
						System.out.print(".");
						count = 0;
					}
				}
				System.out.println("Total count: " + total);
			}
		}
	}
}
