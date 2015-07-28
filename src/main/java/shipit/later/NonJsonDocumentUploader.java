package shipit.later;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import com.couchbase.client.java.document.RawJsonDocument;

public class NonJsonDocumentUploader extends CouchbaseWorker {

	public NonJsonDocumentUploader(String host, String bucket,
			String password) {
		super(host, bucket, password);
	}

	public void upload(String inputPath) throws IOException {
		String docFilename = inputPath + "/nonjson_docs.txt";
		File docFile = new File(docFilename);
		if (docFile.exists()) {
			try (BufferedReader br = new BufferedReader(new InputStreamReader(
					new FileInputStream(docFile), "UTF-8"))) {
				String line = null;
				int count = 0;
				int total = 0;
				while ((line = br.readLine()) != null) {
					int commaIndex = line.indexOf(',');
					String key = line.substring(0, commaIndex);
					String value = line.substring(commaIndex + 1,
							line.length());
					client.getBucket()
							.upsert(RawJsonDocument.create(key, value));
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
