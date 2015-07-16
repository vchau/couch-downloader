package shipit.later;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.couchbase.client.java.document.StringDocument;
import com.couchbase.client.java.view.DesignDocument;
import com.couchbase.client.java.view.View;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;

public class BucketDownloader {
	public static final String DESIGN_DOC = "CouchDownloader";
	public static final String VIEW = "all_docs";
	private String host;
	private String bucket;
	private String password;
	private CouchbaseClient client;

	public BucketDownloader(String host, String bucket, String password) {
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

	private void downloadViews(String outputPath) throws IOException {
		String viewDir = outputPath + "/views";
		File vd = new File(viewDir);
		if (!vd.exists()) {
			vd.mkdirs();
			System.out.println("Created: " + viewDir);
		}

		List<DesignDocument> listDesignDoc = client.getBucket().bucketManager()
				.getDesignDocuments();

		for (DesignDocument dd : listDesignDoc) {
			String designDocDir = viewDir + "/" + dd.name();
			File ddd = new File(designDocDir);
			ddd.mkdirs();
			System.out.println("Created: " + designDocDir);

			List<View> listView = dd.views();
			for (View v : listView) {
				String viewFileName = designDocDir + "/" + v.name() + ".json";
				FileOutputStream fos = new FileOutputStream(viewFileName);
				System.out.println("Writing view: " + viewFileName);
				fos.write(v.map().getBytes());
				fos.write("\n".getBytes());
				if (v.reduce() != null) {
					fos.write(v.reduce().getBytes());
				}
				fos.close();
			}
		}
	}

	public void download(String outputPath) throws IOException {
		downloadViews(outputPath);

		// 1: Load the View infos
		ViewQuery viewQuery = ViewQuery.from(DESIGN_DOC, VIEW);

		ViewResult result = client.getBucket().query(viewQuery);
		String documentFileName = outputPath + Main.OUT_JSON_DOCS_FILENAME;
		FileOutputStream fos = new FileOutputStream(documentFileName);
		System.out.println("Writing documents: " + documentFileName);
		BufferedOutputStream bos = new BufferedOutputStream(fos);
		FileOutputStream kvFile = new FileOutputStream(outputPath
				+ Main.OUT_NONJSON_DOCS_FILENAME);
		BufferedOutputStream kvBos = new BufferedOutputStream(kvFile);

		try {
			// 4: Iterate over the Data and print out the full document
			for (ViewRow row : result) {
				try {
					String csvLine = row.id() + ","
							+ row.document().content().toString();
					bos.write(csvLine.getBytes());
					bos.write("\n".getBytes());
				} catch (Exception e) {
					// System.out.println("Error: " + e);
					if (e instanceof com.couchbase.client.java.error.TranscodingException) {
						kvBos.write((row.key().toString() + "," + row.document(
								StringDocument.class).content()).getBytes());
						kvBos.write("\n".getBytes());
					}
				}
			}
		} finally {
			bos.close();
			kvBos.close();
		}
	}
}
