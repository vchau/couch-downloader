package shipit.later;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import com.couchbase.client.java.view.DefaultView;
import com.couchbase.client.java.view.DesignDocument;
import com.couchbase.client.java.view.View;

public class ViewUploader extends CouchbaseWorker {

	public ViewUploader(String host, String bucket, String password) {
		super(host, bucket, password);
	}

	public void upload(String inputPath) throws IOException {
		String viewDir = inputPath + "/views";
		File vd = new File(viewDir);
		if (vd.exists()) {
			String[] listDesignDocs = vd.list();
			if (listDesignDocs != null) {
				for (String dd : listDesignDocs) {
					String ddDir = viewDir + "/" + dd;
					File ddd = new File(ddDir);
					String[] listViewFiles = ddd.list();
					List<View> viewsToInsert = new ArrayList<View>();
					for (String viewFileName : listViewFiles) {
						System.out.println("Found view: " + viewFileName);
						String fullViewFileName = ddDir + "/" + viewFileName;
						Scanner scanner = new Scanner(
								new File(fullViewFileName));
						try {
							String map = scanner.useDelimiter("\\Z").next();
							String viewName = viewFileName.substring(0,
									viewFileName.indexOf("."));
							viewsToInsert
									.add(DefaultView.create(viewName, map));
							System.out.println("View " + viewName + ": " + map);
						} finally {
							scanner.close();
						}
					}
					DesignDocument designDocument = DesignDocument.create(dd,
							viewsToInsert);
					this.client.getBucket().bucketManager()
							.upsertDesignDocument(designDocument, true);
					this.client.getBucket().bucketManager()
							.upsertDesignDocument(designDocument);
				}
			}
		}
	}
}
