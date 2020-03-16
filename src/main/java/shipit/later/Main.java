package shipit.later;

import java.io.File;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

public class Main {
	public static final String OUT_JSON_DOCS_FILENAME = "json_docs.json";
	public static final String OUT_NONJSON_DOCS_FILENAME = "nonjson_docs.txt";

	public static final String ARG_BUCKET = "b";
	public static final String ARG_OUTPUT_PATH = "o";
	public static final String ARG_INPUT_PATH = "i";
	public static final String ARG_COUCH_HOST = "h";
	public static final String ARG_PASSWORD = "p";
	public static final String ARG_DOWNLOAD = "download";
	public static final String ARG_UPLOAD = "upload";
	public static final String ARG_UPLOAD_SINGLE_DOC = "uploaddoc";
	public static final String ARG_TARGET_DOC_ID = "d";
	public static final String ARG_UPLOAD_VIEWONLY = "uploadviewonly";
	public static final String ARG_COPY_DOC = "copydoc";
	public static final String ARG_SRC_HOST = "4";
	public static final String ARG_SRC_BUCKET = "6";
	public static final String ARG_SRC_PASSWORD = "8";

	public static void main(String[] args) throws Exception {
		Options options = new Options();
		options.addOption(ARG_COUCH_HOST, true,
				"(Required) Couchbase host name. Port is assumed to be 8091.");
		options.addOption(ARG_PASSWORD, true, "(Required) Couchbase password.");
		options.addOption(ARG_BUCKET, true, "(Required) Couch bucket name.");
		options.addOption(ARG_OUTPUT_PATH, true,
				"(Optional) Output directory path. Default: /tmp/couchbase");
		options.addOption(ARG_INPUT_PATH, true,
				"(Optional) Input directory path. Default: /tmp/couchbase");
		options.addOption(ARG_DOWNLOAD, false,
				"(Optional) Download mode. Default: false");
		options.addOption(ARG_UPLOAD, false,
				"(Optional) Upload mode. Default: false");
		options.addOption(ARG_UPLOAD_SINGLE_DOC, false, "(Optional) Upload single document mode. Default: false");
		options.addOption(ARG_UPLOAD_VIEWONLY, false,
				"(Optional) Upload views only mode. Default: false");
		options.addOption(ARG_TARGET_DOC_ID, true, "(Optional) The id of the document created.");
		options.addOption(ARG_COPY_DOC, false, "(Optional) Copy documeent from src to dest.");
		options.addOption(ARG_SRC_HOST, true, "(Optional) Src host to copy from.");
		options.addOption(ARG_SRC_BUCKET, true, "(Optional) Src buckeeet to copy from.");
		options.addOption(ARG_SRC_PASSWORD, true, "(Optional) Src password to copy from.");
		

		CommandLineParser parser = new BasicParser();
		CommandLine cmd = parser.parse(options, args);
		String host = cmd.getOptionValue(ARG_COUCH_HOST);
		String bucket = cmd.getOptionValue(ARG_BUCKET);
		String password = cmd.getOptionValue(ARG_PASSWORD);
		String outputPath = cmd.getOptionValue(ARG_OUTPUT_PATH,
				"/tmp/couchbase");
		String inputPath = cmd.getOptionValue(ARG_INPUT_PATH, "/tmp/couchbase");
		boolean isDownload = cmd.hasOption(ARG_DOWNLOAD);
		boolean isUpload = cmd.hasOption(ARG_UPLOAD);
		String docId = cmd.getOptionValue(ARG_TARGET_DOC_ID);
		boolean isUploadSingleDoc = cmd.hasOption(ARG_UPLOAD_SINGLE_DOC);
		boolean isUploadViewOnly = cmd.hasOption(ARG_UPLOAD_VIEWONLY);
		boolean isCopyDoc = cmd.hasOption(ARG_COPY_DOC);
		String srcHost = cmd.getOptionValue(ARG_SRC_HOST);
		String srcBucket = cmd.getOptionValue(ARG_SRC_BUCKET);
		String srcPassword = cmd.getOptionValue(ARG_SRC_PASSWORD);

		if (host == null || host.isEmpty()) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("couch-downloader -[OPTION] [value]", options);
			return;
		}

		if (isDownload) {
			System.out.println("Begin downloading...");
			File outputDir = new File(outputPath);
			if (!outputDir.exists()) {
				outputDir.mkdirs();
				System.out.println("Created output path: " + outputPath);
			}

			BucketDownloader downloader = new BucketDownloader(host, bucket,
					password);
			try {
				downloader.init();
				downloader.download(outputPath);
			} finally {
				downloader.shutdown();
			}
		} else if (isUpload) {
			System.out.println("Uploading document beginning");
			JsonDocumentUploader jsonUploader = new JsonDocumentUploader(host,
					bucket, password);
			try {
				System.out.print("Uploading json documents...");
				jsonUploader.init();
				jsonUploader.upload(inputPath);
			} finally {
				jsonUploader.shutdown();
			}

			NonJsonDocumentUploader nonJsonUploader = new NonJsonDocumentUploader(
					host, bucket, password);
			try {
				System.out.print("Uploading non-json documents...");
				nonJsonUploader.init();
				nonJsonUploader.upload(inputPath);
			} finally {
				nonJsonUploader.shutdown();
			}
		} else if (isUploadSingleDoc) {
			System.out.println("Uploading single document");
			SingleDocumentUploader jsonUploader = new SingleDocumentUploader(host, bucket, password);
			try {
				System.out.print("Uploading single json documents...");
				jsonUploader.init();
				jsonUploader.upload(docId, inputPath);
			} finally {
				jsonUploader.shutdown();
			}
		} else if (isUploadViewOnly) {
			System.out.println("Uploading views only...");
			ViewUploader viewUploader = new ViewUploader(host, bucket,
					password);
			try {
				viewUploader.init();
				viewUploader.upload(inputPath);
			} finally {
				viewUploader.shutdown();
			}
		} else if (isCopyDoc) {
			System.out.println("Copying single document");
			CopyDocumentWorker worker = new CopyDocumentWorker(host, bucket, password);
			try {
				System.out.print("Copying single json documents...");
				worker.init();
				worker.copy(docId, srcHost, srcBucket, srcPassword);
			} finally {
				worker.shutdown();
			}

		} else {
			System.out.println(
					"Neither -download nor -upload specified. Do nothing.");
		}
	}
}
