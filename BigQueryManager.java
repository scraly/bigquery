package com.vachea.google.bigquery;

import java.io.IOException;

import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.googleapis.batch.BatchRequest;
import com.google.api.client.googleapis.batch.json.JsonBatchCallback;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Jobs.GetQueryResults;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.QueryRequest;
import com.google.api.services.bigquery.model.QueryResponse;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableList.Tables;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

public class BigQueryManager {

    private static final Logger logger = LoggerFactory.getLogger(BigQueryManager.class);

    private static final String TYPE_STRING = "STRING";

    private static final int SLEEP_MILLIS = 5000;

    private static final long ONE_YEAR_RETENTION = 367*24*60*60*1000L;

    private String projectId; // Your Google Developer "Project Number"
    private String datasetId; // To be created in BigQuery console
    private GoogleHelper googleHelper;
    private Bigquery bigquery;

    public BigQueryManager(String projectId, String datasetId) throws IOException {
        super();
        this.projectId = projectId;
        this.datasetId = datasetId;
        this.googleHelper = new GoogleHelper();
        this.bigquery = new Bigquery.Builder(GoogleHelper.HTTP_TRANSPORT, GoogleHelper.JSON_FACTORY, this.googleHelper.getCredential())
	        .setHttpRequestInitializer(this.googleHelper.getCredential())
		//set your applicationName
	        .setApplicationName("toto")
	        .build();   		
    }

    public BigQueryManager() throws IOException {
	//projectId and applicationName
        this("123456789", "toto");
    }

    public BigQueryManager(String projectId, String datasetId,
			String clientId, String clientSecret, String accessToken,
			String refreshToken, String userId, String dataStoreDir) throws IOException {
        this.projectId = projectId;
        this.datasetId = datasetId;
        this.googleHelper = new GoogleHelper(clientId, clientSecret, accessToken,
    			refreshToken, userId, dataStoreDir);
        this.bigquery = new Bigquery.Builder(GoogleHelper.HTTP_TRANSPORT, GoogleHelper.JSON_FACTORY, this.googleHelper.getCredential())
	        .setHttpRequestInitializer(this.googleHelper.getCredential())
		//your applicationName
	        .setApplicationName("toto")
	        .build();
	}

	public Bigquery getBigquery() {
        return this.bigquery;
    }

    public List<Tables> getTablesList() throws IOException {
        return getBigquery().tables()
                .list(this.projectId, this.datasetId).execute().getTables();
    }

    public boolean checkTableExists(String tableId) {
        try {
        	getBigquery().tables()
            .get(this.projectId, this.datasetId, tableId).execute();
            return true;
        } catch (IOException e) { // table doesn't exists
            return false;
        }
    }

    public boolean checkTableExists(List<Tables> tables, String tableId) throws IOException {
        boolean exists = false;
        if (tables != null) {
            for (Tables t : tables) {
                if (t.getTableReference().getTableId().equals(tableId)) {
                    exists = true;
                    break;
                }
            }
        }
        return exists;
    }

    public Table createTable(String tableId, String[] schemaDef) throws IOException {
        List<TableFieldSchema> sfList = new ArrayList<TableFieldSchema>();
        for (String name : schemaDef) {
            String type = TYPE_STRING;
            if (name.contains(":")) {
                String[] nd = name.split(":");
                name = nd[0];
                type = nd[1];
            }
            sfList.add(new TableFieldSchema().setName(name).setType(type));
        } 
        TableSchema schema = new TableSchema().setFields(sfList);
        TableReference tableReference = new TableReference()
        .setProjectId(this.projectId)
        .setDatasetId(this.datasetId)
        .setTableId(tableId);

        long expirationTime = System.currentTimeMillis()+ONE_YEAR_RETENTION;

        Table table = new Table()
        .setTableReference(tableReference)
        .setExpirationTime(expirationTime)
        .setSchema(schema);
        return getBigquery().tables()
                .insert(this.projectId, this.datasetId, table).execute();
    }

    public TableDataInsertAllResponse insertRowsInTable(String tableName, 
            List<TableDataInsertAllRequest.Rows> rowList) throws IOException {
        return getBigquery().tabledata()
                .insertAll(this.projectId, this.datasetId, tableName, 
                        new TableDataInsertAllRequest().setRows(rowList))
                        .execute();
    }

    public QueryResponse queryTable(String query) throws IOException {
        return queryTable(query, null, null);
    }

    public QueryResponse queryTable(String query, Long maxRes, Long timeout) throws IOException {
        QueryRequest request = new QueryRequest();
        if (maxRes != null) {
            request.setMaxResults(maxRes);
        }
        if (timeout != null) {
            request.setTimeoutMs(timeout);
        }
        request.setQuery(query);
        request.setUseQueryCache(true);
        QueryResponse queryResponse = getBigquery().jobs()
                .query(this.projectId, request).execute();
        return queryResponse;

    }

    public GetQueryResultsResponse getQueryResults(String jobId, Long maxRes, Long timeout) throws IOException {
        GetQueryResults resultsReq = getBigquery().jobs().getQueryResults(projectId, jobId);
        if (timeout != null)
            resultsReq.setTimeoutMs(timeout);
        if (maxRes != null)
            resultsReq.setMaxResults(maxRes);
        GetQueryResultsResponse response = resultsReq.execute();
        while (!response.getJobComplete()) {
            try {
                Thread.sleep(SLEEP_MILLIS);
            } catch (InterruptedException e) {
            }
            response = resultsReq.execute();
        }
        return response;
    }

    public void close() {

    }

    public List<TableRow> runQueryRequest(String query) throws IOException {
        List<TableRow> res = new ArrayList<TableRow>();
        QueryRequest queryRequest = new QueryRequest().setQuery(query).setUseQueryCache(true);
        QueryResponse queryResponse = getBigquery().jobs().query(this.projectId, queryRequest).execute();
        if (queryResponse.getJobComplete()) {
            res.addAll(queryResponse.getRows());
            if (null == queryResponse.getPageToken()) {
                return res;
            }
        }
        return getQueryResults(queryResponse.getJobReference(), res);
    }

    public List<TableRow> getQueryResults(JobReference jref) throws IOException {
        return getQueryResults(jref, null);
    }

    private List<TableRow> getQueryResults(JobReference jref, List<TableRow> res) throws IOException {
        logger.debug("getQueryResults: jref= " + jref);

        // This loop polls until results are present, then loops over result pages.
        String pageToken = null;
        if (res == null) {
            res = new ArrayList<TableRow>();
        }
        while (true) {
            GetQueryResultsResponse queryResults = getBigquery().jobs().getQueryResults(projectId, jref.getJobId())
                    .setPageToken(pageToken).execute();
            if (queryResults.getJobComplete()) {
                if(queryResults.getRows() != null) {
                    res.addAll(queryResults.getRows());
                    pageToken = queryResults.getPageToken();
                    if (null == pageToken) {
                        return res;
                    }
                } else {
                    return res;
                }
            }
        }		
    }

    public Job startAsyncQuery(String query, String jobId)
            throws IOException {
        logger.debug("startAsyncQuery: query=" + query + ", jobId=" + jobId);

        JobConfigurationQuery queryConfig = new JobConfigurationQuery().setQuery(query).setUseQueryCache(true);
        JobConfiguration config = new JobConfiguration().setQuery(queryConfig);
        Job job = new Job().setId(jobId).setConfiguration(config);
        logger.debug("will insert jobs in queue");
        Job queuedJob = getBigquery().jobs().insert(this.projectId, job).execute();

        logger.debug("end of startAsyncQuery");
        return queuedJob;
    }

    public Job getJob(String query, String jobId) throws IOException {
        logger.debug("getJob: query=" + query + ", jobId=" + jobId);

        JobConfigurationQuery queryConfig = new JobConfigurationQuery().setQuery(query).setUseQueryCache(true);
        JobConfiguration config = new JobConfiguration().setQuery(queryConfig);
        Job job = new Job().setId(jobId).setConfiguration(config);
        logger.debug("new job created: " + job + " - jobId: " + jobId);
        logger.debug("end of queryJob");
        return job;
    }

    public void executingJobsInRequestBatching(ArrayList<Job> jobs, JsonBatchCallback<Job> callback) throws IOException {
        logger.debug("Add BigQuery using Batch");

        BatchRequest batch = getBigquery().batch();
        for (Job job : jobs) {
        	getBigquery().jobs().insert(this.projectId, job).queue(batch, callback);
        }

        logger.debug("executing requestbatching");

        batch.execute();
    }

    /**
     * 
     * @param job
     * @return -1 if error, 0 if not finished, 1 if done
     * @throws IOException 
     */
    public Job pollJob(JobReference jref) throws IOException {
        return getBigquery().jobs()
                .get(jref.getProjectId(), jref.getJobId())
                .execute();
    }
    public static void main(String[] args) throws IOException {

        //        String REDIRECT_URI = "urn:ietf:wg:oauth:2.0:oob";
        //        GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(new JacksonFactory(),
        //                new InputStreamReader(BigQueryManager.class.getResourceAsStream("client_secrets.json")));
        //        List<String> acl = new ArrayList<String>();
        //        acl.add(BigqueryScopes.BIGQUERY);
        //        acl.add("https://mail.google.com/");
        //        acl.add(CalendarScopes.CALENDAR_READONLY);
        //        // Create a URL to request that the user provide access to the BigQuery API
        //        String authorizeUrl = new GoogleAuthorizationCodeRequestUrl(
        //                clientSecrets,
        //                REDIRECT_URI,
        //                acl).build();
        //        // Prompt the user to visit the authorization URL, and retrieve the provided authorization code
        //        System.out.println("Paste this URL into a web browser to authorize BigQuery Access:\n" + authorizeUrl);
        //        System.out.println("... and paste the code you received here: ");
        //        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        //        String authorizationCode = in.readLine();
        //
        //        // Create a Authorization flow object
        //        GoogleAuthorizationCodeFlow flow =  new GoogleAuthorizationCodeFlow.Builder(HTTP_TRANSPORT,
        //                JSON_FACTORY, clientSecrets, acl)
        //        .setAccessType("offline")
        //        .setApprovalPrompt("force")
        //        .build();
        //        // Exchange the access code for a credential authorizing access to BigQuery
        //        GoogleTokenResponse response = flow.newTokenRequest(authorizationCode)
        //                .setRedirectUri(REDIRECT_URI).execute();
        //        flow.createAndStoreCredential(response, "toto@gmail.com");
        //        System.out.println("stored");
    }
}

class GoogleHelper {

	private static final String DEFAULT_DATASTORE_DIR = "/home/tutu/etc/project"; // to modify

	private static final String DEFAULT_USERID = "toto@gmail.com"; //to modify

	private static final String DEFAULT_CLIENT_SECRET = "toto"; //to modify

	private static final String DEFAULT_CLIENT_ID = "123456789.apps.googleusercontent.com"; //to modify

    // From javadoc http://javadoc.google-http-java-client.googlecode.com/hg/1.10.2-beta/com/google/api/client/http/HttpTransport.html
    // "Implementation is thread-safe, and sub-classes must be thread-safe. For maximum efficiency, 
    // applications should use a single globally-shared instance of the HTTP transport."
    public static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
    // Implementation is thread-safe, and sub-classes must be thread-safe. For maximum efficiency, 
    // applications should use a single globally-shared instance of the JSON factory.
    public static final JsonFactory JSON_FACTORY = new JacksonFactory();

    private String clientId;
    private String clientSecret;
    private String accessToken;
    private String refreshToken;
    private String userId; 
    private String dataStoreDir;
    private Credential credential;

    public GoogleHelper(String clientId, String clientSecret, 
    		String accessToken, 
    		String refreshToken, String userId,
            String dataStoreDir) throws IOException {
        super();
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.accessToken = accessToken;
        this.refreshToken = refreshToken;
        this.userId = userId;
        this.dataStoreDir = dataStoreDir;
        DataStoreFactory credentialStore = new FileDataStoreFactory(new File(this.dataStoreDir));
        this.credential = new GoogleCredential.Builder()
	        .setTransport(HTTP_TRANSPORT)
	        .setJsonFactory(JSON_FACTORY)
	        .setClientSecrets(this.clientId, this.clientSecret)
	        .addRefreshListener(new DataStoreCredentialRefreshListener(this.userId, credentialStore))
	        .build()
	        .setAccessToken(this.accessToken)
	        .setRefreshToken(this.refreshToken);
    }

    public GoogleHelper() throws IOException {
        this(DEFAULT_CLIENT_ID, DEFAULT_CLIENT_SECRET,
                "your_client_id",
                "your_client_secret", DEFAULT_USERID,
                DEFAULT_DATASTORE_DIR);

    }

    public GoogleHelper(String accessToken, String refreshToken, String userIdExt) throws IOException {
        this(DEFAULT_CLIENT_ID,  DEFAULT_CLIENT_SECRET,
                accessToken,
                refreshToken, DEFAULT_USERID+"/"+userIdExt,
                DEFAULT_DATASTORE_DIR);    	
    }
    
    public Credential getCredential() {
        return this.credential;
    }

}
