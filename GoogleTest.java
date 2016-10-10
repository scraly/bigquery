package com.vachea.google.bigquery;

import java.io.BufferedReader;

/**
 * Project Id : 123456789
 *
 */
public class GoogleTest {
    //TODO change your home and app secrets
    private static final String TMP_REPOSITORY = "/home/vachea/tmp/";
    private static final String TMP_TOKEN_PROPERTIES = "/home/vachea/tmp/token.properties";

    private static final String TABLE_ID = "table_id";
    private static final String DATASET_ID = "dataset_id";
    private static final String CLIENT_ID = "client_id";
    private static final String CLIENT_SECRET = "client_secret";
    private static final String ACCESS_TOKEN = "access_token";
    private static final String REFRESH_TOKEN = "refresh_token";
    private static final String USER_EMAIL = "toto@gmail.com";

    // Your Google Developer Project number
    private static final String PROJECT_NUMBER = "project_number";

    // Load Client ID/secret from client_secrets.json file
    private static final String CLIENTSECRETS_LOCATION = "client_secrets.json";
    static GoogleClientSecrets clientSecrets = loadClientSecrets();

    static GoogleClientSecrets loadClientSecrets() {
        try {
            GoogleClientSecrets clientSecrets =
                    GoogleClientSecrets.load(new JacksonFactory(),
                            new InputStreamReader(GoogleTest.class.getResourceAsStream(CLIENTSECRETS_LOCATION)));
            return clientSecrets;
        } catch (Exception e) {
            System.out.println("Could not load client_secrets.json");
            e.printStackTrace();
            return null;
        }
    }
    // For installed applications, use the redirect URI "urn:ietf:wg:oauth:2.0:oob"
    private static final String REDIRECT_URI = "urn:ietf:wg:oauth:2.0:oob";

    // Objects for handling HTTP transport and JSON formatting of API calls
    private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
    private static final JsonFactory JSON_FACTORY = new JacksonFactory();


    public GoogleTest() {
    }


    /**
     * @param args
     * @throws IOException 
     */
    //	 static Bigquery bigquery;
    public static void main(String[] args) throws IOException {
        //        Credential credential = getInteractiveAndStoreGoogleCredential();
        //		query(credential);
        //		uploadFileInStorage(credential);
        //		uploadFileInBigQuery(credential, false);
        //		streamDataToBigQuery(credential);

    }

    public static void streamDataToBigQuery(final Credential credential) throws IOException {
        Bigquery bigquery = new Bigquery.Builder(HTTP_TRANSPORT,JSON_FACTORY,credential)
        .setHttpRequestInitializer(new HttpRequestInitializer() {
            public void initialize(HttpRequest httpRequest) throws IOException {
                credential.initialize(httpRequest);
                httpRequest.setConnectTimeout(3 * 60000);  // 3 minutes connect timeout
                httpRequest.setReadTimeout(3 * 60000);  // 3 minutes read timeout
            }
        })
        .setApplicationName("BigQueryTest")
        .build();

        // create table
        TableReference tableReference = new TableReference()
        .setDatasetId(DATASET_ID)
        .setProjectId(PROJECT_NUMBER)
        .setTableId(TABLE_ID);
        List<TableFieldSchema> sfList = new ArrayList<TableFieldSchema>();
        sfList.add(new TableFieldSchema().setName("age").setType("STRING"));
        sfList.add(new TableFieldSchema().setName("sex").setType("STRING"));
        TableSchema schema = new TableSchema().setFields(sfList);
        Table table = new Table()
        .setTableReference(tableReference)
        .setSchema(schema);
        Table resp = bigquery.tables().insert(PROJECT_NUMBER, DATASET_ID, table).execute();
        System.out.println(resp.toPrettyString());

        // insert data

        List<TableDataInsertAllRequest.Rows> rowList = new ArrayList<TableDataInsertAllRequest.Rows>();
        rowList.add(new TableDataInsertAllRequest.Rows()
        .setInsertId(""+System.currentTimeMillis())
        .setJson(new TableRow().set("adt", null)));

        TableDataInsertAllRequest content = new TableDataInsertAllRequest().setRows(rowList);
        TableDataInsertAllResponse response = bigquery.tabledata().insertAll(
                PROJECT_NUMBER, DATASET_ID, TABLE_ID, content).execute();	
        System.out.println("kind="+response.getKind());
        System.out.println("errors="+response.getInsertErrors());
        System.out.println(response.toPrettyString());

    }

    public static void uploadFileInBigQuery(final Credential credential, boolean useStorage) throws IOException {
        Bigquery bigquery = new Bigquery.Builder(HTTP_TRANSPORT,JSON_FACTORY,credential)
        .setHttpRequestInitializer(new HttpRequestInitializer() {
            public void initialize(HttpRequest httpRequest) throws IOException {
                credential.initialize(httpRequest);
                httpRequest.setConnectTimeout(3 * 60000);  // 3 minutes connect timeout
                httpRequest.setReadTimeout(3 * 60000);  // 3 minutes read timeout
            }
        })
        .setApplicationName("BigQueryTest")
        .build();

        TableReference table = new TableReference();
        table.setProjectId(PROJECT_NUMBER);
        table.setDatasetId(DATASET_ID);
        table.setTableId(TABLE_ID);

        JobConfigurationLoad jcl = new JobConfigurationLoad();
        jcl.setDestinationTable(table);
        jcl.setWriteDisposition("WRITE_TRUNCATE");
        if (useStorage) {
            jcl.setSourceUris(Arrays.asList("gs://totodata/CLIENT.txt"));
        }
        JobConfiguration jconf = new JobConfiguration();
        jconf.setLoad(jcl);

        Job job = new Job();
        job.setJobReference(new JobReference().setProjectId(PROJECT_NUMBER));
        job.setConfiguration(jconf);

        Bigquery.Jobs.Insert insert = null;
        if (useStorage) {
            insert = bigquery.jobs().insert(PROJECT_NUMBER, job);			
        } else {
            FileContent content = new  FileContent("application/octet-stream", new File("/home/vachea","file.csv"));
            insert = bigquery.jobs().insert(PROJECT_NUMBER, job, content);			
        }
        insert.setProjectId(PROJECT_NUMBER);
        Job insertJob = insert.execute();
        if (insertJob.getStatus().getState().equals("RUNNING") ||
                insertJob.getStatus().getState().equals("PENDING")) {
            while (true) {
                System.out.println("Waiting 5000 ms");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    // ignore
                }
                Job pollJob = bigquery.jobs().get(PROJECT_NUMBER, insertJob.getJobReference().getJobId()).execute();
                if (!(pollJob.getStatus().getState().equals("RUNNING") ||
                        pollJob.getStatus().getState().equals("PENDING"))) {
                    System.out.println(pollJob.toPrettyString());
                    break;
                }
            }
        } else {
            System.out.println("Error while inserting job");
        }
    }

    public static void query(final Credential credential) throws IOException {
        // Use the credential to create an authorized BigQuery client
        Bigquery bigquery = new Bigquery.Builder(HTTP_TRANSPORT,JSON_FACTORY,credential)
        .setHttpRequestInitializer(new HttpRequestInitializer() {
            public void initialize(HttpRequest httpRequest) throws IOException {
                credential.initialize(httpRequest);
                httpRequest.setConnectTimeout(3 * 60000);  // 3 minutes connect timeout
                httpRequest.setReadTimeout(3 * 60000);  // 3 minutes read timeout
            }
        })
        .setApplicationName("BigQueryTest")
        .build();
        // Create a query statement and query request object
        String query = "SELECT top, COUNT(top) AS cnt FROM toto.tutu WHERE top CONTAINS 'rance' GROUP BY top order by cnt DESC;";
        QueryRequest request = new QueryRequest();

        request.setQuery(query);

        // Make a call to the BigQuery API
        QueryResponse queryResponse = bigquery.jobs().query(PROJECT_NUMBER, request).execute();
        // Retrieve and print the result of the query API response
        List<TableRow> rows = queryResponse.getRows();

        System.out.println("Query Results:\n----------------");
        for (TableRow row : rows) {
            for (TableCell field : row.getF()) {
                System.out.printf("%-50s", field.getV());
            }
            System.out.println();
        }
    }

    public static Credential getGoogleCredentialFromRefresh() throws IOException {
        File tokenFile = new File(TMP_REPOSITORY);
        DataStoreFactory credentialStore = new FileDataStoreFactory(tokenFile);
        Credential credential = new GoogleCredential.Builder()
        .setTransport(HTTP_TRANSPORT)
        .setJsonFactory(JSON_FACTORY)
        .setClientSecrets(CLIENT_ID, CLIENT_SECRET)
        .addRefreshListener(new DataStoreCredentialRefreshListener(USER_EMAIL, credentialStore))
        .build()
        .setAccessToken(ACCESS_TOKEN)
        .setRefreshToken(REFRESH_TOKEN);

        return credential;
    }

    public static Credential getInteractiveAndStoreGoogleCredential() throws IOException {
        Credential credential = null;
        // Attempt to Load existing Refresh Token
        String storedRefreshToken = loadRefreshToken();
        // Check to see if the an existing refresh token was loaded.
        // If so, create a credential and call refreshToken() to get a new
        // access token.
        if (storedRefreshToken != null) {
            // Request a new Access token using the refresh token.
            credential = new GoogleCredential.Builder().setTransport(HTTP_TRANSPORT)
                    .setJsonFactory(JSON_FACTORY)
                    .setClientSecrets(clientSecrets)
                    .build()
                    .setFromTokenResponse(new TokenResponse().setRefreshToken(storedRefreshToken));
            credential.refreshToken();

            System.out.println("access token (readKey)= "+credential.getAccessToken());
            System.out.println("refresh token (deleteKey)= "+credential.getRefreshToken());
        } else {
            // Exchange the auth code for an access token and refesh token
            credential = getInteractiveGoogleCredential();
            System.out.println("access token= "+credential.getAccessToken());
            // Store the refresh token for future use.
            storeRefreshToken(credential.getRefreshToken());
        }
        return credential;
    }
    /**
     * Gets a credential by prompting the user to access a URL and copy/paste the token
     * @return
     * @throws IOException
     */
    public static Credential getInteractiveGoogleCredential() throws IOException {
        List<String> acl = new ArrayList<String>();
        acl.add(BigqueryScopes.BIGQUERY);
        //		acl.add(StorageScopes.DEVSTORAGE_READ_WRITE);
        // Create a URL to request that the user provide access to the BigQuery API
        String authorizeUrl = new GoogleAuthorizationCodeRequestUrl(
                clientSecrets,
                REDIRECT_URI,
                acl).build();
        // Prompt the user to visit the authorization URL, and retrieve the provided authorization code
        System.out.println("Paste this URL into a web browser to authorize BigQuery, Youtube and MyBusiness access:\n" + authorizeUrl);
        System.out.println("... and paste the code you received here: ");
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        String authorizationCode = in.readLine();

        // Create a Authorization flow object

        GoogleAuthorizationCodeFlow flow = 
                new GoogleAuthorizationCodeFlow.Builder(HTTP_TRANSPORT,
                        JSON_FACTORY,
                        clientSecrets,
                        acl)
        .setAccessType("offline")
        .setApprovalPrompt("force")
        .build();
        // Exchange the access code for a credential authorizing access to BigQuery
        GoogleTokenResponse response = flow.newTokenRequest(authorizationCode)
                .setRedirectUri(REDIRECT_URI).execute();
        Credential credential = flow.createAndStoreCredential(response, null);
        return credential;
    }

    /**
     *  Helper to store a new refresh token in token.properties file.
     */
    private static void storeRefreshToken(String refresh_token) {
        Properties properties = new Properties();
        properties.setProperty("refreshtoken", refresh_token);
        System.out.println("refresh token= " + properties.get("refreshtoken"));
        try {
            properties.store(new FileOutputStream(TMP_TOKEN_PROPERTIES), null);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     *  Helper to load refresh token from the token.properties file.
     */
    private static String loadRefreshToken(){
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(TMP_TOKEN_PROPERTIES));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return (String) properties.get("refreshtoken");
    }

}

