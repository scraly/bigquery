# bigquery

## General

Google BigQuery is a fast, economical and fully managed data warehouse for large-scale data analytics.

1. If you're new to BQ, take a look at: https://cloud.google.com/bigquery/what-is-bigquery

2. Sign up for a free BigQuery trial: https://console.cloud.google.com/freetrial?pli=1

3. Here you can find BigQuery UI console: https://bigquery.cloud.google.com/welcome

4. In this git repository you can find some tooltip and useful (i guess) classes

5. If you want to know more about BQ, you can take a look at my slides: http://www.slideshare.net/aurelievache/dans-les-coulisses-de-google-bigquery?qid=d0fe2426-ecf9-411e-a6f7-a63351490934&v=&b=&from_search=1

##BQ command line tool

### Adding a column to a table

```bq --format=prettyjson show yourdataset.yourtable > newschema.json```

Edit table.json and remove everything except the inside of "fields" (e.g. keep the [ { "name": "x" ... }, ... ]). Then add your new field to the schema. Then run, for each table: 

```bq update <dataset>.<tablename> newschema.json```

You can add --apilog=apilog.txt to the beginning of the command line which will show exactly what is sent / returned from the bigquery server.

Enjoy BQ!
