## This script extracts crime data from an api and loads it to an S3 bucket.


#### To run locally

Set all these environment variables in a .env file

```
RAPID_API_KEY
SNOWFLAKE_USERNAME
SNOWFLAKE_PASSWORD
SNOWFLAKE_ACCOUNT
BUCKET_NAME
AWS_ACCESS_KEY
AWS_SECRET_ACCESS_KEY
```

Build and run the docker container

`docker compose up --build`

*If you encounter a 403 during this step, try restarting docker hub and runnning this:

`aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws`

Post an event to trigger the script

`curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{}'`
