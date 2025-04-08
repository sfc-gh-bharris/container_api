-- From https://quickstarts.snowflake.com/guide/snowflake_personalization_api/index.html?index=..%2F..index#1

USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE WAREHOUSE DATA_API_WH WITH WAREHOUSE_SIZE='xsmall';

CREATE ROLE DATA_API_ROLE;

GRANT USAGE ON WAREHOUSE DATA_API_WH TO ROLE DATA_API_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE_SAMPLE_DATA TO ROLE DATA_API_ROLE;

GRANT ROLE DATA_API_ROLE TO ROLE ACCOUNTADMIN;

CREATE DATABASE API_TEST;
CREATE SCHEMA API_TEST.DATA;

GRANT ALL ON DATABASE API_TEST TO ROLE DATA_API_ROLE;
GRANT ALL ON SCHEMA API_TEST.PUBLIC TO ROLE DATA_API_ROLE;
GRANT ALL ON SCHEMA API_TEST.DATA TO ROLE DATA_API_ROLE;
GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO ROLE DATA_API_ROLE;


-- Now we're going to create our user and key for that user
CREATE or REPLACE USER API_USER PASSWORD='<CHANGE_THIS>' LOGIN_NAME='API_USER' MUST_CHANGE_PASSWORD=FALSE, DISABLED=FALSE, DEFAULT_WAREHOUSE='DATA_API_WH', DEFAULT_NAMESPACE='API_TEST.DATA', DEFAULT_ROLE='DATA_API_ROLE';

GRANT ROLE DATA_API_ROLE TO USER API_USER;

-- We're going to connect via keypair authentication.
-- We need to run the following from our command line (this is for mac, PC may vary slightly)

-- openssl genrsa 4096 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
-- openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
-- PUBK=`cat ./rsa_key.pub | grep -v KEY- | tr -d '\012'`
-- echo "ALTER USER INGEST SET RSA_PUBLIC_KEY='$PUBK';

-- we're going to take the output from that command and run it here.
-- 

ALTER USER API_USER SET RSA_PUBLIC_KEY='<CHANGE_THIS>';


-- At this point, we should be able to connect.
-- Check out /Users/bharris/src/apr-4-25-API-TEST/src/simple_test.py
-- You may need to set up your environment for the imports, you can add them via python -m pip install <NAME_OF_IMPORT>

-- This is the query that will run when someone hits '/customer/<cust_id>'
SELECT
    C_CUSTOMER_SK,
	C_CUSTOMER_ID,
	C_CURRENT_CDEMO_SK,
	C_CURRENT_HDEMO_SK,
	C_CURRENT_ADDR_SK,
	C_FIRST_SHIPTO_DATE_SK,
	C_FIRST_SALES_DATE_SK,
	C_SALUTATION,
	C_FIRST_NAME,
	C_LAST_NAME,
	C_PREFERRED_CUST_FLAG,
	C_BIRTH_DAY,
	C_BIRTH_MONTH,
	C_BIRTH_YEAR,
	C_BIRTH_COUNTRY,
	C_LOGIN,
	C_EMAIL_ADDRESS,
	C_LAST_REVIEW_DATE
    FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.CUSTOMER
    WHERE C_CUSTOMER_SK = 1;

-- Now, we're going to build the API.
-- Check out /Users/bharris/src/apr-4-25-API-TEST/src/app.py
-- We can run that and we can call and view our API. 
-- Now, this is a very basic API, and flask isn't really ready for production based on the server that it uses.
-- So, we're going to create another set of files that we will use in a docker container.
-- Check out /Users/bharris/src/apr-4-25-API-TEST/src/container_app.py, /Users/bharris/src/apr-4-25-API-TEST/src/container_connector.py, /Users/bharris/src/apr-4-25-API-TEST/src/container_snowpark.py

-- we'll build our docker container.
-- docker build -t papi .

-- we're going to run it on Snowpark Container Services.

CREATE OR REPLACE IMAGE REPOSITORY API;
GRANT READ ON IMAGE REPOSITORY API TO ROLE DATA_API_ROLE;
SHOW IMAGE REPOSITORIES;

-- Get the repository_url from this (sfsenorthamerica-demo-bharris.registry.snowflakecomputing.com/api_test/data/api)
-- Now we'll push the container to the repository we just created

-- docker login sfsenorthamerica-demo-bharris.registry.snowflakecomputing.com/api_test/data/api
-- docker build -t sfsenorthamerica-demo-bharris.registry.snowflakecomputing.com/api_test/data/api/papi .
-- docker push sfsenorthamerica-demo-bharris.registry.snowflakecomputing.com/api_test/data/api/papi

-- then we should see our image here.
SHOW IMAGES IN IMAGE REPOSITORY API;

-- now we're going to create the compute pool.
USE ROLE ACCOUNTADMIN;

CREATE COMPUTE POOL API
  MIN_NODES = 1
  MAX_NODES = 2
  INSTANCE_FAMILY = CPU_X64_XS;

GRANT USAGE ON COMPUTE POOL API TO ROLE DATA_API_ROLE;
GRANT MONITOR ON COMPUTE POOL API TO ROLE DATA_API_ROLE;

-- and this is the service that will host the application.
USE ROLE DATA_API_ROLE;
DROP SERVICE API_TEST.DATA.API;
CREATE SERVICE API_TEST.DATA.API
 IN COMPUTE POOL API
 FROM SPECIFICATION  
$$
spec:
  container:
  - name: api
    image: /api_test/data/api/papi:latest
  endpoint:
  - name: api
    port: 8001
    public: true
$$
QUERY_WAREHOUSE = DATA_API_WH;

ALTER SERVICE API_TEST.DATA.API RESUME;

-- it will take a bit to provision and initialize. 
-- You can check status with these commands:

CALL SYSTEM$GET_SERVICE_STATUS('api');
CALL SYSTEM$GET_SERVICE_LOGS('api_test.data.api', 0, 'api');

-- After your service has started, you can get the endpoints with this command:
SHOW ENDPOINTS IN SERVICE API;

-- and here, you can shut down the container
ALTER SERVICE API.PUBLIC.API SUSPEND;