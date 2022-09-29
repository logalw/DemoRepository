%let MYUSER=franir ;

%let TENANTID=b1c14d5c-3625-45b3-a430-9552373a0c2f ;
%let APPID=a2e7cfdc-93f8-4f12-9eda-81cf09a84566 ;
%let APPSECRET=O1e7Q~Xp2WNfMGJc8M-iONqvClXGaOOWiZ4mQ ;

%let HADOOPJARPATH=/azuredm/access-clients/spark/jars/sas ;

%let SYNAPSE_SERVER=&MYUSER.-synapse-ws.dev.azuresynapse.net ;

%let SQL_SERVERLESS_SERVER=&MYUSER.-synapse-ws-ondemand.sql.azuresynapse.net ;
%let SQL_UID=sqladminuser ;
%let SQL_PWD=Synapse123 ;

%let SPARK_POOL=sparkpool ;
%let SPARK_DB=sparkdb ;

%let SYNAPSE_RESTURL=https://&MYUSER.-synapse-ws.dev.azuresynapse.net/livyApi/versions/2019-11-01-preview/sparkPools/&SPARK_POOL ;

%let SQL_DEDICATED_SERVER=&MYUSER.-synapse-ws.sql.azuresynapse.net ;
%let SQL_DEDICATED_POOL=sqlpool ;

%let SQL_DSN=sqls_dsnless ;


cas mysession sessopts=(azuretenantid="&TENANTID");


/* Prepare CASLIBs */

/* ADLS Connection */
caslib adls datasource=
   (
      srctype="adls",
      accountname="&MYUSER.azuredmstorage",
      filesystem="blobdata",
      applicationid="&APPID",
      resource="https://storage.azure.com/", 
      dnssuffix="dfs.core.windows.net"
   ) subdirs libref=adls ;

proc casutil incaslib="adls" ;
   list files ;
quit ;

caslib spark drop ;
caslib spark datasource=
   (
      srctype="spark",
      username="&APPID",
      password="&APPSECRET",
      server="&SYNAPSE_SERVER",
      schema="&SQL_DEDICATED_POOL",
      hadoopJarPath="&HADOOPJARPATH",
      resturl="&SYNAPSE_RESTURL",
      bulkload=no
   ) libref=spark ;

/* Fails because no spark support for Synapse */
/* CASLIB just used for publishing */
proc casutil incaslib="spark" ;
   list files ;
quit ;

caslib sqlpool drop ;
caslib sqlpool datasource=
   (
      srctype="sqlserver",
      conopts="HostName=&SQL_DEDICATED_SERVER;Database=&SQL_DEDICATED_POOL;uid=&SQL_UID;pwd=&SQL_PWD;dsn=&SQL_DSN;EnableScrollableCursors=4;EnableQuotedIdentifiers=1;EnableBulkLoad=4;",
      schema="dbo"
   ) libref=sqlpool ;

proc casutil incaslib="sqlpool" ;
   list files ;
quit ;

caslib spkpool drop ;
caslib spkpool datasource=
   (
      srctype="sqlserver",
      conopts="HostName=&SQL_SERVERLESS_SERVER;Database=&SPARK_DB;uid=&SQL_UID;pwd=&SQL_PWD;dsn=&SQL_DSN;EnableScrollableCursors=4;EnableQuotedIdentifiers=1;EnableBulkLoad=4;",
      schema="dbo"
   ) libref=spkpool ;

proc casutil incaslib="spkpool" ;
   list files ;
quit ;

proc casutil incaslib="spkpool" outcaslib="spkpool" ;
	load casdata="hmeq_prod_spark" casout="hmeq_prod" ;
quit ;



/* Scoring Accelerator */

data adls.hmeq_train adls.hmeq_test ;
   length RECORD_PK 8 ;
   set sampsio.hmeq ;
   record_pk+1 ;
   if _n_<=4172 then output adls.hmeq_train ;
   else output adls.hmeq_test ;
run ;

data adls.hmeq_prod(drop=i) ;
   length RECORD_PK 8 ;
   set sampsio.hmeq ;
   do i=1 to 2 ;
      record_pk+1 ;
      output ;
   end ;
run ;

/* Create the hmeq_prod in ADLS as a CSV file */ 
proc casutil incaslib="adls" outcaslib="adls" ;
   deleteSource casdata="hmeq_prod.csv" quiet ;
   save casdata="hmeq_prod" casout="hmeq_prod.csv" replace ;
   list files ;
quit ;

/* Create the hmeq_prod in SQL Pool */ 
proc casutil ;
   deleteSource incaslib="sqlpool" casdata="hmeq_sql" quiet ;
   save casdata="hmeq_prod" incaslib="adls" outcaslib="sqlpool" casout="hmeq_sql"
      options=(
         bulkload=true
         accountname="&MYUSER.azuredmstorage",
         dnsSuffix="dfs.core.windows.net",
         filesystem="blobdata",
         folder="bulkload",
         identity="MANAGED IDENTITY",
         applicationid="&APPID"
      ) replace ;
   list files incaslib="sqlpool" ;
quit ;


/* Gradient Boosting modeling */
proc gradboost data=adls.hmeq_train seed=12345 ;
   id record_pk ;
   input Delinq Derog Job nInq Reason / level = nominal ;
   input CLAge CLNo DebtInc Loan Mortdue Value YoJ / level = interval ;
   target Bad / level = nominal ;
   /* Save an analytic store */
   savestate rstore=adls.gradboost_store ;
run ;

/* List caslib contents */
proc casutil incaslib="adls" ;
   list tables ;
quit ;


/*
proc scoreaccel sessref=mysession ;
   deletemodel
      target=filesystem
      caslib="adls"
      modelname="gradboost1" 
      modeldir="/models" ;
quit ;
*/

proc scoreaccel sessref=mysession ;
   publishmodel
      target=filesystem
      caslib="adls"
      password="&APPSECRET"
      modelname="gradboost1"
      storetables="adls.gradboost_store"
      modeldir="/models" 
      replacemodel=yes ;
quit ;

proc cas ;
   sparkEmbeddedProcess.startSynapseSparkEP caslib="spark" trace=true ;
quit ;

/*
proc cas;
   sparkEmbeddedProcess.startSynapseSparkEP caslib="spark" 
      executorInstances=2 executorCores=2 
      executorMemory=10 driverMemory=4;
quit;
*/

/* SparkTable -> SparkTable */

/* Drop the table in Synapse */

proc scoreaccel sessref=mysession ;
   runmodel 
      target=synapse
      caslib="spark"
      modelname="01_gradboost_astore" 
      modeldir="/models" 
      intable="hmeq_spark"
      schema="&SPARK_DB"
      outtable="hmeq_spark_astore"
      outschema="&SPARK_DB"
      forceoverwrite=yes ;
quit ;

/* SQL Table -> SQL Table */

proc casutil incaslib="sqlpool" ;
   deletesource casdata="hmeq_sql_astore" quiet ;
   list files ;
quit ;

proc scoreaccel sessref=mysession ;
   runmodel 
      target=synapse
      caslib="spark"
      modelname="01_gradboost_astore" 
      modeldir="/models" 
      schema="&SQL_DEDICATED_POOL"
      intable="dbo.hmeq_sql"
      outtable="dbo.hmeq_sql_astore"
      outschema="&SQL_DEDICATED_POOL"
      forceoverwrite=yes ;
quit ;

proc casutil incaslib="sqlpool" ;
   list files ;
quit ;

/* SparkTable -> SparkDataset */

proc scoreaccel sessref=mysession ;
   runmodel 
      target=synapse
      caslib="spark"
      modelname="gradboost1" 
      modeldir="/models" 
      intable="hmeq_prod"
      schema="sparkdb"
      outdataset="ds"
      forceoverwrite=yes ;
quit ;

proc cas;
   sparkEmbeddedProcess.executeProgram caslib="spark"
   program='
println("Number of rows = " + ds.count);
ds.show;
' ;
run ;
quit ;

proc cas;
   sparkEmbeddedProcess.stopSynapseSparkEP caslib="spark" ;
quit ;

cas mysession terminate ;


