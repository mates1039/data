select count(1) as cnt,imp_date,sum(preloads) preload_amt 
from dbo._ppc_pfr_preloads_filtered_tb (nolock) 
group by imp_date; --> max data recevied by 5:30pm IST

 for removing duplicates: (If count is more than 2M)
-----------------------

--create table tmp.daily_tape_bkp_2026_06_30 as  
--select * from ods.treasury_datatape_daily where as_of_date = current_date - 1;

--delete from ods.treasury_datatape_daily where as_of_date = current_date - 1;

--INSERT INTO ods.treasury_datatape_daily 
--SELECT DISTINCT ON (orderid) *
--FROM tmp.daily_tape_bkp_2026_06_30
--ORDER BY orderid, as_of_date DESC;

select count(*) from ods.treasury_datatape_daily where as_of_date = current_date - 1;

for removing mismatch:
----------------------
--delete from ods.treasury_datatape_daily
--where  orderid  IN ('60723315', '60861080', '61170066', '61939584', '60902218', 
--'61859639', '60795011', '59914201', '61118512', '61208303', '60291394', 
--'60874383', '61851360', '59575953', '59674568', '61918276', '60405235', 
--'61379098', '61400138', '60160899', '60702511', '61688170', '61056008', 
--'59701182', '63629400', '61355433', '62052112', '60003959')
--and as_of_date = current_date - 1;


select orderid,SOLDTO, SOLDAMT, SOLDDATE, SOLDPOOL from ods.xxcon_contract_stage where contractid in
('60723315', '60861080', '61170066', '61939584', '60902218', '61859639', '60795011', 
'59914201', '61118512', '61208303', '60291394', '60874383', '61851360', '59575953', 
'59674568', '61918276', '60405235', '61379098', '61400138', '60160899', '60702511', 
'61688170', '61056008', '59701182', '63629400', '61355433', '62052112', '60003959');


--insert into ods.treasury_datatape_daily
--select * from ods.nls_treasury_datatape_daily_v where orderid in
--('60723315', '60861080', '61170066', '61939584', '60902218', '61859639', 
--'60795011', '59914201', '61118512', '61208303', '60291394', '60874383', 
--'61851360', '59575953', '59674568', '61918276', '60405235', '61379098', 
--'61400138', '60160899', '60702511', '61688170', '61056008', '59701182', 
--'63629400', '61355433', '62052112', '60003959');

GET http://10.175.50.219:8083/connectors/con-prodfin-xxcontract-xxcon_contract_incremental_ot_ss_20260630/config/
GET http://10.175.50.219:8083/connectors/con-prodfin-xxcontract-xxcon_contract_incremental_ot_ss_20260630/status/
PUT http://10.175.50.219:8083/connectors/con-prodfin-xxcontract-xxcon_contract_incremental_ot_ss_20260630/config/
DELETE http://10.175.50.219:8083/connectors/con-prodfin-xxcontract-xxcon_contract_incremental_ot_ss_20260630/

{
  "connection.attempts": "3",
  "connection.backoff.ms": "10000",
  "connection.password": "${file:/data1/edm_credentials.properties:prod_fin_pwd}",
  "connection.url": "${file:/data1/edm_credentials.properties:prod_fin_url}",
  "connection.user": "${file:/data1/edm_credentials.properties:prod_fin_user}",
  "connector.class": "JdbcSourceConnector",
  "mode": "bulk",
  "name": "con-prodfin-xxcontract-xxcon_contract_incremental_ot_ss_20260630",
  "numeric.mapping": "best_fit",
  "poll.interval.ms": "86400000",
  "producer.override.client.id": "con-prodfin-xxcontract-xxcon_contract_incremental_ot_ss_20260630",
  "query": "SELECT A.*, greatest(nvl(NIGHTLYUPDATE,'01-jan-2001'),nvl(CREATIONDATE,'01-jan-2001')) AS CDC_FIELD,XXCONTRACT.GETDEDFIRSTSENTDT(ORDERID) AS FIRST_DED_DATE ,NVL((SELECT MAX('1') FROM XXCONTRACT.XX_CONTRACT_MODIFICATIONS WHERE ORDERID = A.ORDERID), '0') AS MODIFICATION_FLG FROM XXCONTRACT.XXCON_CONTRACT A WHERE A.CONTRACTID IN ('60723315', '60861080', '61170066', '61939584', '60902218', '61859639', '60795011', '59914201', '61118512', '61208303', '60291394', '60874383', '61851360', '59575953', '59674568', '61918276', '60405235', '61379098', '61400138', '60160899', '60702511', '61688170', '61056008', '59701182', '63629400', '61355433', '62052112', '60003959')",
  "schema.pattern": "XXCONTRACT",
  "table.types": "QUERY",
  "tasks.max": "1",
  "topic.prefix": "tpc-prodfin-xxcontract-XXCON_CONTRACT",
  "transforms": "Cast",
  "transforms.Cast.spec": "INVAMT:float64,INTRATE:float64,ORDERNETAMT:float64,OVERUNDERAMT:float64,NETPRINCIPAL:float64,SOLDAMT:float64,PREVBALANCE:float64,MAXPCAMT:float64,MINPCAMT:float64,COAMT:float64,COREVAMT:float64,ORIGINAL:float64,BALANCE:float64,CREDITED:float64,ADJUSTED:float64,APPLIED:float64,FIRSTAPPLYAMT:float64,LASTAPPLYAMT:float64,FEESORIGINAL:float64,FEESBALANCE:float64,FEESPDAMT:float64,FEESPDDAYS:float64,DEPORIGINAL:float64,DEPBALANCE:float64,PCPDAMT:float64,FEESPDCNT:float64,VERSIONTARGETAMT:float64,COLLSOLDAMT:float64,COLLWARRADJ:float64,COLLREDIRECTPMTS:float64,ORDER_TOTAL:float64,ORDER_SHIPPED:float64,ORDER_UNSHIPPED:float64,ORDER_CANCELLED:float64,CONTRACTUAL_CO_BAL:float64",
  "transforms.Cast.type": "org.apache.kafka.connect.transforms.Cast$Value"
}

Once connector is created check for last 15 mins in Kibana (es-prodfin-xxcontract-xxcon_contract_stage) to cross check the hits and data which is the mismatch record counts
Then check on the PROD_es_to_gp airflow DAG for the contract table to get loaded.


 SELECT 
    pid,
    usename,
    datname,
    state,
    query_start,
    now() - query_start AS duration,
    now() as current_timestamp,
    LEFT(query, 1000) AS query_preview
FROM pg_stat_activity 
WHERE state != 'idle';



select now()- max(order_dt) as delay from edw.order_transaction_fact_v;   --> Daily Metrics (45mins delay accepted) - [PROD_cust_to_orders]

select now()- max(modifiedts) as delay from ods.stocklevels_stage;    --> Merch Buyer's Report (2hrs delay accepted) - [PROD_es_to_gp_stage]




thank you Hari and Shyam for the helpful session I know lots of stuff for me to learn but with you guys I believe it will be easy I appreciate that 
 
I will be keeping an eye on the treasury datatape dag today but today I will have some meetings with helpdesk team to setup couple of tools also it seems I will need couple more jira tickets to get all of these tools up and running on my side  
 
checking the dag log seems like same issue happened as yesterday and you need to kill the insert  query manually ? do I see that correctly or any other issue happened ? 
 
I see data is matching message
 
yes you are right
 
but we may have duplicates again as yesterday right ?
 
I killed the second insert manually
 
MUCAHIT ATES
but we may have duplicates again as yesterday right ?
yesterday we missed to kill it
 
so no duplicates we need manually check for sale info mismatch now
 
Will include you as well . I will ping you shortly. 
 
sorry I was in the meeting with help team. thank you Hari
 
 
Wednesday 1:18 PM Call started

 
 
select now()- max(order_dt) as delay from edw.order_transaction_fact_v;   --> Daily Metrics (45mins delay accepted) - [PROD_cust_to_orders]
select now()- max(modifiedts) as delay from ods.stocklevels_stage;    --> Merch Buyer's Report (2hrs delay accepted) - [PROD_es_to_gp_stage]
 
Wednesday 2:04 PM Call ended 45m 51s 

 
Thursday 9:57 AM Call started

 
SELECT

    pid,

    usename,

    state,

    now() - query_start AS duration,

    LEFT(query, 100) AS query_preview, query_start

FROM pg_stat_activity

WHERE state != 'idle'

order by duration desc

;

 
Thursday 10:17 AM Call ended 19m 55s 

 
Friday 9:39 AM Call started

 
looking at the treasury and seems like you had to set it to success manually. is that the line you take it as an indicator that it exceeds the max capacity >500000 then you set it to success manually ?
[2026-07-09, 13:03:45 UTC] {ssh.py:522} INFO - Data has been loaded to the stage table - ods_stg.stage_stage_nls_pfr_preloads with TotalRecord Count = 505718 
 
Friday 10:01 AM Call ended 21m 49s 

 
pid 16011 not seen anymore I think loading is complete yes ?
 
yes correct
 
in the airflow log if you see it will say --> ods.treasury_datatape_daily is Loaded
 
so since that was the 1st insert into so we are good and no need to run the queries for mismatch yes ?
 
Shyam Sundar S
in the airflow log if you see it will say --> ods.treasury_datatape_daily is Loaded
yup I also saw that too everything looking good I believe 
 
MUCAHIT ATES
so since that was the 1st insert into so we are good and no need to run the queries for mismatch yes ?
yes teh process will check for mismatches and give the count if we have any
 
I mean so far
 
oh we still need it then even for the first insert into
 
can I do that guys like yesterday I did let me try that one more time 
 


 
