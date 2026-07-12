CREATE OR REPLACE VIEW analytics.treasury_datatape_tableau AS 
 WITH dollar AS (
         SELECT treasury_datatape_daily.contracthostid,
            treasury_datatape_daily.as_of_date,
            sum(
                CASE
                    WHEN treasury_datatape_daily.pcpddays > 30::double precision AND treasury_datatape_daily.pcpddays <= 150::double precision AND upper(treasury_datatape_daily.empstatus) <> 'DECEASED'::text AND treasury_datatape_daily.bankrupttype IS NULL THEN treasury_datatape_daily.spv
                    ELSE 0::double precision
                END) AS spv_dq_amt,
            sum(
                CASE
                    WHEN treasury_datatape_daily.pcpddays > 150::double precision OR upper(treasury_datatape_daily.empstatus) = 'DECEASED'::text OR treasury_datatape_daily.bankrupttype IS NOT NULL THEN treasury_datatape_daily.spv
                    ELSE 0::double precision
                END) AS spv_default_amt,
            sum(
                CASE
                    WHEN treasury_datatape_daily.pcpddays <= 150::double precision AND upper(treasury_datatape_daily.empstatus) <> 'DECEASED'::text AND treasury_datatape_daily.bankrupttype IS NULL THEN treasury_datatape_daily.spv
                    ELSE 0::double precision
                END) AS spv_dq_total_amt,
            sum(treasury_datatape_daily.spv) AS spv_default_total_amt
           FROM ods.treasury_datatape_daily
          GROUP BY treasury_datatape_daily.as_of_date, treasury_datatape_daily.contracthostid
        ), percent AS (
         SELECT dollar.contracthostid,
            dollar.as_of_date,
                CASE
                    WHEN dollar.spv_dq_amt = 0::double precision THEN 0::double precision
                    ELSE dollar.spv_dq_amt / dollar.spv_dq_total_amt
                END AS spv_dq,
                CASE
                    WHEN dollar.spv_default_amt = 0::double precision THEN 0::double precision
                    ELSE dollar.spv_default_amt / dollar.spv_default_total_amt
                END AS spv_default
           FROM dollar
        ), clients AS (
         SELECT percent.contracthostid,
            percent.as_of_date,
                CASE
                    WHEN percent.contracthostid = ANY (ARRAY['5850'::text, '1500'::text]) THEN 'No'::text
                    WHEN percent.spv_dq > 0.25::double precision OR percent.spv_default > 0.2::double precision THEN 'No'::text
                    ELSE 'Yes'::text
                END AS spv_eligible
           FROM percent
        )
 SELECT a.as_of_date,
    a.orderid,
    a.soldto,
    a.soldpool,
    a.soldamt,
    a.original,
    a.balance,
    a.applied_total,
    a.adjusted_total,
    a.credited_total,
    a.preloads,
    a.cash_applied_total,
    a.cashbalance,
    a.applied_inmonth,
    a.adjusted_inmonth,
    a.credited_inmonth,
    a.cash_applied_inmonth,
    a.spv,
    a.duration,
    a.contracthostid,
    a.account_name,
    a.client_grade,
    a.industry,
    a.channel,
    a.contractstate,
    a.accountstate,
    a.backup_payment_client,
    a.salary,
    a.salary_range,
    a.pmttype,
    a.tenure,
    a.pcpddays,
    a.status,
    a.contractual_co,
    a.contractual_co_date,
    a.contractual_co_bal,
    a.bankrupttype,
    a.termstatus,
    a.termdate,
    a.accountid,
    a.invamt,
    a.empstatus,
    a.payfreq,
    a.hiredate,
    a.originationdate,
    a.lastapplydate,
    a.firstduedate,
    a.nextduedate,
    a.numpmts,
    a.remainingpayments,
    a.over2500,
    a.eligible,
    a.reason,
    a.facilitystatus,
    a.lessfrequentthanmonthly,
    a.modification_flg,
    a.exclusionreason,
    a.default_in_month,
    a.returns_in_month,
    a.contractversion,
    a.order_cancelled,
    a.order_shipped,
    a.order_unshipped,
    a.gracedays,
    a.order_type,
    a.repurchased_in_month,
    c.spv_eligible
   FROM ods.treasury_datatape_daily a
     LEFT JOIN clients c ON a.contracthostid = c.contracthostid AND a.as_of_date = c.as_of_date;;
