-- Databricks notebook source
create external location if not exists datamart
url 'gs://delta_mof/gold'
with (storage credential datamart_acc)


-- COMMAND ----------

create external location if not exists ext_edw
url 'gs://delta_mof/tarnsformed'
with (storage credential 
edw_acc )

-- COMMAND ----------

create external location if not exists landing
url 'gs://delta_mof/landing/external'
with (storage credential landing_ext_files_acc)

-- COMMAND ----------

select current_catalog()

-- COMMAND ----------

create catalog if not exists MOF

-- COMMAND ----------

create schema if not exists mof.datamart


-- COMMAND ----------

create schema if not exists mof.edw


-- COMMAND ----------

create schema if not exists mof.staging


-- COMMAND ----------

create EXTERNAL volume if not exists mof.staging.crm
location 'gs://delta_mof/landing/external/CRM'

-- COMMAND ----------

create external volume if not exists mof.staging.ubs
location 'gs://delta_mof/landing/external/UBS'

-- COMMAND ----------


