define term1_begin = 26120
define term1_end   = 26150
define term2_begin = 26160
define term2_end   = 26230

set verify off
set line 3000
set pages 0
set long 1000000
set longchunksize 1000000
set feed off
set trims on

spool test_sqldiff.log

WITH t1 AS (
  select
    SQL_ID
    , PLAN_HASH_VALUE
    , sum(EXECUTIONS_DELTA) as EXECUTIONS
    , sum(ELAPSED_TIME_DELTA) as ELAPSED_TIME
    , sum(CPU_TIME_DELTA) as CPU_TIME
    , sum(BUFFER_GETS_DELTA) as BUFFER_GETS
    , sum(IOWAIT_DELTA) as IOWAIT
    , sum(CLWAIT_DELTA) as CLWAIT
    , sum(APWAIT_DELTA) as APWAIT
    , sum(CCWAIT_DELTA) as CCWAIT
    , sum(PLSEXEC_TIME_DELTA) as PLSEXEC_TIME
    , sum(JAVEXEC_TIME_DELTA) as JAVEXEC_TIME
    , sum(ROWS_PROCESSED_DELTA) as ROWS_PROCESSED
    , sum(FETCHES_DELTA) as FETCHES
    , sum(SORTS_DELTA) as SORTS
    , sum(DISK_READS_DELTA) as DISK_READS
    , sum(DIRECT_WRITES_DELTA) as DIRECT_WRITES
    , sum(PHYSICAL_READ_REQUESTS_DELTA) as PHYSICAL_READ_REQUESTS
    , sum(PHYSICAL_READ_BYTES_DELTA) as PHYSICAL_READ_BYTES
    , sum(PHYSICAL_WRITE_REQUESTS_DELTA) as PHYSICAL_WRITE_REQUESTS
    , sum(PHYSICAL_WRITE_BYTES_DELTA) as PHYSICAL_WRITE_BYTES
    , sum(OPTIMIZED_PHYSICAL_READS_DELTA) as OPTIMIZED_PHYSICAL_READS
    , sum(CELL_UNCOMPRESSED_BYTES_DELTA) as CELL_UNCOMPRESSED_BYTES
    , sum(IO_OFFLOAD_RETURN_BYTES_DELTA) as IO_OFFLOAD_RETURN_BYTES
    , sum(IO_OFFLOAD_ELIG_BYTES_DELTA) as IO_OFFLOAD_ELIG_BYTES
    , sum(IO_INTERCONNECT_BYTES_DELTA)    as IO_INTERCONNECT_BYTES
  from dba_hist_sqlstat
  where snap_id >= &term1_begin
    and snap_id <= &term1_end
  group by sql_id, plan_hash_value
),
t2 AS (
  select
    SQL_ID
    , PLAN_HASH_VALUE
    , sum(EXECUTIONS_DELTA) as EXECUTIONS
    , sum(ELAPSED_TIME_DELTA) as ELAPSED_TIME
    , sum(CPU_TIME_DELTA) as CPU_TIME
    , sum(BUFFER_GETS_DELTA) as BUFFER_GETS
    , sum(IOWAIT_DELTA) as IOWAIT
    , sum(CLWAIT_DELTA) as CLWAIT
    , sum(APWAIT_DELTA) as APWAIT
    , sum(CCWAIT_DELTA) as CCWAIT
    , sum(PLSEXEC_TIME_DELTA) as PLSEXEC_TIME
    , sum(JAVEXEC_TIME_DELTA) as JAVEXEC_TIME
    , sum(ROWS_PROCESSED_DELTA) as ROWS_PROCESSED
    , sum(FETCHES_DELTA) as FETCHES
    , sum(SORTS_DELTA) as SORTS
    , sum(DISK_READS_DELTA) as DISK_READS
    , sum(DIRECT_WRITES_DELTA) as DIRECT_WRITES
    , sum(PHYSICAL_READ_REQUESTS_DELTA) as PHYSICAL_READ_REQUESTS
    , sum(PHYSICAL_READ_BYTES_DELTA) as PHYSICAL_READ_BYTES
    , sum(PHYSICAL_WRITE_REQUESTS_DELTA) as PHYSICAL_WRITE_REQUESTS
    , sum(PHYSICAL_WRITE_BYTES_DELTA) as PHYSICAL_WRITE_BYTES
    , sum(OPTIMIZED_PHYSICAL_READS_DELTA) as OPTIMIZED_PHYSICAL_READS
    , sum(CELL_UNCOMPRESSED_BYTES_DELTA) as CELL_UNCOMPRESSED_BYTES
    , sum(IO_OFFLOAD_RETURN_BYTES_DELTA) as IO_OFFLOAD_RETURN_BYTES
    , sum(IO_OFFLOAD_ELIG_BYTES_DELTA) as IO_OFFLOAD_ELIG_BYTES
    , sum(IO_INTERCONNECT_BYTES_DELTA)    as IO_INTERCONNECT_BYTES
  from dba_hist_sqlstat
  where snap_id >= &term2_begin
    and snap_id <= &term2_end
  group by sql_id, plan_hash_value
),
result AS (
  select
    t1.sql_id
    , TRUNC( t1.ELAPSED_TIME / t1.EXECUTIONS , 0) as avg_elapsed_time_t1
    , TRUNC( t2.ELAPSED_TIME / t2.EXECUTIONS , 0) as avg_elapsed_time_t2
    , TRUNC( (t2.ELAPSED_TIME             / t2.EXECUTIONS ) / nullif( t1.ELAPSED_TIME             / t1.EXECUTIONS , 0) * 100 , 1) as ELAPSED_TIME            
    , TRUNC( (t2.CPU_TIME                 / t2.EXECUTIONS ) / nullif( t1.CPU_TIME                 / t1.EXECUTIONS , 0) * 100 , 1) as CPU_TIME                
    , TRUNC( (t2.BUFFER_GETS              / t2.EXECUTIONS ) / nullif( t1.BUFFER_GETS              / t1.EXECUTIONS , 0) * 100 , 1) as BUFFER_GETS             
    , TRUNC( (t2.IOWAIT                   / t2.EXECUTIONS ) / nullif( t1.IOWAIT                   / t1.EXECUTIONS , 0) * 100 , 1) as IOWAIT                  
    , TRUNC( (t2.CLWAIT                   / t2.EXECUTIONS ) / nullif( t1.CLWAIT                   / t1.EXECUTIONS , 0) * 100 , 1) as CLWAIT                  
    , TRUNC( (t2.APWAIT                   / t2.EXECUTIONS ) / nullif( t1.APWAIT                   / t1.EXECUTIONS , 0) * 100 , 1) as APWAIT                  
    , TRUNC( (t2.CCWAIT                   / t2.EXECUTIONS ) / nullif( t1.CCWAIT                   / t1.EXECUTIONS , 0) * 100 , 1) as CCWAIT                  
    , TRUNC( (t2.PLSEXEC_TIME             / t2.EXECUTIONS ) / nullif( t1.PLSEXEC_TIME             / t1.EXECUTIONS , 0) * 100 , 1) as PLSEXEC_TIME            
    , TRUNC( (t2.JAVEXEC_TIME             / t2.EXECUTIONS ) / nullif( t1.JAVEXEC_TIME             / t1.EXECUTIONS , 0) * 100 , 1) as JAVEXEC_TIME            
    , TRUNC( (t2.ROWS_PROCESSED           / t2.EXECUTIONS ) / nullif( t1.ROWS_PROCESSED           / t1.EXECUTIONS , 0) * 100 , 1) as ROWS_PROCESSED          
    , TRUNC( (t2.FETCHES                  / t2.EXECUTIONS ) / nullif( t1.FETCHES                  / t1.EXECUTIONS , 0) * 100 , 1) as FETCHES                 
    , TRUNC( (t2.SORTS                    / t2.EXECUTIONS ) / nullif( t1.SORTS                    / t1.EXECUTIONS , 0) * 100 , 1) as SORTS                   
    , TRUNC( (t2.DISK_READS               / t2.EXECUTIONS ) / nullif( t1.DISK_READS               / t1.EXECUTIONS , 0) * 100 , 1) as DISK_READS              
    , TRUNC( (t2.DIRECT_WRITES            / t2.EXECUTIONS ) / nullif( t1.DIRECT_WRITES            / t1.EXECUTIONS , 0) * 100 , 1) as DIRECT_WRITES           
    , TRUNC( (t2.PHYSICAL_READ_REQUESTS   / t2.EXECUTIONS ) / nullif( t1.PHYSICAL_READ_REQUESTS   / t1.EXECUTIONS , 0) * 100 , 1) as PHYSICAL_READ_REQUESTS  
    , TRUNC( (t2.PHYSICAL_READ_BYTES      / t2.EXECUTIONS ) / nullif( t1.PHYSICAL_READ_BYTES      / t1.EXECUTIONS , 0) * 100 , 1) as PHYSICAL_READ_BYTES     
    , TRUNC( (t2.PHYSICAL_WRITE_REQUESTS  / t2.EXECUTIONS ) / nullif( t1.PHYSICAL_WRITE_REQUESTS  / t1.EXECUTIONS , 0) * 100 , 1) as PHYSICAL_WRITE_REQUESTS 
    , TRUNC( (t2.PHYSICAL_WRITE_BYTES     / t2.EXECUTIONS ) / nullif( t1.PHYSICAL_WRITE_BYTES     / t1.EXECUTIONS , 0) * 100 , 1) as PHYSICAL_WRITE_BYTES    
    , TRUNC( (t2.OPTIMIZED_PHYSICAL_READS / t2.EXECUTIONS ) / nullif( t1.OPTIMIZED_PHYSICAL_READS / t1.EXECUTIONS , 0) * 100 , 1) as OPTIMIZED_PHYSICAL_READS
    , TRUNC( (t2.CELL_UNCOMPRESSED_BYTES  / t2.EXECUTIONS ) / nullif( t1.CELL_UNCOMPRESSED_BYTES  / t1.EXECUTIONS , 0) * 100 , 1) as CELL_UNCOMPRESSED_BYTES 
    , TRUNC( (t2.IO_OFFLOAD_RETURN_BYTES  / t2.EXECUTIONS ) / nullif( t1.IO_OFFLOAD_RETURN_BYTES  / t1.EXECUTIONS , 0) * 100 , 1) as IO_OFFLOAD_RETURN_BYTES 
    , TRUNC( (t2.IO_OFFLOAD_ELIG_BYTES    / t2.EXECUTIONS ) / nullif( t1.IO_OFFLOAD_ELIG_BYTES    / t1.EXECUTIONS , 0) * 100 , 1) as IO_OFFLOAD_ELIG_BYTES   
    , TRUNC( (t2.IO_INTERCONNECT_BYTES    / t2.EXECUTIONS ) / nullif( t1.IO_INTERCONNECT_BYTES    / t1.EXECUTIONS , 0) * 100 , 1) as IO_INTERCONNECT_BYTES   
  from t1, t2
  where t1.sql_id = t2.sql_id
    and t1.plan_hash_value = t2.plan_hash_value
    and t1.EXECUTIONS > 0
    and t2.EXECUTIONS > 0
  order by sql_id
),
percentile as (
  select 
    percentile_cont(0.1) within group (order by rates asc) as p010
    , percentile_cont(0.2) within group (order by rates asc) as p020
    , percentile_cont(0.3) within group (order by rates asc) as p030
    , percentile_cont(0.4) within group (order by rates asc) as p040
    , percentile_cont(0.5) within group (order by rates asc) as p050
    , percentile_cont(0.6) within group (order by rates asc) as p060
    , percentile_cont(0.7) within group (order by rates asc) as p070
    , percentile_cont(0.8) within group (order by rates asc) as p080
    , percentile_cont(0.9) within group (order by rates asc) as p090
    , percentile_cont(1.0) within group (order by rates asc) as p100
  from result
  unpivot(
    rates for cols in (
      ELAPSED_TIME, CPU_TIME, BUFFER_GETS, IOWAIT, CLWAIT, APWAIT, CCWAIT, PLSEXEC_TIME, JAVEXEC_TIME, ROWS_PROCESSED, FETCHES, SORTS, DISK_READS, DIRECT_WRITES, PHYSICAL_READ_REQUESTS, PHYSICAL_READ_BYTES, PHYSICAL_WRITE_REQUESTS, PHYSICAL_WRITE_BYTES, OPTIMIZED_PHYSICAL_READS, CELL_UNCOMPRESSED_BYTES, IO_OFFLOAD_RETURN_BYTES, IO_OFFLOAD_ELIG_BYTES, IO_INTERCONNECT_BYTES
    )
  )
)
select
  '<html>' || CHR(10) ||
  '<head>' || CHR(10) ||
  '<title> awr diff report for SQLSTAT</title>' || CHR(10) ||
  '<style type="text/css">' || CHR(10) ||
  'th.wb {font:bold 8pt Arial,Helvetica,Geneva,sans-serif; color:White; background:#0066CC;padding-left:4px; padding-right:4px;padding-bottom:2px}' || CHR(10) ||
  'td.ac {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;background:#FFFFCC; vertical-align:top;text-align:start}' || CHR(10) ||
  'td.ae090 {font:8pt Arial,Helvetica,Geneva,sans-serif;color:white;background:#AA0000; vertical-align:top;text-align:end}' || CHR(10) ||
  'td.ae080 {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;background:#FF2020; vertical-align:top;text-align:end}' || CHR(10) ||
  'td.ae070 {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;background:#FF4040; vertical-align:top;text-align:end}' || CHR(10) ||
  'td.ae060 {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;background:#FF6060; vertical-align:top;text-align:end}' || CHR(10) ||
  'td.ae050 {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;background:#FF8080; vertical-align:top;text-align:end}' || CHR(10) ||
  'td.ae040 {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;background:#FFA0A0; vertical-align:top;text-align:end}' || CHR(10) ||
  'td.ae030 {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;background:#FFC0C0; vertical-align:top;text-align:end}' || CHR(10) ||
  'td.ae020 {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;background:#FFD0D0; vertical-align:top;text-align:end}' || CHR(10) ||
  'td.ae010 {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;background:#FFE0E0; vertical-align:top;text-align:end}' || CHR(10) ||
  'td.ae000 {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;background:#FFF0F0; vertical-align:top;text-align:end}' || CHR(10) ||
  'td.ae {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;background:#FFFFFF; vertical-align:top;text-align:end}' || CHR(10) ||
  '</style>' || CHR(10) ||
  '</head>' || CHR(10) ||
  '<body>' || CHR(10) ||
  '<h2>AWR SQLSTAT DIFF REPORT</h2>' || CHR(10)
from dual
union all
select
  '<h4>begin and end time at term1,term2</h4>' || CHR(10) ||
  '<table border="1">' || CHR(10) ||
  '<tr><th class="wb">term</th><th class="wb">Begin SnapID</th><th class="wb">Begin Time</th><th class="wb">END SnapID</th><th class="wb">End Time</th></tr>' || CHR(10) ||
  '<tr><td class="ac">term1</td>' || 
    '<td class="ac">' || &term1_begin || '</td>' ||
    '<td class="ac">' || (select to_char(END_INTERVAL_TIME,'YYYY-MM-DD HH24:MI:SS') from dba_hist_snapshot where snap_id = &term1_begin and instance_number = 1) || '</td>' ||
    '<td class="ac">' || &term1_end || '</td>' ||
    '<td class="ac">' || (select to_char(END_INTERVAL_TIME,'YYYY-MM-DD HH24:MI:SS') from dba_hist_snapshot where snap_id = &term1_end and instance_number = 1) || '</td>' ||
  '</tr>' || CHR(10) ||
  '<tr><td class="ac">term2</td>' || 
    '<td class="ac">' || &term2_begin || '</td>' ||
    '<td class="ac">' || (select to_char(END_INTERVAL_TIME,'YYYY-MM-DD HH24:MI:SS') from dba_hist_snapshot where snap_id = &term2_begin and instance_number = 1) || '</td>' ||
    '<td class="ac">' || &term2_end || '</td>' ||
    '<td class="ac">' || (select to_char(END_INTERVAL_TIME,'YYYY-MM-DD HH24:MI:SS') from dba_hist_snapshot where snap_id = &term2_end and instance_number = 1) || '</td>' ||
  '</tr>' || CHR(10) ||
  '</table>' || CHR(10)
from dual
union all
select
  '<h4>legends of AWR SQLDIFF results</h4>' || CHR(10) ||
  '<table border="1">' || CHR(10) ||
  '<tr><td class="ae">legends</td><td class="ae090">top 10%</td><td class="ae080">top 20%</td><td class="ae070">top 30%</td><td class="ae060">top 40%</td><td class="ae050">top 50%</td><td class="ae040">top 60%</td><td class="ae030">top 70%</td><td class="ae020">top 80%</td><td class="ae010">top 90%</td></tr>'  || CHR(10) ||
  '<tr><td class="ae"></td><td class="ae090">' || p090 || '</td><td class="ae080">' || p080 || '</td><td class="ae070">' || p070 || '</td><td class="ae060">' || p060 || '</td><td class="ae050">' || p050 || '</td><td class="ae040">' || p040 || '</td><td class="ae030">' || p030 || '</td><td class="ae020">' || p020 || '</td><td class="ae010">' || p010 || '</td></tr>'  || CHR(10) ||
  '</table>' || CHR(10)
from percentile
union all
select
  '<h3>AWR SQLDIFF results(term2 / term1)</h3>' || CHR(10) ||
  '<table border="1">' || CHR(10) ||
  '<tr><th class="wb">SQL_ID</th><th class="wb">AVG_ELAPSED_TIME_T1</th><th class="wb">AVG_ELAPSED_TIME_T2</th><th class="wb">ELAPSED_TIME</th><th class="wb">CPU_TIME</th><th class="wb">BUFFER_GETS</th><th class="wb">IOWAIT</th><th class="wb">CLWAIT</th><th class="wb">APWAIT</th><th class="wb">CCWAIT</th><th class="wb">PLSEXEC_TIME</th><th class="wb">JAVEXEC_TIME</th><th class="wb">ROWS_PROCESSED</th><th class="wb">FETCHES</th><th class="wb">SORTS</th><th class="wb">DISK_READS</th><th class="wb">DIRECT_WRITES</th><th class="wb">PHYSICAL_READ_REQUESTS</th><th class="wb">PHYSICAL_READ_BYTES</th><th class="wb">PHYSICAL_WRITE_REQUESTS</th><th class="wb">PHYSICAL_WRITE_BYTES</th><th class="wb">OPTIMIZED_PHYSICAL_READS</th><th class="wb">CELL_UNCOMPRESSED_BYTES</th><th class="wb">IO_OFFLOAD_RETURN_BYTES</th><th class="wb">IO_OFFLOAD_ELIG_BYTES</th><th class="wb">IO_INTERCONNECT_BYTES</th></tr>' || CHR(10)
from dual
union all
select
  '<tr><td class="ac">' || SQL_ID || '</td>' ||
  '<td class="ae">' || AVG_ELAPSED_TIME_T1 || '</td>' ||
  '<td class="ae">' || AVG_ELAPSED_TIME_T2 || '</td>' ||
  '<td class="ae' || 
    CASE WHEN ELAPSED_TIME >= p090 THEN '090'
         WHEN ELAPSED_TIME >= p080 THEN '080'
         WHEN ELAPSED_TIME >= p070 THEN '070'
         WHEN ELAPSED_TIME >= p060 THEN '060'
         WHEN ELAPSED_TIME >= p050 THEN '050'
         WHEN ELAPSED_TIME >= p040 THEN '040'
         WHEN ELAPSED_TIME >= p030 THEN '030'
         WHEN ELAPSED_TIME >= p020 THEN '020'
         WHEN ELAPSED_TIME >= p010 THEN '010'
    ELSE null END
  || '">' || CASE WHEN ELAPSED_TIME IS NULL THEN null ELSE ELAPSED_TIME || '%' END || '</td>' ||
  '<td class="ae' || 
    CASE WHEN CPU_TIME >= p090 THEN '090'
         WHEN CPU_TIME >= p080 THEN '080'
         WHEN CPU_TIME >= p070 THEN '070'
         WHEN CPU_TIME >= p060 THEN '060'
         WHEN CPU_TIME >= p050 THEN '050'
         WHEN CPU_TIME >= p040 THEN '040'
         WHEN CPU_TIME >= p030 THEN '030'
         WHEN CPU_TIME >= p020 THEN '020'
         WHEN CPU_TIME >= p010 THEN '010'
    ELSE null END
  || '">' || CASE WHEN CPU_TIME IS NULL THEN null ELSE CPU_TIME || '%' END || '</td>' ||
  '<td class="ae' || 
    CASE WHEN BUFFER_GETS >= p090 THEN '090'
         WHEN BUFFER_GETS >= p080 THEN '080'
         WHEN BUFFER_GETS >= p070 THEN '070'
         WHEN BUFFER_GETS >= p060 THEN '060'
         WHEN BUFFER_GETS >= p050 THEN '050'
         WHEN BUFFER_GETS >= p040 THEN '040'
         WHEN BUFFER_GETS >= p030 THEN '030'
         WHEN BUFFER_GETS >= p020 THEN '020'
         WHEN BUFFER_GETS >= p010 THEN '010'
    ELSE null END
  || '">' || CASE WHEN BUFFER_GETS IS NULL THEN null ELSE BUFFER_GETS  || '%' END || '</td>' ||
  '<td class="ae' || 
    CASE WHEN IOWAIT >= p090 THEN '090'
         WHEN IOWAIT >= p080 THEN '080'
         WHEN IOWAIT >= p070 THEN '070'
         WHEN IOWAIT >= p060 THEN '060'
         WHEN IOWAIT >= p050 THEN '050'
         WHEN IOWAIT >= p040 THEN '040'
         WHEN IOWAIT >= p030 THEN '030'
         WHEN IOWAIT >= p020 THEN '020'
         WHEN IOWAIT >= p010 THEN '010'
    ELSE null END
  || '">' || CASE WHEN IOWAIT IS NULL THEN null ELSE IOWAIT || '%' END || '</td>' ||
  '<td class="ae' || 
    CASE WHEN CLWAIT >= p090 THEN '090'
         WHEN CLWAIT >= p080 THEN '080'
         WHEN CLWAIT >= p070 THEN '070'
         WHEN CLWAIT >= p060 THEN '060'
         WHEN CLWAIT >= p050 THEN '050'
         WHEN CLWAIT >= p040 THEN '040'
         WHEN CLWAIT >= p030 THEN '030'
         WHEN CLWAIT >= p020 THEN '020'
         WHEN CLWAIT >= p010 THEN '010'
    ELSE null END
  || '">' || CASE WHEN CLWAIT IS NULL THEN null ELSE CLWAIT || '%' END || '</td>' ||
  '<td class="ae' || 
    CASE WHEN APWAIT >= p090 THEN '090'
         WHEN APWAIT >= p080 THEN '080'
         WHEN APWAIT >= p070 THEN '070'
         WHEN APWAIT >= p060 THEN '060'
         WHEN APWAIT >= p050 THEN '050'
         WHEN APWAIT >= p040 THEN '040'
         WHEN APWAIT >= p030 THEN '030'
         WHEN APWAIT >= p020 THEN '020'
         WHEN APWAIT >= p010 THEN '010'
    ELSE null END
  || '">' || CASE WHEN APWAIT IS NULL THEN null ELSE APWAIT || '%' END || '</td>' ||
  '<td class="ae' || 
    CASE WHEN CCWAIT >= p090 THEN '090'
         WHEN CCWAIT >= p080 THEN '080'
         WHEN CCWAIT >= p070 THEN '070'
         WHEN CCWAIT >= p060 THEN '060'
         WHEN CCWAIT >= p050 THEN '050'
         WHEN CCWAIT >= p040 THEN '040'
         WHEN CCWAIT >= p030 THEN '030'
         WHEN CCWAIT >= p020 THEN '020'
         WHEN CCWAIT >= p010 THEN '010'
    ELSE null END
  || '">' || CASE WHEN CCWAIT IS NULL THEN null ELSE CCWAIT || '%' END || '</td>' ||
  '<td class="ae' || 
    CASE WHEN PLSEXEC_TIME >= p090 THEN '090'
         WHEN PLSEXEC_TIME >= p080 THEN '080'
         WHEN PLSEXEC_TIME >= p070 THEN '070'
         WHEN PLSEXEC_TIME >= p060 THEN '060'
         WHEN PLSEXEC_TIME >= p050 THEN '050'
         WHEN PLSEXEC_TIME >= p040 THEN '040'
         WHEN PLSEXEC_TIME >= p030 THEN '030'
         WHEN PLSEXEC_TIME >= p020 THEN '020'
         WHEN PLSEXEC_TIME >= p010 THEN '010'
    ELSE null END
  || '">' || CASE WHEN PLSEXEC_TIME IS NULL THEN null ELSE PLSEXEC_TIME || '%' END || '</td>' ||
  '<td class="ae' || 
    CASE WHEN JAVEXEC_TIME >= p090 THEN '090'
         WHEN JAVEXEC_TIME >= p080 THEN '080'
         WHEN JAVEXEC_TIME >= p070 THEN '070'
         WHEN JAVEXEC_TIME >= p060 THEN '060'
         WHEN JAVEXEC_TIME >= p050 THEN '050'
         WHEN JAVEXEC_TIME >= p040 THEN '040'
         WHEN JAVEXEC_TIME >= p030 THEN '030'
         WHEN JAVEXEC_TIME >= p020 THEN '020'
         WHEN JAVEXEC_TIME >= p010 THEN '010'
    ELSE null END
  || '">' || CASE WHEN JAVEXEC_TIME IS NULL THEN null ELSE JAVEXEC_TIME || '%' END || '</td>' ||
  '<td class="ae' || 
    CASE WHEN ROWS_PROCESSED >= p090 THEN '090'
         WHEN ROWS_PROCESSED >= p080 THEN '080'
         WHEN ROWS_PROCESSED >= p070 THEN '070'
         WHEN ROWS_PROCESSED >= p060 THEN '060'
         WHEN ROWS_PROCESSED >= p050 THEN '050'
         WHEN ROWS_PROCESSED >= p040 THEN '040'
         WHEN ROWS_PROCESSED >= p030 THEN '030'
         WHEN ROWS_PROCESSED >= p020 THEN '020'
         WHEN ROWS_PROCESSED >= p010 THEN '010'
    ELSE null END
  || '">' || CASE WHEN ROWS_PROCESSED IS NULL THEN null ELSE ROWS_PROCESSED || '%' END || '</td>' ||
  '<td class="ae' || 
    CASE WHEN FETCHES >= p090 THEN '090'
         WHEN FETCHES >= p080 THEN '080'
         WHEN FETCHES >= p070 THEN '070'
         WHEN FETCHES >= p060 THEN '060'
         WHEN FETCHES >= p050 THEN '050'
         WHEN FETCHES >= p040 THEN '040'
         WHEN FETCHES >= p030 THEN '030'
         WHEN FETCHES >= p020 THEN '020'
         WHEN FETCHES >= p010 THEN '010'
    ELSE null END
  || '">' || CASE WHEN FETCHES IS NULL THEN null ELSE FETCHES || '%' END || '</td>' ||
  '<td class="ae' || 
    CASE WHEN SORTS >= p090 THEN '090'
         WHEN SORTS >= p080 THEN '080'
         WHEN SORTS >= p070 THEN '070'
         WHEN SORTS >= p060 THEN '060'
         WHEN SORTS >= p050 THEN '050'
         WHEN SORTS >= p040 THEN '040'
         WHEN SORTS >= p030 THEN '030'
         WHEN SORTS >= p020 THEN '020'
         WHEN SORTS >= p010 THEN '010'
    ELSE null END
  || '">' || CASE WHEN SORTS IS NULL THEN null ELSE SORTS || '%' END || '</td>' ||
  '<td class="ae' || 
    CASE WHEN DISK_READS >= p090 THEN '090'
         WHEN DISK_READS >= p080 THEN '080'
         WHEN DISK_READS >= p070 THEN '070'
         WHEN DISK_READS >= p060 THEN '060'
         WHEN DISK_READS >= p050 THEN '050'
         WHEN DISK_READS >= p040 THEN '040'
         WHEN DISK_READS >= p030 THEN '030'
         WHEN DISK_READS >= p020 THEN '020'
         WHEN DISK_READS >= p010 THEN '010'
    ELSE null END
  || '">' || CASE WHEN DISK_READS IS NULL THEN null ELSE DISK_READS || '%' END || '</td>' ||
  '<td class="ae' || 
    CASE WHEN DIRECT_WRITES >= p090 THEN '090'
         WHEN DIRECT_WRITES >= p080 THEN '080'
         WHEN DIRECT_WRITES >= p070 THEN '070'
         WHEN DIRECT_WRITES >= p060 THEN '060'
         WHEN DIRECT_WRITES >= p050 THEN '050'
         WHEN DIRECT_WRITES >= p040 THEN '040'
         WHEN DIRECT_WRITES >= p030 THEN '030'
         WHEN DIRECT_WRITES >= p020 THEN '020'
         WHEN DIRECT_WRITES >= p010 THEN '010'
    ELSE null END
  || '">' || CASE WHEN DIRECT_WRITES IS NULL THEN null ELSE DIRECT_WRITES || '%' END || '</td>' ||
  '<td class="ae' || 
    CASE WHEN PHYSICAL_READ_REQUESTS >= p090 THEN '090'
         WHEN PHYSICAL_READ_REQUESTS >= p080 THEN '080'
         WHEN PHYSICAL_READ_REQUESTS >= p070 THEN '070'
         WHEN PHYSICAL_READ_REQUESTS >= p060 THEN '060'
         WHEN PHYSICAL_READ_REQUESTS >= p050 THEN '050'
         WHEN PHYSICAL_READ_REQUESTS >= p040 THEN '040'
         WHEN PHYSICAL_READ_REQUESTS >= p030 THEN '030'
         WHEN PHYSICAL_READ_REQUESTS >= p020 THEN '020'
         WHEN PHYSICAL_READ_REQUESTS >= p010 THEN '010'
    ELSE null END
  || '">' || CASE WHEN PHYSICAL_READ_REQUESTS IS NULL THEN null ELSE PHYSICAL_READ_REQUESTS || '%' END || '</td>' ||
  '<td class="ae' || 
    CASE WHEN PHYSICAL_READ_BYTES >= p090 THEN '090'
         WHEN PHYSICAL_READ_BYTES >= p080 THEN '080'
         WHEN PHYSICAL_READ_BYTES >= p070 THEN '070'
         WHEN PHYSICAL_READ_BYTES >= p060 THEN '060'
         WHEN PHYSICAL_READ_BYTES >= p050 THEN '050'
         WHEN PHYSICAL_READ_BYTES >= p040 THEN '040'
         WHEN PHYSICAL_READ_BYTES >= p030 THEN '030'
         WHEN PHYSICAL_READ_BYTES >= p020 THEN '020'
         WHEN PHYSICAL_READ_BYTES >= p010 THEN '010'
    ELSE null END
  || '">' || CASE WHEN PHYSICAL_READ_BYTES IS NULL THEN null ELSE PHYSICAL_READ_BYTES || '%' END || '</td>' ||
  '<td class="ae' || 
    CASE WHEN PHYSICAL_WRITE_REQUESTS >= p090 THEN '090'
         WHEN PHYSICAL_WRITE_REQUESTS >= p080 THEN '080'
         WHEN PHYSICAL_WRITE_REQUESTS >= p070 THEN '070'
         WHEN PHYSICAL_WRITE_REQUESTS >= p060 THEN '060'
         WHEN PHYSICAL_WRITE_REQUESTS >= p050 THEN '050'
         WHEN PHYSICAL_WRITE_REQUESTS >= p040 THEN '040'
         WHEN PHYSICAL_WRITE_REQUESTS >= p030 THEN '030'
         WHEN PHYSICAL_WRITE_REQUESTS >= p020 THEN '020'
         WHEN PHYSICAL_WRITE_REQUESTS >= p010 THEN '010'
    ELSE null END
  || '">' || CASE WHEN PHYSICAL_WRITE_REQUESTS IS NULL THEN null ELSE PHYSICAL_WRITE_REQUESTS || '%' END || '</td>' ||
  '<td class="ae' || 
    CASE WHEN PHYSICAL_WRITE_BYTES >= p090 THEN '090'
         WHEN PHYSICAL_WRITE_BYTES >= p080 THEN '080'
         WHEN PHYSICAL_WRITE_BYTES >= p070 THEN '070'
         WHEN PHYSICAL_WRITE_BYTES >= p060 THEN '060'
         WHEN PHYSICAL_WRITE_BYTES >= p050 THEN '050'
         WHEN PHYSICAL_WRITE_BYTES >= p040 THEN '040'
         WHEN PHYSICAL_WRITE_BYTES >= p030 THEN '030'
         WHEN PHYSICAL_WRITE_BYTES >= p020 THEN '020'
         WHEN PHYSICAL_WRITE_BYTES >= p010 THEN '010'
    ELSE null END
  || '">' || CASE WHEN PHYSICAL_WRITE_BYTES IS NULL THEN null ELSE PHYSICAL_WRITE_BYTES || '%' END || '</td>' ||
  '<td class="ae' || 
    CASE WHEN OPTIMIZED_PHYSICAL_READS >= p090 THEN '090'
         WHEN OPTIMIZED_PHYSICAL_READS >= p080 THEN '080'
         WHEN OPTIMIZED_PHYSICAL_READS >= p070 THEN '070'
         WHEN OPTIMIZED_PHYSICAL_READS >= p060 THEN '060'
         WHEN OPTIMIZED_PHYSICAL_READS >= p050 THEN '050'
         WHEN OPTIMIZED_PHYSICAL_READS >= p040 THEN '040'
         WHEN OPTIMIZED_PHYSICAL_READS >= p030 THEN '030'
         WHEN OPTIMIZED_PHYSICAL_READS >= p020 THEN '020'
         WHEN OPTIMIZED_PHYSICAL_READS >= p010 THEN '010'
    ELSE null END
  || '">' || CASE WHEN OPTIMIZED_PHYSICAL_READS IS NULL THEN null ELSE OPTIMIZED_PHYSICAL_READS || '%' END || '</td>' ||
  '<td class="ae' || 
    CASE WHEN CELL_UNCOMPRESSED_BYTES >= p090 THEN '090'
         WHEN CELL_UNCOMPRESSED_BYTES >= p080 THEN '080'
         WHEN CELL_UNCOMPRESSED_BYTES >= p070 THEN '070'
         WHEN CELL_UNCOMPRESSED_BYTES >= p060 THEN '060'
         WHEN CELL_UNCOMPRESSED_BYTES >= p050 THEN '050'
         WHEN CELL_UNCOMPRESSED_BYTES >= p040 THEN '040'
         WHEN CELL_UNCOMPRESSED_BYTES >= p030 THEN '030'
         WHEN CELL_UNCOMPRESSED_BYTES >= p020 THEN '020'
         WHEN CELL_UNCOMPRESSED_BYTES >= p010 THEN '010'
    ELSE null END
  || '">' || CASE WHEN CELL_UNCOMPRESSED_BYTES IS NULL THEN null ELSE CELL_UNCOMPRESSED_BYTES || '%' END || '</td>' ||
  '<td class="ae' || 
    CASE WHEN IO_OFFLOAD_RETURN_BYTES >= p090 THEN '090'
         WHEN IO_OFFLOAD_RETURN_BYTES >= p080 THEN '080'
         WHEN IO_OFFLOAD_RETURN_BYTES >= p070 THEN '070'
         WHEN IO_OFFLOAD_RETURN_BYTES >= p060 THEN '060'
         WHEN IO_OFFLOAD_RETURN_BYTES >= p050 THEN '050'
         WHEN IO_OFFLOAD_RETURN_BYTES >= p040 THEN '040'
         WHEN IO_OFFLOAD_RETURN_BYTES >= p030 THEN '030'
         WHEN IO_OFFLOAD_RETURN_BYTES >= p020 THEN '020'
         WHEN IO_OFFLOAD_RETURN_BYTES >= p010 THEN '010'
    ELSE null END
  || '">' || CASE WHEN IO_OFFLOAD_RETURN_BYTES IS NULL THEN null ELSE IO_OFFLOAD_RETURN_BYTES || '%' END || '</td>' ||
  '<td class="ae' || 
    CASE WHEN IO_OFFLOAD_ELIG_BYTES >= p090 THEN '090'
         WHEN IO_OFFLOAD_ELIG_BYTES >= p080 THEN '080'
         WHEN IO_OFFLOAD_ELIG_BYTES >= p070 THEN '070'
         WHEN IO_OFFLOAD_ELIG_BYTES >= p060 THEN '060'
         WHEN IO_OFFLOAD_ELIG_BYTES >= p050 THEN '050'
         WHEN IO_OFFLOAD_ELIG_BYTES >= p040 THEN '040'
         WHEN IO_OFFLOAD_ELIG_BYTES >= p030 THEN '030'
         WHEN IO_OFFLOAD_ELIG_BYTES >= p020 THEN '020'
         WHEN IO_OFFLOAD_ELIG_BYTES >= p010 THEN '010'
    ELSE null END
  || '">' || CASE WHEN IO_OFFLOAD_ELIG_BYTES IS NULL THEN null ELSE IO_OFFLOAD_ELIG_BYTES || '%' END || '</td>' ||
  '<td class="ae' || 
    CASE WHEN IO_INTERCONNECT_BYTES >= p090 THEN '090'
         WHEN IO_INTERCONNECT_BYTES >= p080 THEN '080'
         WHEN IO_INTERCONNECT_BYTES >= p070 THEN '070'
         WHEN IO_INTERCONNECT_BYTES >= p060 THEN '060'
         WHEN IO_INTERCONNECT_BYTES >= p050 THEN '050'
         WHEN IO_INTERCONNECT_BYTES >= p040 THEN '040'
         WHEN IO_INTERCONNECT_BYTES >= p030 THEN '030'
         WHEN IO_INTERCONNECT_BYTES >= p020 THEN '020'
         WHEN IO_INTERCONNECT_BYTES >= p010 THEN '010'
    ELSE null END
  || '">' || CASE WHEN IO_INTERCONNECT_BYTES IS NULL THEN null ELSE IO_INTERCONNECT_BYTES || '%' END || '</td>' ||
  '</tr>' || CHR(10)
from result, percentile
union all
select
  '</table>' || CHR(10) ||
  '<h4>SQL_ID and SQL_TEXT</h4>' || CHR(10) ||
  '<table>' || CHR(10) ||
  '<tr><th class="wb">SQL_ID</th><th class="wb">SQL_TEXT</th></tr>' || CHR(10)
from dual
;
WITH t1 AS (
  select
    distinct SQL_ID
  from dba_hist_sqlstat
  where snap_id >= &term1_begin
    and snap_id <= &term1_end
),
t2 AS (
  select
    distinct SQL_ID
  from dba_hist_sqlstat
  where snap_id >= &term2_begin
    and snap_id <= &term2_end
),
result AS (
  select
    t1.sql_id  
  from t1, t2
  where t1.sql_id = t2.sql_id
  order by sql_id
)
select
  xmlelement("tr", 
    xmlconcat(
      xmlelement("td", XMLATTRIBUTES('ac' as class), sql_id),
      xmlelement("td", XMLATTRIBUTES('ac' as class), sql_text)
    )
  )
from
  ( select sql_id, ( select XMLAGG( XMLELEMENT(e, sql_text).EXTRACT('//text()') ).GetClobVal() from dba_hist_sqltext where sql_id = r.SQL_ID ) as sql_text from result r )
;
select
  '</table>' || CHR(10) ||
  '</body>' || CHR(10) ||
  '</html>'
from dual;
spool off
