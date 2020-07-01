define term1_begin = 168
define term1_end   = 268
define term2_begin = 1390
define term2_end   = 1400

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
  from
    dba_hist_sqlstat
  where snap_id >= &term1_begin
    and snap_id <= &term1_end
  group by sql_id, plan_hash_value
), t2 AS (
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
  from
    dba_hist_sqlstat
  where snap_id >= &term2_begin
    and snap_id <= &term2_end
  group by sql_id, plan_hash_value
), result AS (
  select
    t1.sql_id
    , t1.ELAPSED_TIME / t1.EXECUTIONS as avg_elapsed_time_t1
    , t2.ELAPSED_TIME / t2.EXECUTIONS as avg_elapsed_time_t2
    , (t2.ELAPSED_TIME             / t2.EXECUTIONS ) / nullif( t1.ELAPSED_TIME             / t1.EXECUTIONS , 0) as ELAPSED_TIME            
    , (t2.CPU_TIME                 / t2.EXECUTIONS ) / nullif( t1.CPU_TIME                 / t1.EXECUTIONS , 0) as CPU_TIME                
    , (t2.BUFFER_GETS              / t2.EXECUTIONS ) / nullif( t1.BUFFER_GETS              / t1.EXECUTIONS , 0) as BUFFER_GETS             
    , (t2.IOWAIT                   / t2.EXECUTIONS ) / nullif( t1.IOWAIT                   / t1.EXECUTIONS , 0) as IOWAIT                  
    , (t2.CLWAIT                   / t2.EXECUTIONS ) / nullif( t1.CLWAIT                   / t1.EXECUTIONS , 0) as CLWAIT                  
    , (t2.APWAIT                   / t2.EXECUTIONS ) / nullif( t1.APWAIT                   / t1.EXECUTIONS , 0) as APWAIT                  
    , (t2.CCWAIT                   / t2.EXECUTIONS ) / nullif( t1.CCWAIT                   / t1.EXECUTIONS , 0) as CCWAIT                  
    , (t2.PLSEXEC_TIME             / t2.EXECUTIONS ) / nullif( t1.PLSEXEC_TIME             / t1.EXECUTIONS , 0) as PLSEXEC_TIME            
    , (t2.JAVEXEC_TIME             / t2.EXECUTIONS ) / nullif( t1.JAVEXEC_TIME             / t1.EXECUTIONS , 0) as JAVEXEC_TIME            
    , (t2.ROWS_PROCESSED           / t2.EXECUTIONS ) / nullif( t1.ROWS_PROCESSED           / t1.EXECUTIONS , 0) as ROWS_PROCESSED          
    , (t2.FETCHES                  / t2.EXECUTIONS ) / nullif( t1.FETCHES                  / t1.EXECUTIONS , 0) as FETCHES                 
    , (t2.SORTS                    / t2.EXECUTIONS ) / nullif( t1.SORTS                    / t1.EXECUTIONS , 0) as SORTS                   
    , (t2.DISK_READS               / t2.EXECUTIONS ) / nullif( t1.DISK_READS               / t1.EXECUTIONS , 0) as DISK_READS              
    , (t2.DIRECT_WRITES            / t2.EXECUTIONS ) / nullif( t1.DIRECT_WRITES            / t1.EXECUTIONS , 0) as DIRECT_WRITES           
    , (t2.PHYSICAL_READ_REQUESTS   / t2.EXECUTIONS ) / nullif( t1.PHYSICAL_READ_REQUESTS   / t1.EXECUTIONS , 0) as PHYSICAL_READ_REQUESTS  
    , (t2.PHYSICAL_READ_BYTES      / t2.EXECUTIONS ) / nullif( t1.PHYSICAL_READ_BYTES      / t1.EXECUTIONS , 0) as PHYSICAL_READ_BYTES     
    , (t2.PHYSICAL_WRITE_REQUESTS  / t2.EXECUTIONS ) / nullif( t1.PHYSICAL_WRITE_REQUESTS  / t1.EXECUTIONS , 0) as PHYSICAL_WRITE_REQUESTS 
    , (t2.PHYSICAL_WRITE_BYTES     / t2.EXECUTIONS ) / nullif( t1.PHYSICAL_WRITE_BYTES     / t1.EXECUTIONS , 0) as PHYSICAL_WRITE_BYTES    
    , (t2.OPTIMIZED_PHYSICAL_READS / t2.EXECUTIONS ) / nullif( t1.OPTIMIZED_PHYSICAL_READS / t1.EXECUTIONS , 0) as OPTIMIZED_PHYSICAL_READS
    , (t2.CELL_UNCOMPRESSED_BYTES  / t2.EXECUTIONS ) / nullif( t1.CELL_UNCOMPRESSED_BYTES  / t1.EXECUTIONS , 0) as CELL_UNCOMPRESSED_BYTES 
    , (t2.IO_OFFLOAD_RETURN_BYTES  / t2.EXECUTIONS ) / nullif( t1.IO_OFFLOAD_RETURN_BYTES  / t1.EXECUTIONS , 0) as IO_OFFLOAD_RETURN_BYTES 
    , (t2.IO_OFFLOAD_ELIG_BYTES    / t2.EXECUTIONS ) / nullif( t1.IO_OFFLOAD_ELIG_BYTES    / t1.EXECUTIONS , 0) as IO_OFFLOAD_ELIG_BYTES   
    , (t2.IO_INTERCONNECT_BYTES    / t2.EXECUTIONS ) / nullif( t1.IO_INTERCONNECT_BYTES    / t1.EXECUTIONS , 0) as IO_INTERCONNECT_BYTES   
  from
    t1, t2
  where t1.sql_id = t2.sql_id
    and t1.plan_hash_value = t2.plan_hash_value
    and t1.EXECUTIONS > 0
    and t2.EXECUTIONS > 0
), percentile as (
  select 
    percentile_cont(0.1) within group (order by rates asc) as p010
    ,percentile_cont(0.2) within group (order by rates asc) as p020
    ,percentile_cont(0.3) within group (order by rates asc) as p030
    ,percentile_cont(0.4) within group (order by rates asc) as p040
    ,percentile_cont(0.5) within group (order by rates asc) as p050
    ,percentile_cont(0.6) within group (order by rates asc) as p060
    ,percentile_cont(0.7) within group (order by rates asc) as p070
    ,percentile_cont(0.8) within group (order by rates asc) as p080
    ,percentile_cont(0.9) within group (order by rates asc) as p090
    ,percentile_cont(1.0) within group (order by rates asc) as p100
  from result
  unpivot(
    rates for cols in (
      ELAPSED_TIME            
      , CPU_TIME                
      , BUFFER_GETS             
      , IOWAIT                  
      , CLWAIT                  
      , APWAIT                  
      , CCWAIT                  
      , PLSEXEC_TIME            
      , JAVEXEC_TIME            
      , ROWS_PROCESSED          
      , FETCHES                 
      , SORTS                   
      , DISK_READS              
      , DIRECT_WRITES           
      , PHYSICAL_READ_REQUESTS  
      , PHYSICAL_READ_BYTES     
      , PHYSICAL_WRITE_REQUESTS 
      , PHYSICAL_WRITE_BYTES    
      , OPTIMIZED_PHYSICAL_READS
      , CELL_UNCOMPRESSED_BYTES 
      , IO_OFFLOAD_RETURN_BYTES 
      , IO_OFFLOAD_ELIG_BYTES   
      , IO_INTERCONNECT_BYTES   
    )
  )
)
select * from result;
