#!/bin/bash 

function killtree()
{
    local parent=$1 child
    for child in $(ps -o ppid= -o pid= | awk "\$1==$parent {print \$2}"); do
        killtree $child
    done
    kill $parent > /dev/null 2>&1
}

function handle_shutdown()
{
    killtree $1
    cleanup $2
    echo ""
    echo "Script was terminated abnormally. Finished cleaning up.. "
}

function handle_shutdown1()
{
    echo "Script was terminated abnormally. Finished cleaning up.. "
    exit 1
}

function cleanup()
{
    if [ -n "$1" ]; then
        rm -rf $1/*.log
        rm -rf $1/*.txt
        rm -rf $1/*.sql
        rm -rf $1/*.properties
        rm -rf $1/*.out
        rm -rf $1/*.res
        rm -rf $1/*.dat
        rm -rf $1/*.rrn
        rm -rf $1/*.tpl
        rm -rf $1/*.lst
        rm -rf $1/*.err
        rm -rf $1/README
    fi
}

function validate_querynum()
{
   re='^[0-9]+$'
   if ! [[ $1 =~ $re ]] ; then
       return 1
   fi
   if [[ $1 -le 0 || $1 -gt 99 ]] ; then
       return 1
   fi
   return 0
}

function cleanup_spark()
{
    logInfo "Clean up spark structures"
    rm -rf ${SPARK_HOME}/metastore_db ${SPARK_HOME}/spark-warehouse/tpcds.db
    logInfo "Clean up spark successful..."
}

function cleanup_all()
{
    cleanup ${TPCDS_WORK_DIR}
    cleanup ${TPCDS_LOG_DIR}
    logInfo "Cleanup successful.."
}

function check_createtables() 
{
    result=$?
    if [ "$result" -ne 0 ]; then
        return 1 
    fi
  
    cd $SPARK_HOME
    DRIVER_OPTIONS="--driver-memory 4g --driver-java-options -Dlog4j.configuration=file:///${output_dir}/log4j.properties"
    EXECUTOR_OPTIONS="--executor-memory 2g --conf spark.executor.extraJavaOptions=-Dlog4j.configuration=file:///${output_dir}/log4j.properties"
    logInfo "Checking pre-reqs for running TPC-DS queries. May take a few seconds.."
    bin/spark-sql ${DRIVER_OPTIONS} ${EXECUTOR_OPTIONS} --conf spark.sql.catalogImplementation=hive -f ${TPCDS_WORK_DIR}/row_counts.sql > ${TPCDS_WORK_DIR}/rowcounts.out 2> ${TPCDS_WORK_DIR}/rowcounts.err
    cat ${TPCDS_WORK_DIR}/rowcounts.out | grep -v "Time" | grep -v "SLF4J" >> ${TPCDS_WORK_DIR}/rowcounts.rrn
    file1=${TPCDS_WORK_DIR}/rowcounts.rrn
    if [ -f ${TPCDS_GENDATA_DIR}/../rowcounts.expected ] ; then
        file2=${TPCDS_GENDATA_DIR}/../rowcounts.expected
    else
        file2=${TPCDS_ROOT_DIR}/src/ddl/rowcounts.expected
    fi
    if cmp -s "$file1" "$file2"
    then
      logInfo "Checking pre-reqs for running TPC-DS queries is successful."
      return 0 
    else
        logError "The rowcounts for TPC-DS tables are not correct. Please make sure that tables "
        echo     "are successfully run before continuing with the test execution"
        return 1
    fi
}

function logDebug() 
{
    if [ ! -z "${DEBUG_SCRIPT}" ] ;
    then
        echo "DEBUG: $1"
    fi
}

function logInfo() 
{
    echo "INFO: $1"
}

function logError() 
{
    echo "ERROR: $1"
}

function set_environment() 
{
    bin_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    script_dir="$(dirname "$bin_dir")"
  
    if [ -z "${TPCDS_ROOT_DIR}" ]; then
        TPCDS_ROOT_DIR=${script_dir}
    fi  
    if [ -z "$TPCDS_DBNAME" ]; then
        TPCDS_DBNAME="TPCDS"
    fi  
}

function check_environment() 
{
   if [ -z "${SPARK_HOME}" ]; then
      logError "SPARK_HOME is not set. Please make sure the following conditions are met."
      logError "1. Set SPARK_HOME and make sure it points to a valid spark installation."
      logError "2. The userid running the script has permission to execute spark shell."
      exit 1
   fi  
   if [ -z "${TPCDS_LOG_DIR}" ]  || [ -z "${TPCDS_WORK_DIR}" ] \
      || [ -z "${TPCDS_GENDATA_DIR}" ] || [ -z "${TPCDS_GENQUERIES_DIR}" ] ; then
      logError "One of the follow set of variables has not been set.  Set them and rerun the script" 
      logError "  TPCDS_GENDATA_DIR    = $TPCDS_GENDATA_DIR"
      logError "  TPCDS_GENQUERIES_DIR = $TPCDS_GENQUERIES_DIR"
      logError "  TPCDS_LOG_DIR        = $TPCDS_LOG_DIR"
      logError "  TPCDS_WORK_DIR       = ${TPCDS_WORK_DIR}"
      exit 1
   fi
}

function template()
{
    # usage: template file.tpl
    while read -r line ; do
            line=${line//\"/\\\"}
            line=${line//\`/\\\`}
            line=${line//\$/\\\$}
            line=${line//\\\$\{/\${}
            eval "echo \"$line\""; 
    done < ${1}
}

function ProgressBar()
{
  # Process data
    let _progress=(${1}*100/${2}*100)/100
    let _done=(${_progress}*4)/10
    let _left=40-$_done
  # Build progressbar string lengths
    _fill=$(printf "%${_done}s")
    _empty=$(printf "%${_left}s")

  # 1.2 Build progressbar strings and print the ProgressBar line
  # 1.2.1 Output example:
  # 1.2.1.1 Progress : [########################################] 100%
  printf "\rINFO: Progress : [${_fill// /\#}${_empty// /-}] ${_progress}%%"
}

function track_progress()
{
   ROOT_DIR=${1}
   total_results=$(( TPCDS_NUM_STREAMS * NUM_QUERIES ))
   while true ; do
       progress=`find ${ROOT_DIR} -name "*.res" | wc -l`
       ProgressBar ${progress} ${NUM_QUERIES}
   done
}

function run_tpcds_throughput_common()
{
    local output_dir=${1}
    cp ${2}/*.sql ${output_dir}

    ${TPCDS_ROOT_DIR}/bin/run_throughput_queries.sh ${SPARK_HOME} ${output_dir}  \
         > ${output_dir}/runqueries.out 2>&1 
    local error_code=0
    # The throughput query is one large conglomeration
    # Add 1 to 1 queries to signal the end of the run to progress bar
    NUM_QUERIES=2
    while true ; do
        local progress=`find ${TPCDS_WORK_DIR} -name "*.res" | wc -l`
        # ProgressBar ${progress} ${NUM_QUERIES}

        ps -p ${script_pid} > /dev/null 
        if [ $? == 1 ]; then
            local error_code=1
        fi
        if [ "$error_code" -gt 0 ] || [ "$progress" -ge $NUM_QUERIES ] ; then 
            echo ${progress}, ${error_code}
            break
        fi
        sleep 0.1
    done 
    progress=`find ${output_dir} -name "*.res" | wc -l`
   
    if [ "${progress}" -lt $NUM_QUERIES ] ; then 
        echo ""
        logError "Failed to run TPCDS queries. Please look at ${output_dir}/runqueries.out for error details" 
    else
        echo ""
        logInfo "TPCDS queries ran successfully. Below are the result details"
        logInfo "Overall result file: ${output_dir}/query_0.res"
        logInfo "Summary file: ${output_dir}/run_summary.txt"
    fi
}

function run_tpcds_common() 
{
    output_dir=${TPCDS_WORK_DIR}
    cp ${TPCDS_GENQUERIES_DIR}/*.sql ${TPCDS_WORK_DIR}

    ${TPCDS_ROOT_DIR}/bin/runqueries.sh ${SPARK_HOME} ${TPCDS_WORK_DIR}  > ${TPCDS_WORK_DIR}/runqueries.out 2>&1 &
    script_pid=$!
    trap 'handle_shutdown $$ $output_dir; exit' SIGHUP SIGQUIT SIGINT SIGTERM
    error_code=0
    while true ; do
        progress=`find ${TPCDS_WORK_DIR} -name "*.res" | wc -l`
        ProgressBar ${progress} ${NUM_QUERIES}

        ps -p $script_pid > /dev/null 
        if [ $? == 1 ]; then
            error_code=1
        fi
        if [ "$error_code" -gt 0 ] || [ "$progress" -ge $NUM_QUERIES ] ; then 
            break
        fi
        sleep 0.1
    done 
    progress=`find ${TPCDS_WORK_DIR} -name "*.res" | wc -l`
   
    if [ "$progress" -lt $NUM_QUERIES ] ; then 
        echo ""
        logError "Failed to run TPCDS queries. Please look at ${TPCDS_WORK_DIR}/runqueries.out for error details" 
    else
        echo ""
        logInfo "TPCDS queries ran successfully. Below are the result details"
        logInfo "Individual result files: ${TPCDS_WORK_DIR}/query<number>.res"
        logInfo "Summary file: ${TPCDS_WORK_DIR}/run_summary.txt"
    fi 
}

function run_tpcds_queries()
{
    output_dir=${TPCDS_WORK_DIR}
    cleanup ${output_dir}
    touch ${output_dir}/runlist.txt
    for i in `seq 1 99`
    do
        echo "$i" >> ${output_dir}/runlist.txt
    done
    for i in `ls ${TPCDS_ROOT_DIR}/src/properties/*`
    do
        baseName="$(basename $i)"
        template $i > ${output_dir}/$baseName
    done 
    for i in `ls ${TPCDS_ROOT_DIR}/src/ddl/*.sql`
    do
        baseName="$(basename $i)"
        template $i > ${output_dir}/$baseName
    done 
    # 1 add to 99 queries to signal the end of the run to progress bar
    NUM_QUERIES=100
    logInfo "Running TPCDS queries. Will take a few hours.. "
    run_tpcds_common
}

function setup_throughput_env()
{
    local orig_TPCDS_LOG_DIR=${TPCDS_LOG_DIR}
    for i in `seq 0 ${TPCDS_NUM_STREAMS}` ;
    do
        local output_dir=${TPCDS_WORK_DIR}/stream${i}
        mkdir -p ${output_dir}
        cleanup ${output_dir}
        TPCDS_LOG_DIR=${output_dir}
        for f in `ls ${TPCDS_ROOT_DIR}/src/properties/*`
        do
            baseName="$(basename $f)"
            template $f > ${output_dir}/$baseName
        done 
        for i in `ls ${TPCDS_ROOT_DIR}/src/ddl/*.sql`
        do
            baseName="$(basename $i)"
            template $i > ${output_dir}/$baseName
        done 
    done
    TPCDS_LOG_DIR=${orig_TPCDS_LOG_DIR}
}

function run_tpcds_throughput_queries()
{
    TPCDS_NUM_STREAMS=21
    local TPCDS_MAX_AVAILABLE_STREAMS=21
    setup_throughput_env

    # track_progress ${TPCDS_WORK_DIR}/stream
    logInfo "Running TPCDS throughput queries. Will take a few hours.. "
    for i in `seq 0 ${TPCDS_NUM_STREAMS}`;
    do
        # Take the modulo of the throughput stream.  We have only a fixed number
        # of steam queries generated, thus we need to round robin through them
        local base_stream_index=$(( i % TPCDS_MAX_AVAILABLE_STREAMS ))
        run_tpcds_throughput_common ${TPCDS_WORK_DIR}/stream${i} \
                                    ${TPCDS_STREAM_BASE}${base_stream_index} &
    done
    wait
}

function create_spark_tables()
{
    check_environment
    output_dir=${TPCDS_WORK_DIR}
    cleanup ${TPCDS_WORK_DIR}
    trap 'handle_shutdown $$ $output_dir; exit' SIGHUP SIGQUIT SIGINT SIGTERM
    echo "USE ${TPCDS_DBNAME};" >> ${output_dir}/create_tables_temp.sql
    for i in `ls ${TPCDS_ROOT_DIR}/src/ddl/individual/*.sql`
    do
        cat $i >> ${output_dir}/create_tables_temp.sql
        echo "" >> ${output_dir}/create_tables_temp.sql
    done
    template ${output_dir}/create_tables_temp.sql > ${output_dir}/create_tables_work.sql

    for i in `ls ${TPCDS_ROOT_DIR}/src/ddl/*.sql`
    do
        baseName="$(basename $i)"
        template $i > ${output_dir}/$baseName
    done 
    for i in `ls ${TPCDS_ROOT_DIR}/src/properties/*`
    do
        baseName="$(basename $i)"
        template $i > ${output_dir}/$baseName
    done 
    result=$?
    if [ "$result" -ne 1 ]; then 
        current_dir=`pwd`
        cd $SPARK_HOME
        DRIVER_OPTIONS="--driver-java-options -Dlog4j.configuration=file:///${output_dir}/log4j.properties"
        EXECUTOR_OPTIONS="--conf spark.executor.extraJavaOptions=-Dlog4j.configuration=file:///${output_dir}/log4j.properties"
        logInfo "Creating tables. Will take a few minutes ..."
        ProgressBar 2 122
        bin/spark-sql ${DRIVER_OPTIONS} ${EXECUTOR_OPTIONS} --conf spark.sql.catalogImplementation=hive -f ${TPCDS_WORK_DIR}/create_database.sql > ${TPCDS_WORK_DIR}/create_database.out 2>&1
        script_pid=$!
        bin/spark-sql ${DRIVER_OPTIONS} ${EXECUTOR_OPTIONS} --conf spark.sql.catalogImplementation=hive -f ${TPCDS_WORK_DIR}/create_tables_work.sql > ${TPCDS_WORK_DIR}/create_tables.out 2>&1 &
        script_pid=$!
        cont=1
        error_code=0
        while [  $cont -gt 0 ]; do
            progress=`cat ${TPCDS_WORK_DIR}/create_tables.out | grep -i "time taken" | wc -l`
            progress=`expr $progress + 2`
            ProgressBar ${progress} 122
            if [ -e ${TPCDS_WORK_DIR}/create_tables.out ]; then
                error_code=`cat ${TPCDS_WORK_DIR}/create_tables.out | grep -i "error" | wc -l`
            fi

            ps -p $script_pid > /dev/null 
            if [ $? == 1 ]; then
                error_code=1
            fi
 
            if [ "$error_code" -gt 0 ] || [ "$progress" -gt 121 ] ; then 
                cont=-1
            fi
            sleep 0.1
        done  
        if [ "$error_code" -gt 0 ] ; then 
            logError "Failed to create spark tables. Please review the following logs"
            logError "${TPCDS_WORK_DIR}/create_tables.out"
            logError "${TPCDS_WORK_DIR}/temp/create_database.out"
            logError "${TPCDS_LOG_DIR}/spark-tpcds.log"
        else
            echo ""
            logInfo "Spark tables created successfully.."
        fi
        cd $current_dir
    fi
}

function set_env()
{
    TEST_ROOT=`pwd`
    echo "SPARK_HOME is " $SPARK_HOME
    set_environment
    logInfo "Run Environment is set"
    logDebug "  TPCDS_GENDATA_DIR    = ${TPCDS_GENDATA_DIR}"
    logDebug "  TPCDS_GENQUERIES_DIR = ${TPCDS_GENQUERIES_DIR}"
    logDebug "  TPCDS_LOG_DIR        = ${TPCDS_LOG_DIR}"
    logDebug "  TPCDS_WORK_DIR       = ${TPCDS_WORK_DIR}"
    logDebug "  TPCDS_STREAM_BASE    = ${TPCDS_STREAM_BASE}"
    logDebug "  TPCDS_ROOT_DIR       = ${TPCDS_ROOT_DIR}"
    logDebug "  TPCDS_DBNAME         = ${TPCDS_DBNAME}"
}

function main()
{
    set_env
    time create_spark_tables
    check_createtables
    result=$?
    if [ "$result" -ne 1 ]; then 
        # Execute the Run tests if and only if the tables have been successfully created.
        logInfo "Executing Power Phase"
        time run_tpcds_queries
        logInfo "Executing Througput Phase"
        time run_tpcds_throughput_queries
    fi
    cleanup_spark
}


[[ "${BASH_SOURCE[0]}" == "${0}" ]] && main 
