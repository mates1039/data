#!/bin/bash

now="$(date +"%Y%m%d%H%M%S")"
myDir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
commonLibDir=${myDir}/../../common-lib
sourceLibDir=${myDir}/../../source-lib

######################################
### Setting up the spark Master ####
######################################
sparkMaster=$1
if [ $# -lt 1 ]; then
  echo "Spark Master not provided ... yarn will be used as default ...."
  echo "If you need to run this file in local mode pass local[*] as param."
  sparkMaster='yarn'
fi


######################################
### Setting up the dependent jars ####
######################################
DEPS_JAR=
add_jars=${commonLibDir}/*.jar
for file in $add_jars; do if [ -z "$DEPS_JAR" ]; then DEPS_JAR=$file; else  DEPS_JAR=$DEPS_JAR,$file; fi; done
echo $DEPS_JAR

########################################
### Spark Submit Related Parameters ###
########################################
sparkClass=com.ppc.nda.spark.stream.NdaMiddleTierUpsertStage
driverMemory='1g'
driverCores='1'
executorCores='2'
executorMemory='1g'
numExecutors='3'

##########################
### Job Configurations ###
##########################
appname="${myDir##*/}"
propRemoteBasePath='/edm-platform/spark'
confDir="${myDir}/conf"
driverLogDir="${myDir}/logs"
executorLogDir="/var/log/spark/"
sparkJar=$(ls ${sourceLibDir}/edm-*.jar)
propRemoteLoc="${propRemoteBasePath}/${appname}"
sparkConfigFile="${confDir}/sparkProperties.conf"
appConfigFile="${confDir}/app.conf"

##########################
### Log Confiugrations ###
##########################
#log4jFileName="log4j-${appname}.properties"
log4jFileName="log4j-nda.properties"
jobLogFileName="${appname}"
logFileDatePattern='yyyyMMdd'
logMaxFileSize='250MB'
logMaxBackup='5'
errKafkaTopicName="${appname}-err"
errKafkaBootstrapServers="${kbs}"


##########################
####### Job Setup ########
##########################
driverJavaOptions="-Dlog4j.configuration=file://${commonLibDir}/${log4jFileName} -Djob.log.dir=${driverLogDir} -Djob.log.file.name=${jobLogFileName} -Djob.log.datepattern=${logFileDatePattern} -Djob.log.maxfilesize=${logMaxFileSize} -Djob.log.maxbackupindex=${logMaxBackup}"
executorExtraJavaOption="spark.executor.extraJavaOptions=-Dlog4j.configuration=${log4jFileName} -Djob.log.dir=${executorLogDir} -Djob.log.file.name=${jobLogFileName} -Djob.log.datepattern=${logFileDatePattern} -Djob.log.maxfilesize=${logMaxFileSize} -Djob.log.maxbackupindex=${logMaxBackup}"


echo "This is a sample \"$(basename $0)\" file ..."


### If mode is local ####
if [ "${sparkMaster}" == "local[*]" ]; then
  echo "Job will execute in local mode.."
  #in local mode conf file is in local node
  echo "pulling job conf file @ : $appConfigFile"
  spark-submit --verbose --master local[*]  --class ${sparkClass} --jars $DEPS_JAR source_jar/$spark_jar "${confDir}/*"
fi

### If mode is yarn ####
if [ "${sparkMaster}" == "yarn" ]; then
  #hadoop fs -rm -r ${propRemoteLoc}
  #hadoop fs -mkdir -p ${propRemoteLoc}
  #hadoop fs -put ${confDir} ${propRemoteLoc}

  echo ""
  echo "Job will execute in yarn mode ..." # conf file of hdfs:/gs:/ @ : $configPropFile"

spark-submit --verbose --queue "default" --master yarn \
--driver-memory ${driverMemory} --driver-cores ${driverCores} \
--executor-cores ${executorCores} --executor-memory ${executorMemory} --num-executors ${numExecutors} \
--files ${commonLibDir}/${log4jFileName} \
--driver-java-options "${driverJavaOptions}" \
--conf "spark.driver.extraClassPath=${confClassPath}" \
--conf "${executorExtraJavaOption}" \
--conf "spark.executor.extraClassPath=${confClassPath}" \
--properties-file ${sparkConfigFile} \
--class ${sparkClass} \
--jars $DEPS_JAR \
${sparkJar} ${appConfigFile} &
fi


echo "Exitting script ...."
exit 0
