#!/usr/bin/env bash

# https://gist.github.com/jonsuh/3c89c004888dfc7352be
RED='\033[0;31m'
YELLOW='\033[1;33m'
NOCOLOR='\033[0m'

# merge job params with all string arguments we may have got, to keep compatibility
# 1. CONVERT STRING TO ARRAY
read -a JOB_PARAMS <<< ${JOB_PARAMS}
# 2. MERGE THE 2 ARRAYS INTO ONE
JOB_PARAMS=("$@" "${JOB_PARAMS[@]}")
# 3. ECHO PARAMS
if [ ${#JOB_PARAMS[@]} -gt 0 ]; then
  echo -e "${YELLOW}Using the following job parameters:"
  for p in "${JOB_PARAMS[@]}"; do
      echo -e "  - $p";
  done
  echo -e "${NOCOLOR}"
else
  echo -e "${RED}No parameters passed to the job!${NOCOLOR}"
  exit
fi

if [ ! -z "${KRB_PRINCIPAL}" ]; then
  KRB_LOGIN="--principal ${KRB_PRINCIPAL} --keytab ${KRB_MOUNTED_KEYTAB}"
  echo "Authenticating as ${KRB_LOGIN}"
fi

# https://spark.apache.org/docs/latest/running-on-kubernetes.html#dependency-management
read -r -d '' SUBMIT_COMMAND <<- EOF
  /opt/spark/bin/spark-submit \
  --master ${K8SMASTER} \
  --deploy-mode ${DEPLOYMODE} \
  ${KRB_LOGIN} \
  --name ${APP_NAME} \
  --class com.github.pilillo.Gilberto \
  --conf spark.kubernetes.namespace=${NAMESPACE} \
  --conf spark.kubernetes.container.image=${TAG} \
  --properties-file /opt/spark/work-dir/$(basename ${SPARK_CONF}) \
  --verbose \
  local:///gilberto.jar
EOF

SUBMIT_COMMAND="${SUBMIT_COMMAND} ${JOB_PARAMS[@]}"
SUBMIT_COMMAND=$(echo ${SUBMIT_COMMAND} | tr '\n' ' ' | sed -e 's/[[:space:]]*$//')

echo ${SUBMIT_COMMAND}
${SUBMIT_COMMAND}

