#!/usr/bin/env bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

usage(){
  cat << EOF
  usage: $0 -m <k8s-cluster> -dm <deploy-mode> -n <name> -ns <namespace> [-hv <hadoop-version>, -sv <spark-version>, -b, -t <tag>]
      |  $0 -m <k8s-cluster> -dm cluster -n test-app -ns mynamespace -hv 3.2 -sv 3.1.2 -b
      |  $0 -m <k8s-cluster> -l kubernetes.default:host-gateway -dm cluster -n test-app -ns mynamespace -t <tag>
      |  $0 -l kubernetes.default:host-gateway -m k8s://https://kubernetes.default:62769 -dm cluster ...
  -----
  -h  | --help: print usage
  Required:
  -m  | --master: k8s master of kind k8s://https://<k8s-apiserver-host>:<k8s-apiserver-port>
  -l  | --localhost-cluster: to bind to the host network, when the cluster runs on the same node
  -dm | --deploy-mode: spark deploy mode
  -ns | --namespace: k8s namespace to deploy the spark application
  -n  | --name: name of the spark application
  Optional:
  -b  | --build-image: whether to locally build the image to be spawned and used for the submitter (version extracted from local build.sbt or envvar GILBERTO_VERSION)
  -hv | --hadoop-version: version of the target hadoop environment (default 3.2)
  -kp | --kerberos-principal: the principal to use for kerberos authentication (must provide a keytab as well)
  -kt | --kerberos-keytab: the keytab to use for kerberos authentication (must define a principal as well)
  -k  | --kill: kill specified Spark Job
  -p  | --params: params passed over to the job (the envvar JOB_PARAMS can also be used)
  -pr | --push-to-repo: whether to push the image to a remote repo, if set, it expects a parameter of kind myrepo:port/organization (without trailing /) to prepend to the image tag
  -sa | --sa-conf-dir: the path to the directory containing the sa secret and token
  -sc | --spark-conf: the path to the desired spark.conf file for the deploy
  -sv | --spark-version: version of the target spark environment (default 3.1.2)
  -t  | --tag: run specific image tag instead of building one or using an available project image
EOF
}

# setting input args
while [[ $# -ne 0 ]];
do
  case "${1}" in
    -k|--kill)
    export KILL="${2}"
    shift
    ;;
    -kp|--kerberos-principal)
    export KRB_PRINCIPAL="${2}"
    shift
    ;;
    -kt|--kerberos-keytab)
    KRB_KEYTAB="${2}"
    shift
    ;;
    -m|--master)
    export K8SMASTER="${2}"
    shift
    ;;
    -l|--localhost-cluster)
    LOCALHOST_CLUSTER="${2}"
    shift
    ;;
    -dm|--deploy-mode)
    export DEPLOYMODE="${2}"
    shift
    ;;
    -ns|--namespace)
    export NAMESPACE="${2}"
    shift
    ;;
    -n|--name)
    export APP_NAME="${2}"
    shift
    ;;
    -sv|--spark-version)
    export SPARK_VERSION="${2}"
    shift
    ;;
    -hv|--hadoop-version)
    export HADOOP_VERSION="${2}"
    shift
    ;;
    -sa|--sa-conf-dir)
    SA_CONF="${2}"
    shift
    ;;
    -sc|--spark-conf)
    SPARK_CONF="${2}"
    shift
    ;;
    -b|--build)
    BUILD_IMAGE="true"
    ;;
    -t|--tag)
    export TAG="${2}"
    shift
    ;;
    -pr|--push-to-repo)
    PUSH_TO_REPO="${2}"
    shift
    ;;
    -p|--params)
    export JOB_PARAMS="${2}"
    shift
    ;;
    -h|--help)
    usage
    exit
    ;;
    *) # unknown
    usage
    exit
    ;;
  esac
shift
done

# check all required vars are set
declare -a REQUIRED=("K8SMASTER" "DEPLOYMODE" "NAMESPACE" "APP_NAME")
for A in "${REQUIRED[@]}"
do
   if [[ -z "${!A}" ]]; then
     usage
     exit
   fi
done

# ----- Build Step
# retrieve proj version from sbt file, if available
command -v sbt >/dev/null 2>&1 && {
  GILBERTO_VERSION=$(sbt -Dsbt.supershell=false -error "print version")
} || {
  # if not, use the provided var, if any, or default to a version for fault tolerance
  GILBERTO_VERSION=${GILBERTO_VERSION:-0.1}
}

# if a tag is not defined, create one from the other parameters
if [ -z "${TAG}" ]; then
  # set to default version or to provided one if any
  export HADOOP_VERSION=${HADOOP_VERSION:-3.2}
  export SPARK_VERSION=${SPARK_VERSION:-3.1.2}

  # tag of target image
  export TAG="${APP_NAME}:${GILBERTO_VERSION}_${HADOOP_VERSION}_${SPARK_VERSION}"

  # build image, if specified
  if [ "${BUILD_IMAGE}" = "true" ]; then
    echo "Building ${TAG} using local Dockerfile"
    docker build --build-arg HADOOP_VERSION --build-arg SPARK_VERSION --tag ${TAG} .
    if [ ! -z "${PUSH_TO_REPO}" ]; then
      docker tag ${TAG} ${PUSH_TO_REPO}/${TAG}
      export TAG=${PUSH_TO_REPO}/${TAG}
      docker push ${TAG}
    fi
  fi
fi

# default values - optional but provided to the submit
export JOB_PARAMS=${JOB_PARAMS:-""}

# https://stackoverflow.com/questions/3572030/bash-script-absolute-path-with-os-x
realpath() {
    [[ $1 = /* ]] && echo "$1" || echo "$PWD/${1#./}"
}

if [ ! -z "${KRB_PRINCIPAL}" ]; then
  if [ -z "${KRB_KEYTAB}" ]; then
    echo "If you define a principal, you must provide a keytab file!"
    exit
  else
    export KRB_MOUNTED_KEYTAB="/opt/spark/work-dir/$(basename ${KRB_KEYTAB})"
    HOST_KRB_KEYTAB=$(realpath ${KRB_KEYTAB})
    echo "Using keytab at ${HOST_KRB_KEYTAB} as ${KRB_MOUNTED_KEYTAB}"
    MOUNT_KEYTAB="--mount type=bind,source=${HOST_KRB_KEYTAB},target=${KRB_MOUNTED_KEYTAB}"
  fi
fi

# mount default spark conf unless the user provided one at a specific path
SPARK_CONF=$(realpath ${SPARK_CONF:-"${SCRIPT_DIR}/spark.conf"})
echo "Using Spark conf at ${SPARK_CONF}"
MOUNT_SPARK_CONF="--mount type=bind,source=${SPARK_CONF},target=/opt/spark/work-dir/$(basename ${SPARK_CONF})"

# mount default SA from the current folder, or the one provided by the user
SA_CONF=$(realpath ${SA_CONF:-"${SCRIPT_DIR}/sa-conf"})
echo "Using SA conf at ${SA_CONF}"
MOUNT_SA_CONF="--mount type=bind,source=${SA_CONF},target=/opt/spark/work-dir/$(basename ${SA_CONF})"

# ----- Running Step
# https://stackoverflow.com/questions/24319662/from-inside-of-a-docker-container-how-do-i-connect-to-the-localhost-of-the-mach
DOCKER_RUN_COMMAND="docker run"
if [ ! -z "${LOCALHOST_CLUSTER}" ]; then
  # e.g. --add-host kubernetes.default:host-gateway binds the hostname kubernetes.default to host-gateway
  # same as --net=host on linux
  DOCKER_RUN_COMMAND="${DOCKER_RUN_COMMAND} --add-host ${LOCALHOST_CLUSTER}"
fi

read -r -d '' DOCKER_RUN_COMMAND <<- EOF
  ${DOCKER_RUN_COMMAND} \
  -e K8SMASTER -e DEPLOYMODE -e KRB_PRINCIPAL -e KRB_MOUNTED_KEYTAB -e APP_NAME -e NAMESPACE -e TAG -e JOB_PARAMS \
  --mount type=bind,source=${SCRIPT_DIR}/submitter-entrypoint.sh,target=/opt/spark/work-dir/submitter-entrypoint.sh \
  ${MOUNT_SPARK_CONF} \
  ${MOUNT_SA_CONF} \
  ${MOUNT_KEYTAB} \
  --entrypoint /opt/spark/work-dir/submitter-entrypoint.sh \
  ${TAG}
EOF

DOCKER_RUN_COMMAND=$(echo ${DOCKER_RUN_COMMAND} | tr '\n' ' ' | sed -e 's/[[:space:]]*$//')

echo "Running tag ${TAG}"
echo ${DOCKER_RUN_COMMAND}

${DOCKER_RUN_COMMAND}