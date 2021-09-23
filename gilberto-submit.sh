#!/usr/bin/env bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

usage(){
  cat << EOF
  usage: $0 -n <name> -ns <namespace> [-hv <hadoop-version>, -sv <spark-version>, -b, -t <tag>]
      |  $0 -n <name> -ns <namespace> -hv 3.2 -sv 3.1.2 -b
      |  $0 -n <name> -ns <namespace> -t
  -----
  -h  | --help: print usage
  Required:
  -ns | --namespace: k8s namespace to deploy the spark application
  -n  | --name: name of the spark application
  Optional:
  -b  | --build-image: whether to locally build the image to be spawned and used for the submitter (version extracted from local build.sbt or envvar GILBERTO_VERSION)
  -pr | --push-to-repo: whether to push the image to a remote repo, if set, it expects a parameter of kind myrepo:port/organization (without trailing /) to prepend to the image tag
  -sv | --spark-version: version of the target spark environment (default 3.1.2)
  -hv | --hadoop-version: version of the target hadoop environment (default 3.2)
  -t  | --tag: run specific image tag instead of building one or using an available project image
  -p  | --params: params passed over to the job (the envvar JOB_PARAMS can also be used)
EOF
}

# setting input args
while [[ $# -ne 0 ]];
do
  case "${1}" in
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
declare -a REQUIRED=("NAMESPACE" "APP_NAME")
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

export JOB_PARAMS=${JOB_PARAMS:-""}

# ----- Running Step
# https://stackoverflow.com/questions/24319662/from-inside-of-a-docker-container-how-do-i-connect-to-the-localhost-of-the-mach
# host.docker.internal
# --network="host"
read -r -d '' DOCKER_RUN_COMMAND <<- EOF
  docker run \
  -e APP_NAME -e NAMESPACE -e TAG -e JOB_PARAMS \
  --mount type=bind,source=${SCRIPT_DIR}/sa-conf,target=/opt/spark/work-dir/sa-conf \
  --mount type=bind,source=${SCRIPT_DIR}/submitter-entrypoint.sh,target=/opt/spark/work-dir/submitter-entrypoint.sh \
  --mount type=bind,source=${SCRIPT_DIR}/spark.conf,target=/opt/spark/work-dir/spark.conf \
  --entrypoint /opt/spark/work-dir/submitter-entrypoint.sh \
  ${TAG}
EOF

DOCKER_RUN_COMMAND=$(echo ${DOCKER_RUN_COMMAND} | tr '\n' ' ' | sed -e 's/[[:space:]]*$//')

echo "Running tag ${TAG}"
echo ${DOCKER_RUN_COMMAND}

${DOCKER_RUN_COMMAND}