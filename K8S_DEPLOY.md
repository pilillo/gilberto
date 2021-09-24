# Deploy to K8s

## Accessing a K8s Cluster

Setting up a K8s cluster is out of the scope of this markdown.
However, mind that for testing purposes you can easily ramp up a single-node cluster with any of the following:
* [minikube](https://minikube.sigs.k8s.io/docs/start/)
* [microk8s](https://microk8s.io/)
* [code-ready containers](https://github.com/code-ready/crc)
* [kind: k8s in docker](https://kind.sigs.k8s.io/)

You can then install the [kubectl](https://kubernetes.io/docs/tasks/tools/) or oc CLI tools.

## Create a Service Account
Let's start by creating a new namespace to run our applications:

`kubectl create namespace spark`

For the sake of simplicity, we report the imperative version of the commands.
Clearly, you can use `--dry-run=client and -o yaml` to get a yaml version of the resource that can be committed and created in a declarative fashion, such as:

`kubectl create namespace spark --dry-run=client -o yaml`

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: spark
```

As readable from the official doc [here](https://spark.apache.org/docs/latest/running-on-kubernetes.html#rbac), the only requirement to run Spark on K8s is an appropriate service account having edit rights on the namespace:

1. `kubectl create serviceaccount spark`
2. `kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=spark:spark --namespace=spark`

In case you do not have enough rights to create a clusterrolebinding (e.g. if you are not a cluster admin), it is enough to create a rolebinding on the spark namespace.

In order to authenticate to the k8s API server, you now need to export the cert and the service account token.

```bash
NAMESPACE=spark
SA=spark
SA_SECRET=$(kubectl get sa ${SA} -n ${NAMESPACE} -o jsonpath="{.secrets[].name}")
kubectl get secret ${SA_SECRET} -n ${NAMESPACE} -o go-template='{{index .data "ca.crt"}}' | base64 --decode > sa-conf/ca.crt
kubectl get secret ${SA_SECRET} -n ${NAMESPACE} -o jsonpath="{.data['token']}" | base64 --decode > sa-conf/sa.token
| xargs echo
```

You can now mount the files on the submitter container (e.g. at `/sa-conf`) and set it for use with:
```
spark.kubernetes.authenticate.submission.caCertFile=/sa-conf/ca.crt
spark.kubernetes.authenticate.submission.oauthTokenFile=/sa-conf/sa.token
```

## Deploy an application

To deploy a Spark application on k8s, you can:
* use the spark-submit which can natively deploy to K8s starting from Spark 2.3
* use the [spark operator](https://github.com/GoogleCloudPlatform/spark-on-k8s-operator)

For the latter, we firstly retrieve the details of your target k8s cluster, e.g. for minikube:
```
$ kubectl cluster-info
Kubernetes control plane is running at https://127.0.0.1:61668
CoreDNS is running at https://127.0.0.1:61668/api/v1/namespaces/kube-system/services/kube-dns:dns/proxy

To further debug and diagnose cluster problems, use 'kubectl cluster-info dump'.
```

You can use the script `gilberto-submit.sh` to submit a docker-based Spark application on K8s, for instance:

```
./gilberto-submit.sh -m k8s://https://kubernetes.default:62769 -dm cluster  -ns spark -n gilberto -hv 3.2 -sv 3.1.2 -p "-a profile -s test_table -d /result -f 01/01/2021 -t 01/01/2021"
```

Mandatory fields for the submit script are:
* a k8s master
* a deploy mode
* a k8s namespace
* an application name

Spark settings are either provided at submit or written in the `spark.conf` which is mounted as default spark config in the submitter container.

In case of local single-node cluster, such as minikube or microk8s, the container needs to access the host network.
The `-l` or `--localhost-cluster` parameter can be used to add a host alias in the container's `resolv.conf` file, such as:
```
./gilberto-submit.sh -l kubernetes.default:host-gateway -m k8s://https://kubernetes.default:62769 -dm cluster -ns spark -n gilberto -hv 3.2 -sv 3.1.2 -p "-a profile -s test_table -f 01/01/2021 -t 01/01/2021"
```

which adds an alias for `kubernetes.default` to the host ip, so that minikube is reachable and the certificates can be validated with an accepted hostname (as opposed to the default `host.docker.internal`).