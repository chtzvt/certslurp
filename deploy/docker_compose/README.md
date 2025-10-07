# certslurp (Docker Compose)

certslurp is a distributed system for large-scale retrieval and processing of Certificate Transparency (CT) data.
This repository includes a minimal local cluster setup using Docker Compose to demonstrate certslurp's components.

#### 🧩 Architecture

|Component|Role|
|-|-|
|`etcd`|	Cluster coordination and metadata store|
|`head`|	API and job coordinator node|
|`workers`|	Distributed worker daemons that perform scraping or analysis|
|`ctl`|	Developer/debug container with access to the full source tree|


#### 🚀 Getting Started

#### Build and start the cluster

```
make up
```

This command:
- Starts etcd
- Bootstraps users/roles and enables auth
- Brings up the head node and workers

Once complete, visit:

```
http://localhost:8080/healthz
```

You should see:

```
ok
```


##### Access the toolbox (debug shell)

```
make ctl
```

You’ll enter an interactive shell inside the `ctl` container with your full source mounted at /certslurp.

Example commands inside the container:

```
cd /certslurp
certslurpctl job submit /certslurp/examples/jobs/job.yml
```

#### ⚙️ Useful Commands

|Command|Description|
|-|-|
`make build`|	Rebuild local images|
`make up` |	Bring up the full cluster (includes bootstrap)|
`make up-fast`|	Bring up the cluster without reinitializing etcd|
`make down`|	Stop and remove containers and volumes|
`make nuke`|	Hard reset — wipes etcd data|
`make logs`|	Tail logs from all containers|
`make tail-head`|	Tail only head logs|
`make tail-workers`|	Tail worker logs|


#### 🧠 Environment Variables

Set in `.env`:

|Variable|Description|
|-|-|
|`ETCD_ROOT_PASSWORD`|etcd root password|
|`CERTSLURPD_USERNAME` / `CERTSLURPD_PASSWORD`|App-level etcd credentials|
|`CERTSLURPD_CLUSTER_KEY`|Cluster encryption key|
|`CERTSLURPD_API_TOKENS`|API auth tokens for the head node|

#### 🧹 Resetting the Cluster

To completely wipe all data and restart fresh:

```
make nuke
make up
```

#### 🧰 Development Notes

- The `ctl` container mounts the project root (`../../`) into `/certslurp` for convenience.
- Each service runs on the shared Docker network `certslurp`.
- **For production or remote clusters, rotate all credentials in .env before deployment.**
