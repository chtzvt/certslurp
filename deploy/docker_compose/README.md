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

#### 1️⃣ Build and start the cluster

```
make up
```

This command:
- Starts etcd
- Bootstraps users/roles and enables auth
- Brings up the head node and workers

You can check that the head node booted correctly by visiting `http://localhost:8080/healthz`, which should return a 200 status (`ok`).

##### 2️⃣ Access the toolbox (debug shell)

Certslurp clusters are managed by the `certslurpctl` utility. You can access this by running:

```
make ctl
```

This will enter an interactive shell inside the `ctl` container with your full source mounted at `/certslurp`.

Example commands inside the container:

```
cd /certslurp
certslurpctl job submit /certslurp/examples/jobs/job.yml
```

You can then watch the scraped output from the workers with:

```
make tail-workers
```

You should see output like:

```
worker-2-1  | {"cn":"www.waterdamage502.com","dns":["waterdamage502.com","www.waterdamage502.com"],"iss":"CN=Go Daddy Secure Certificate Authority - G2,OU=http://certs.godaddy.com/repository/,O=GoDaddy.com\\, Inc.,L=Scottsdale,ST=Arizona,C=US","li":101803067,"lts":1716036584778,"naf":"2025-05-18T12:49:43Z","nbf":"2024-05-18T12:49:43Z","sn":"a383990cda8d902b","sub":"CN=www.waterdamage502.com","t":"precert"}
worker-2-1  | {"cn":"smittywood.com","dns":["smittywood.com","www.smittywood.com"],"iss":"CN=Go Daddy Secure Certificate Authority - G2,OU=http://certs.godaddy.com/repository/,O=GoDaddy.com\\, Inc.,L=Scottsdale,ST=Arizona,C=US","li":101803068,"lts":1716036580814,"naf":"2025-05-18T12:49:40Z","nbf":"2024-05-18T12:49:40Z","sn":"9c47f9fe032db20b","sub":"CN=smittywood.com","t":"precert"}
worker-2-1  | {"cn":"*.08f1b3e39f08.devices.meraki.direct","co":["US"],"dns":["*.08f1b3e39f08.devices.meraki.direct"],"iss":"CN=Cisco Meraki CA,O=Cisco Systems\\, Inc.,C=US","li":101803065,"loc":["San Francisco"],"lts":1716036583170,"naf":"2025-05-17T23:59:59Z","nbf":"2024-05-18T00:00:00Z","org":["Meraki LLC"],"prv":["California"],"sn":"565544f904a0c2057a11828f380a5bb","sub":"CN=*.08f1b3e39f08.devices.meraki.direct,O=Meraki LLC,L=San Francisco,ST=California,C=US","t":"precert"}
worker-2-1  | {"cn":"*.unitascommunications.com","dns":["*.unitascommunications.com","unitascommunications.com"],"iss":"CN=Encryption Everywhere DV TLS CA - G2,OU=www.digicert.com,O=DigiCert Inc,C=US","li":101803069,"lts":1716036576119,"naf":"2025-05-31T23:59:59Z","nbf":"2024-05-18T00:00:00Z","sn":"14e7e77fa7e8eedeecbeaded2d56e2e","sub":"CN=*.unitascommunications.com","t":"precert"}
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
