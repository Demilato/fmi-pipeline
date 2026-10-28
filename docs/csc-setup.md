# CSC cloud configuration (cPouta)

The whole runtime (Kafka, producer, consumer, Airflow, Streamlit) runs on one virtual machine in
CSC's cPouta IaaS cloud (OpenStack). BigQuery is the only external service.

> Add your own screenshots to `docs/images/` (instance overview, security group rules, key pair,
> floating IP) and reference them here. Hide the floating IP if the repository is public.

## 1. Project and service access

- MyCSC (<https://my.csc.fi>) → **Projects** → own project → **Services** → **cPouta** → accept terms → *Apply for Access*.
- The project belongs to the university, so billing units are an academic quota (CSC: services are free of
  charge for academic research and education at Finnish higher education institutions).

## 2. VM

Created at <https://pouta.csc.fi> → **Compute → Instances → Launch Instance**.

| Setting | Value | Why |
|---|---|---|
| Image | `Ubuntu-24.04` | LTS, supported by Docker's official repository |
| Flavor | `standard.large` (4 vCPU, 7.8 GiB RAM, 80 GB disk) | Kafka (512 MB heap) + 3 Airflow components + Postgres + 3 app containers ≈ 3–4 GB; smaller flavors are too tight |
| Network | project network | default |
| Security group | `fmi-ssh` (own group, not *default*) | see below |
| Key pair | imported public key (`ssh-keygen -t ed25519`) | password login is disabled on Pouta images |
| Floating IP | 1 associated | the only public address; needed for SSH |

Cost in billing units (BU): standard.large 2.1 BU/h + floating IP 0.2 BU/h ≈ 55 BU/day. A shut-down VM
still consumes BUs, so delete the VM and release the floating IP when the project is graded.

## 3. Network security

Security group `fmi-ssh`:

| Direction | Protocol | Port | Remote | Purpose |
|---|---|---|---|---|
| Ingress | TCP | 22 | `<own public IP>/32` | SSH from my machine only |
| Egress | any | any | 0.0.0.0/0 | default rules kept (FMI API, BigQuery, Docker Hub; also needed for key injection) |

No other ports are open. Airflow (8080) and Streamlit (8501) are published by Docker on **127.0.0.1 only**
(`docker-compose.yml`), so they are unreachable from the internet even if a security-group rule were
added by mistake. They are reached through SSH port forwarding:

```
# ~/.ssh/config on the laptop
Host cpouta
  HostName <floating-ip>
  User ubuntu
  IdentityFile ~/.ssh/cpouta
  LocalForward 8080 localhost:8080
  LocalForward 8501 localhost:8501
```

`ssh cpouta` then opens <http://localhost:8080> (Airflow) and <http://localhost:8501> (Streamlit) on the
laptop. VS Code's Remote-SSH extension can use the same host entry.

## 4. Software on the VM

`scripts/setup_vm.sh` (run once):

1. `apt-get upgrade` to bring the image up to date
2. Docker Engine + Compose plugin from Docker's official apt repository
3. adds `ubuntu` to the `docker` group
4. 4 GiB swap file as a safety margin for the 7.8 GiB RAM

## 5. Secrets

| Secret | Where | Protection |
|---|---|---|
| Google service-account key | `/home/ubuntu/secrets/gcp-key.json` | outside the repo, `chmod 600`, mounted read-only into consumer, dashboard and Airflow only (not the producer) |
| Airflow admin password, Fernet key, JWT/API secrets, Postgres password | `.env` | generated per VM by `scripts/init_env.sh`, mode 600, git-ignored, never printed |

Service account roles: **BigQuery Data Editor** + **BigQuery Job User** on the project, nothing else.

## 6. Containers

```
docker compose ps
```

| Service | Image | Role |
|---|---|---|
| kafka | apache/kafka:4.3.1 | single broker, KRaft mode (no ZooKeeper), 7-day retention |
| kafka-init | apache/kafka:4.3.1 | creates topic `fmi-observations` once, then exits |
| producer | fmi-pipeline-app (built from `app/`) | FMI API → Kafka |
| consumer | fmi-pipeline-app | Kafka → BigQuery |
| dashboard | fmi-pipeline-app | Streamlit on 127.0.0.1:8501 |
| postgres | postgres:16 | Airflow metadata DB |
| airflow-init | apache/airflow:3.3.2 | DB migration + admin user, then exits |
| airflow-apiserver | apache/airflow:3.3.2 | UI + API on 127.0.0.1:8080 |
| airflow-scheduler | apache/airflow:3.3.2 | schedules and runs tasks (LocalExecutor) |
| airflow-dag-processor | apache/airflow:3.3.2 | parses DAG files |

All services use `restart: unless-stopped`, so the pipeline comes back after a VM reboot, and Docker
log rotation (3 × 10 MB per container) so logs cannot fill the disk.

## 7. Teardown (after grading)

1. `docker compose down` (add `-v` to also delete Kafka/Postgres volumes)
2. Pouta: **Instances → Delete Instance**, then **Network → Floating IPs → Release**
3. Google Cloud: delete the service-account key (IAM → Service Accounts → Keys)
