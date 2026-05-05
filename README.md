# StreamNest Analytics on GCP — End-to-End Implementation Guide

**Course:** CE 408 Cloud Computing — Assignment 2<br>
**Goal:** Build a working proof-of-concept on GCP that demonstrates (1) a containerized microservice on GKE and (2) a data lakehouse pipeline using Cloud Storage + BigQuery, then submit a professional design document for StreamNest's CTO.

> **This guide assumes you are working entirely from Google Cloud Shell** (the terminal in your browser). No local installs of Docker, gcloud, or kubectl needed. Just follow each phase top-to-bottom.

---

## Table of Contents

- [Phase 0 — One-time setup (10 min)](#phase-0--one-time-setup-10-min)
- [Phase 1 — Scaffold the project in Cloud Shell (5 min)](#phase-1--scaffold-the-project-in-cloud-shell-5-min)
- [Phase 2 — Build & deploy microservice on GKE (40 min)](#phase-2--build--deploy-microservice-on-gke-40-min)
- [Phase 3 — Data lakehouse pipeline: GCS → BigQuery (30 min)](#phase-3--data-lakehouse-pipeline-gcs--bigquery-30-min)
- [Phase 4 — Capture screenshots checklist (15 min)](#phase-4--capture-screenshots-checklist-15-min)
- [Phase 5 — Write the design document (90 min, with fillable template)](#phase-5--write-the-design-document-90-min)
- [Phase 6 — Cleanup (5 min)](#phase-6--cleanup-5-min)
- [Appendix A — Troubleshooting](#appendix-a--troubleshooting)
- [Appendix B — Mapping to assignment requirements](#appendix-b--mapping-to-assignment-requirements)

**Total time budget: ~3.5 hours.**

---

## Phase 0 — One-time setup (10 min)

### 0.1 Open Cloud Shell

In your browser, go to https://console.cloud.google.com → click the `>_` icon (top right). A terminal opens at the bottom.

### 0.2 Set your project ID once and reuse it everywhere

Replace `your-project-id-here` with your actual project ID (visible at the top of the GCP console):

```bash
export PROJECT_ID="your-project-id-here"
export REGION="asia-southeast1"
gcloud config set project $PROJECT_ID
gcloud config set compute/region $REGION
```

> **Tip:** Cloud Shell wipes env vars when you close the tab. To make them survive, append the two `export` lines to `~/.bashrc`.

### 0.3 Enable all APIs in one shot

```bash
gcloud services enable \
  container.googleapis.com \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com \
  bigquery.googleapis.com \
  storage.googleapis.com \
  compute.googleapis.com
```

This takes ~1 minute. Wait for it to finish before moving on.

### Checkpoint
Run `gcloud config list` — confirm `project` and `region` are set correctly.

---

## Phase 1 — Scaffold the project in Cloud Shell (5 min)

Create the folder structure with the microservice code, manifests, and dataset generator, all in one go.

```bash
mkdir -p ~/streamnest && cd ~/streamnest
mkdir -p api k8s data
```

### 1.1 The microservice — `api/app.py`

Use the Cloud Shell editor (`cloudshell edit api/app.py`) or `nano api/app.py`, and paste:

```python
from flask import Flask, jsonify
from datetime import datetime
import os, socket

app = Flask(__name__)

CATALOG = [
    {"id": "c001", "title": "Neon Skyline",     "genre": "Sci-Fi", "duration_min": 112},
    {"id": "c002", "title": "Monsoon Diaries",  "genre": "Drama",  "duration_min": 98},
    {"id": "c003", "title": "Code & Coffee",    "genre": "Doc",    "duration_min": 45},
    {"id": "c004", "title": "Karachi Nights",   "genre": "Thriller","duration_min": 124},
    {"id": "c005", "title": "Bali Sunsets",     "genre": "Travel", "duration_min": 67},
]

@app.get("/")
def root():
    return {
        "service": "StreamNest Catalog API",
        "version": os.getenv("APP_VERSION", "v1"),
        "pod": socket.gethostname(),
        "time": datetime.utcnow().isoformat() + "Z",
    }

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/catalog")
def catalog():
    return jsonify(CATALOG)

@app.get("/catalog/<cid>")
def by_id(cid):
    item = next((c for c in CATALOG if c["id"] == cid), None)
    return (jsonify(item), 200) if item else ({"error": "not found"}, 404)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
```

> Returning the pod hostname lets you visibly see load balancing across pods when you refresh the URL.

### 1.2 `api/requirements.txt`

```
flask==3.0.3
gunicorn==22.0.0
```

### 1.3 `api/Dockerfile`

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
ENV APP_VERSION=v1
EXPOSE 8080
CMD ["gunicorn", "-b", "0.0.0.0:8080", "-w", "2", "app:app"]
```

### 1.4 Kubernetes manifests

**`k8s/deployment.yaml`** — leave the `IMAGE_PLACEHOLDER` exactly as-is; we'll substitute it at deploy time.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: catalog-api
  labels: { app: catalog-api }
spec:
  replicas: 3
  selector:
    matchLabels: { app: catalog-api }
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 0
  template:
    metadata:
      labels: { app: catalog-api }
    spec:
      containers:
        - name: catalog-api
          image: IMAGE_PLACEHOLDER
          ports:
            - containerPort: 8080
          readinessProbe:
            httpGet: { path: /health, port: 8080 }
            initialDelaySeconds: 3
            periodSeconds: 5
          livenessProbe:
            httpGet: { path: /health, port: 8080 }
            initialDelaySeconds: 10
            periodSeconds: 15
          resources:
            requests: { cpu: "250m", memory: "256Mi" }
            limits:   { cpu: "500m", memory: "512Mi" }
```

**`k8s/service.yaml`**

```yaml
apiVersion: v1
kind: Service
metadata:
  name: catalog-api-svc
spec:
  type: LoadBalancer
  selector: { app: catalog-api }
  ports:
    - port: 80
      targetPort: 8080
```

### 1.5 Sample-data generator — `data/gen_watch_events.py`

```python
import csv, random, uuid
from datetime import datetime, timedelta

devices  = ["Android", "iOS", "WebTV", "SmartTV", "Browser"]
regions  = ["Pakistan", "Indonesia", "Vietnam", "Thailand", "Philippines", "Malaysia"]
contents = [f"c{str(i).zfill(3)}" for i in range(1, 21)]

start = datetime(2026, 4, 1)
rows = 50000

with open("watch_events.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow([
        "event_id", "user_id", "content_id", "watch_duration_sec",
        "device_type", "region", "event_timestamp",
    ])
    for _ in range(rows):
        w.writerow([
            str(uuid.uuid4()),
            f"u{random.randint(1, 5000)}",
            random.choice(contents),
            random.randint(30, 7200),
            random.choice(devices),
            random.choice(regions),
            (start + timedelta(seconds=random.randint(0, 30 * 24 * 3600))).isoformat(),
        ])
print(f"Wrote {rows} rows to watch_events.csv")
```

### Checkpoint
`ls -R ~/streamnest` should show:
```
api/app.py  api/Dockerfile  api/requirements.txt
k8s/deployment.yaml  k8s/service.yaml
data/gen_watch_events.py
```

---

## Phase 2 — Build & deploy microservice on GKE (40 min)

### 2.1 Create an Artifact Registry repo

```bash
gcloud artifacts repositories create streamnest-repo \
  --repository-format=docker \
  --location=$REGION \
  --description="StreamNest container images"
```

### 2.2 Build the image with Cloud Build (no local Docker needed)

```bash
cd ~/streamnest/api
export IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/streamnest-repo/catalog-api:v1"
gcloud builds submit --tag $IMAGE .
```

This uploads your `api/` folder to Cloud Build, builds the Docker image on Google's servers, and pushes it to Artifact Registry. Takes ~2–3 minutes.

### 2.3 Create the GKE Autopilot cluster (runs ~8–10 min — **start now**, then continue with Phase 3 in another Cloud Shell tab while it provisions)

```bash
gcloud container clusters create-auto streamnest-cluster --region=$REGION
```

> **Open a second Cloud Shell tab** (`+` icon) to do Phase 3 in parallel. Don't waste 10 minutes staring at this.

When the cluster is ready, fetch credentials:

```bash
gcloud container clusters get-credentials streamnest-cluster --region=$REGION
kubectl get nodes
```

### 2.4 Deploy to the cluster

```bash
cd ~/streamnest
sed "s|IMAGE_PLACEHOLDER|$IMAGE|g" k8s/deployment.yaml | kubectl apply -f -
kubectl apply -f k8s/service.yaml
```

### 2.5 Wait for the public IP and test it

```bash
kubectl get svc catalog-api-svc -w
```

When `EXTERNAL-IP` shows an actual IP (not `<pending>`), press `Ctrl+C`, then:

```bash
export EXT_IP=$(kubectl get svc catalog-api-svc -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
echo "Public URL: http://$EXT_IP"
curl http://$EXT_IP/
curl http://$EXT_IP/catalog
curl http://$EXT_IP/catalog/c002
```

Open `http://$EXT_IP/catalog` in a browser tab too — that's the screenshot you'll embed.

### 2.6 Demonstrate scaling

```bash
kubectl scale deployment catalog-api --replicas=6
kubectl get pods -l app=catalog-api -o wide
```

### 2.7 Demonstrate zero-downtime rolling redeploy

Edit `api/app.py`, change something visible (e.g. add a content item or bump a version string), then:

```bash
cd ~/streamnest/api
export IMAGE_V2="${REGION}-docker.pkg.dev/${PROJECT_ID}/streamnest-repo/catalog-api:v2"
gcloud builds submit --tag $IMAGE_V2 .

kubectl set image deployment/catalog-api catalog-api=$IMAGE_V2
kubectl rollout status deployment/catalog-api
```

While the rollout is in progress, run this in another tab to prove zero downtime:

```bash
while true; do curl -s -o /dev/null -w "%{http_code}\n" http://$EXT_IP/health; sleep 1; done
```

You should see an unbroken stream of `200`s — that's the zero-downtime evidence.

### Checkpoint
- `kubectl get pods` shows pods Running
- `curl http://$EXT_IP/catalog` returns the JSON catalog
- Rolling redeploy completed without a single non-200 response

---

## Phase 3 — Data lakehouse pipeline: GCS → BigQuery (30 min)

### 3.1 Generate the dataset

```bash
cd ~/streamnest/data
python3 gen_watch_events.py
head -5 watch_events.csv
wc -l watch_events.csv     # should be 50001 (header + 50000 rows)
```

### 3.2 Create the GCS bucket and upload (the "raw zone")

> **Note:** GCS bucket names are limited to 63 characters. The shorter prefix `sn-datalake-` is used to stay within the limit.

```bash
export BUCKET="sn-datalake-${PROJECT_ID}"
gsutil mb -l $REGION -p $PROJECT_ID gs://$BUCKET
gsutil cp watch_events.csv gs://$BUCKET/raw/watch_events/watch_events.csv
gsutil ls -l gs://$BUCKET/raw/watch_events/
```

### 3.3 Create the BigQuery dataset

```bash
bq --location=$REGION mk --dataset ${PROJECT_ID}:streamnest_lake
```

### 3.4 Create the external table over the raw CSV

Run in BigQuery console (https://console.cloud.google.com/bigquery) → click **+ COMPOSE NEW QUERY**, paste, replace `<PROJECT_ID>`:

```sql
CREATE OR REPLACE EXTERNAL TABLE `<PROJECT_ID>.streamnest_lake.watch_events_ext` (
  event_id            STRING,
  user_id             STRING,
  content_id          STRING,
  watch_duration_sec  INT64,
  device_type         STRING,
  region              STRING,
  event_timestamp     TIMESTAMP
)
OPTIONS (
  format = 'CSV',
  uris   = ['gs://sn-datalake-<PROJECT_ID>/raw/watch_events/*.csv'],
  skip_leading_rows = 1
);

SELECT COUNT(*) AS total_events FROM `<PROJECT_ID>.streamnest_lake.watch_events_ext`;
```

You should see `total_events = 50000`.

### 3.5 Build the curated aggregated table (the "gold zone")

This answers: **"What are the top 5 most-watched titles in each region?"**

```sql
CREATE OR REPLACE TABLE `<PROJECT_ID>.streamnest_lake.top_content_by_region` AS
SELECT
  region,
  content_id,
  COUNT(*)                                    AS view_count,
  ROUND(SUM(watch_duration_sec) / 3600.0, 2)  AS total_watch_hours,
  ROUND(AVG(watch_duration_sec), 1)           AS avg_watch_sec
FROM `<PROJECT_ID>.streamnest_lake.watch_events_ext`
GROUP BY region, content_id
QUALIFY RANK() OVER (PARTITION BY region ORDER BY SUM(watch_duration_sec) DESC) <= 5
ORDER BY region, total_watch_hours DESC;

SELECT * FROM `<PROJECT_ID>.streamnest_lake.top_content_by_region`
ORDER BY region, total_watch_hours DESC;
```

### 3.6 Bonus query — device popularity (quick second insight)

```sql
SELECT
  region,
  device_type,
  COUNT(*) AS sessions,
  RANK() OVER (PARTITION BY region ORDER BY COUNT(*) DESC) AS rank_in_region
FROM `<PROJECT_ID>.streamnest_lake.watch_events_ext`
GROUP BY region, device_type
QUALIFY rank_in_region = 1
ORDER BY region;
```

### Checkpoint
- GCS console shows `watch_events.csv` under `raw/watch_events/`
- BigQuery sidebar shows `streamnest_lake` dataset with `watch_events_ext` (external) and `top_content_by_region` (table)
- Final query returns rows showing top content per region

---

## Phase 4 — Screenshots reference (already captured throughout)

Screenshots were taken inline during each phase. The table below maps each file to its caption in the design document.

---

### Phase 0 & 1 — Setup and scaffold

| File | What it shows | Used in doc |
|------|---------------|-------------|
| `screenshots/01.png` | GCP Console project dashboard for `cloud-assignment-2-495322` | Section 2.1 |
| `screenshots/04.png` | `gcloud services enable` output + `gcloud config list` confirmation | Section 4.3 — Project Scaffold and Configuration |
| `screenshots/11.png` | `ls -R ~/streamnest` directory tree showing `api/`, `k8s/`, `data/` | Section 4.5 — Building and Deploying the Image |

---

### Phase 2 — GKE microservice

| File | What it shows | Used in doc |
|------|---------------|-------------|
| `screenshots/13.png` | Cloud Build pushing `catalog-api:v1` + all pods in `Running` state | Section 4.7 — Horizontal Scaling |
| `screenshots/14.png` | `gen_watch_events.py` run + CSV `head` output confirming 50,000 rows | Section 4.7 — Horizontal Scaling |
| `screenshots/31.png` | Browser at `/` returning pod #1 hostname | Section 4.7 — Horizontal Scaling (subfigure a) |
| `screenshots/32.png` | Browser at `/` returning pod #2 hostname (different pod) | Section 4.7 — Horizontal Scaling (subfigure b) |
| `screenshots/28.png` | Browser `GET /catalog` returning full JSON content catalog | Section 4.7 — Horizontal Scaling |
| `screenshots/34.png` | Health-check curl loop showing continuous `200`s during scaling test | Section 4.7 — Horizontal Scaling |
| `screenshots/35.png` | Cloud Build pushing `catalog-api:v2` to Artifact Registry | Section 4.8 — Zero-Downtime Rolling Redeploy |
| `screenshots/40.png` | Unbroken stream of `200`s in terminal during rolling update | Section 4.8 — Zero-Downtime Rolling Redeploy |

---

### Phase 3 — Data lakehouse

| File | What it shows | Used in doc |
|------|---------------|-------------|
| `screenshots/22.png` | GCS bucket showing `watch_events.csv` under `raw/watch_events/` | Section 5.3 — Step 1: Raw Dataset in Cloud Storage |
| `screenshots/20.png` | BigQuery query editor with aggregation SQL + `streamnest_lake` sidebar | Section 5.4 — Step 2: BigQuery External Table |
| `screenshots/23.png` | `CREATE EXTERNAL TABLE` SQL + sample data rows from `watch_events_ext` | Section 5.4 — Step 2: BigQuery External Table |
| `screenshots/19.png` | BigQuery `Query completed` confirming `top_content_by_region` created | Section 5.5 — Step 3: Curated Aggregated Table |
| `screenshots/21.png` | Final `top_content_by_region` table preview (region, content, watch hours) | Section 5.5 — Step 3: Curated Aggregated Table |

---

### Phase 6 — Resource cleanup

| File | What it shows | Used in doc |
|------|---------------|-------------|
| `screenshots/42.png` | Cloud Shell showing workload deletion + GCS bucket removal | Appendix C — teardown step 1 |
| `screenshots/43.png` | Successful GKE cluster + BigQuery dataset deletion | Appendix C — teardown step 2 |

---

> **Tip:** When embedding in the design doc, label each one as *Figure 1, Figure 2, Figure 3, ...* and reference it in the text (e.g. "As shown in Figure 3..."). Use **Win + Shift + S** to crop any remaining screenshots you still need.

---

## Phase 5 — Write the design document (90 min)

This is the **actual deliverable**. Use the fillable template below — replace every `[FILL: ...]` placeholder with your own words. Keep tone professional, as if writing to StreamNest's CTO.

### Recommended tools
- **LaTeX on Overleaf** (https://www.overleaf.com) — the provided `design_document.tex` is the complete template; upload it to a new Overleaf project along with your `screenshots/` folder and `architecture.png`.
- **draw.io** (https://app.diagrams.net) for the architecture diagram — export as PNG and upload to the Overleaf root.
- Alternatively: **Microsoft Word** or **Google Docs** if you prefer not to use LaTeX.

### Cover page
```
StreamNest Cloud Migration: Design Proposal for GCP Infrastructure
Prepared for:  Chief Technology Officer, StreamNest
Prepared by:   Ahmed Musharaf
               Reg No: 2022067
Course:        CE 408 — Cloud Computing
               Assignment 2
Submitted to:  Miss Safia Baloch
Date:          May 5, 2026
```

---

### Section 1 — Executive Summary (½ page)

> *Goal: tell the CTO in 4–5 sentences what you built and why it solves their problem.*

**Template paragraph (rewrite in your own voice):**
> StreamNest's current platform — bare-VM microservices and unstructured flat-file data stores — is the root cause of slow deployments and the analytics team's inability to derive insights at scale. This proposal outlines a two-pillar migration to Google Cloud Platform: (1) containerizing backend microservices and orchestrating them on **Google Kubernetes Engine (GKE) Autopilot**, and (2) building a **data lakehouse** on **Cloud Storage + BigQuery** that serves both batch and ad-hoc analytics. A working proof-of-concept has been delivered: a public Catalog API running on GKE that scales horizontally and redeploys with zero downtime, plus a queryable BigQuery layer over a 50,000-event sample dataset. This document explains the architecture, key design decisions, and a path to production.

---

### Section 2 — Problem Restatement (½ page)

> *Show the CTO you understood their pain.*

**Embed screenshot:** `01.png` — GCP Console project dashboard.

Bullet points to expand on:
- **Engineering pain:** manual deploys take hours, dev/prod drift, no horizontal scaling.
- **Data pain:** MySQL + flat files cannot serve real-time + batch workloads from one place; no schema; data scientists blocked.
- **Business consequence:** slower time-to-market for features, no data-driven recommendations, scaling to 2M users will break the current setup.

---

### Section 3 — Proposed Architecture (1 page + diagram)

**Architecture diagram (draw in draw.io and embed as image).** Suggested layout:

```
                          ┌──────────────────────────────┐
   Internet users  ─────► │  GCP HTTP(S) Load Balancer   │
                          └──────────────┬───────────────┘
                                         │
                          ┌──────────────▼───────────────┐
                          │   GKE Autopilot Cluster      │
                          │   ┌─────┐ ┌─────┐ ┌─────┐    │
                          │   │ Pod │ │ Pod │ │ Pod │... │   ← Catalog API
                          │   └─────┘ └─────┘ └─────┘    │
                          └──────────────┬───────────────┘
                                         │ pulls images
                          ┌──────────────▼───────────────┐
                          │     Artifact Registry        │
                          └──────────────────────────────┘

  Event sources ──► Cloud Storage (raw zone)  ──► BigQuery External Table
                            │                              │
                            └──── lifecycle to Coldline    ▼
                                                BigQuery Curated Tables
                                                (top_content_by_region, ...)
                                                          │
                                                          ▼
                                              Looker Studio / ML / dbt
```

Embed this diagram, then write 1–2 paragraphs walking through the request path and the data flow.

---

### Section 4 — Microservices on GKE (1.5 pages)

Cover, in your words:
- **Why containers** solve dev/prod drift and slow deploys (immutable artifact, runs identically anywhere).
- **Why GKE Autopilot specifically** vs. Standard GKE or Cloud Run:
  - Fully managed nodes (no patching, autoscaling out of the box).
  - Pay per pod resource request, not per VM — better economics for variable load.
  - Still gives you full Kubernetes API for future microservices.
- **How rolling updates ensure zero downtime** (`maxUnavailable: 0`, readiness probes).
- **Horizontal scaling** is a one-line `kubectl scale` command, and can be made automatic via HPA in production.

**Embed screenshots** (see Phase 4 table for exact files): `01.png`, `04.png`, `11.png`, `13.png`, `14.png`, `31.png`+`32.png` (subfigure), `28.png`, `34.png`, `35.png`, `40.png`.

---

### Section 5 — Data Lakehouse on GCS + BigQuery (1.5 pages)

Cover:
- **Why a lakehouse** (vs pure data warehouse or pure lake): keeps raw data cheaply on GCS, but layers warehouse-grade SQL on top via BigQuery — best of both worlds.
- **Zone model**:
  - **Raw zone** (GCS): immutable, untouched event logs in their original format.
  - **Curated zone** (BigQuery tables): clean, aggregated, schema-enforced, fast to query.
- **Why BigQuery** vs hosting Postgres on a VM:
  - Serverless, auto-scaling — no capacity planning.
  - Separation of storage and compute — pay only for queries you run.
  - External tables let you query GCS without moving data.
- **How this answers a real business question** — show the `top_content_by_region` query and explain that the recommendation team can now feed this into their model directly.

**Embed screenshots** (see Phase 4 table for exact files): `22.png`, `20.png`, `23.png`, `19.png`, `21.png`.

---

### Section 6 — Scalability & Cost (½ page)

Bullet topics:
- GKE Autopilot scales pods within minutes; HPA can be added on CPU/QPS.
- BigQuery scales to petabytes; on-demand pricing means zero cost when idle.
- Cloud Storage lifecycle rules: transition data older than 30 days to **Nearline**, older than 90 to **Coldline** — drops storage cost ~70%.
- Estimated POC monthly cost at current scale: roughly **$43–60/month**, dominated by the GKE cluster and load balancer; production cost projection scales sub-linearly with users.

---

### Section 7 — Security & Operations (½ page)

- **IAM service accounts per workload** (least-privilege).
- **Workload Identity** so pods authenticate to GCP APIs without static keys.
- **Private GKE cluster + authorized networks** for production.
- **VPC Service Controls** around BigQuery and GCS to prevent data exfiltration.
- **Encryption at rest** is automatic for all GCP storage; consider CMEK for sensitive data.
- **Centralized logging** via Cloud Logging; metrics + alerts via Cloud Monitoring.

---

### Section 8 — Roadmap to Production (½ page)

What this PoC does not yet include, and how to get there:
1. **Real-time ingestion** — replace the CSV upload with **Pub/Sub → Dataflow → BigQuery** for streaming events.
2. **CI/CD** — GitHub → **Cloud Build** triggers → automated test + deploy to GKE on every merge.
3. **Transformation layer** — adopt **dbt** for SQL transformations from raw → curated; version-controlled, tested.
4. **BI** — **Looker Studio** dashboards on top of curated tables for executives.
5. **ML** — feed `top_content_by_region` and watch-history tables into **Vertex AI** for personalized recommendations.
6. **Additional microservices** — containerise the authentication and billing services following the same Dockerfile + GKE deployment pattern and add them to the existing cluster as independent Deployments.

---

### Section 9 — Conclusion (¼ page)

A short paragraph reinforcing that the proof-of-concept proves the architecture works end-to-end and is the right foundation for StreamNest's 2M-user scale.

---

### Appendix A — Commands & Manifests Reference

Includes the final `Dockerfile`, `deployment.yaml`, `service.yaml`, and the BigQuery DDL statements. The TA grading should be able to reproduce your work from the appendix alone.

### Appendix B — Overleaf Upload Checklist

Lists every file that must be uploaded to Overleaf before compiling: `architecture.png` (root), and all 18 screenshots in the `screenshots/` folder.

### Appendix C — Resource Cleanup

Documents the teardown of all GCP resources (GKE cluster, Artifact Registry, GCS bucket, BigQuery dataset) with screenshots confirming each deletion.

---

## Phase 6 — Cleanup (5 min)

**Run this as soon as the design doc is finalized** to avoid eating credits. The GKE cluster + load balancer is the biggest cost.

> **Don't run cleanup until your screenshots and the design doc are 100% done.** Once deleted, the EXTERNAL-IP is gone forever.

> **Warning:** Cloud Shell resets env vars between sessions. If `$REGION`, `$BUCKET`, or `$PROJECT_ID` are empty the commands below will silently fail. Re-export them first, or use the hardcoded fallback version underneath.

**Step 1 — Re-export vars (do this first, every time):**
```bash
export PROJECT_ID=cloud-assignment-2-495322
export REGION=asia-southeast1
export BUCKET=sn-datalake-cloud-assignment-2-495322
```

**Step 2 — Delete workloads and data (run each line separately to catch errors):**
```bash
kubectl delete -f ~/streamnest/k8s/service.yaml
kubectl delete -f ~/streamnest/k8s/deployment.yaml
gcloud storage rm -r gs://$BUCKET
bq rm -r -f -d ${PROJECT_ID}:streamnest_lake
```

**Step 3 — Delete the cluster and registry (these take ~2 min each):**
```bash
gcloud artifacts repositories delete streamnest-repo --location=$REGION --project=$PROJECT_ID --quiet
gcloud container clusters delete streamnest-cluster --region=$REGION --project=$PROJECT_ID --quiet
```

**Hardcoded fallback** (if env vars aren't set):
```bash
gcloud container clusters delete streamnest-cluster --region=asia-southeast1 --project=cloud-assignment-2-495322 --quiet
gcloud artifacts repositories delete streamnest-repo --location=asia-southeast1 --project=cloud-assignment-2-495322 --quiet
bq rm -r -f -d cloud-assignment-2-495322:streamnest_lake
```

> `bq rm` prints nothing on success — an empty prompt return means it worked.

---

## Appendix A — Troubleshooting

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| `gcloud builds submit` says permission denied | Cloud Build API not enabled | Re-run the API enable command in 0.3 |
| `EXTERNAL-IP` stays `<pending>` for >5 min | Quota or load balancer still provisioning | Wait 2 more min; check `gcloud compute forwarding-rules list` |
| Pods stuck in `Pending` | Autopilot resource constraints | Check `kubectl describe pod <name>` — usually the resource requests are too tight; bump CPU/memory |
| `curl` to external IP times out | Firewall — but Autopilot opens this by default | Confirm `kubectl get svc` actually shows an IP, not just hostname |
| BigQuery: "Not found: URI ..." on external table | Bucket name typo or wrong region | Confirm `gsutil ls gs://$BUCKET/raw/watch_events/` works |
| `bq mk` fails with location error | Region mismatch with bucket | Both must be `asia-southeast1` (or whatever region you chose) |
| Cloud Shell session disconnected | Idle timeout (1 hr) or browser closed | Reconnect; re-export `PROJECT_ID` and `REGION`; running pods/jobs are unaffected |

---

## Appendix B — Mapping to assignment requirements

| Assignment requirement (from PDF) | Where it's covered |
|------------------------------------|--------------------|
| Containerize at least one microservice | Phase 1.1–1.3 (Catalog API + Dockerfile) |
| Deploy on GKE with public endpoint | Phase 2.3–2.5 |
| Demonstrate scaling without downtime | Phase 2.6–2.7 |
| Realistic sample dataset (user_id, content_id, duration, device, region, timestamp) | Phase 1.5 + 3.1 |
| Raw data in Cloud Storage | Phase 3.2 |
| BigQuery external table | Phase 3.4 |
| Aggregated table answering a real business question | Phase 3.5 (top content per region) |
| Written design document with embedded screenshots | Phase 5 |
| Screenshots: GKE deployment, public API response, GCS bucket, BigQuery analytics table | Phase 4 checklist |

---

## Final submission checklist

- [ ] All Phase 4 screenshots saved (01.png, 04.png, 11.png, 13.png, 14.png, 19.png, 20.png, 21.png, 22.png, 23.png, 28.png, 31.png, 32.png, 34.png, 35.png, 40.png)
- [ ] Resource cleanup screenshots saved (42.png, 43.png)
- [ ] `architecture.png` exported from draw.io and uploaded to Overleaf root
- [ ] All screenshots uploaded to `screenshots/` folder in Overleaf
- [ ] Design document compiled in Overleaf without errors
- [ ] Each screenshot has a figure number + caption matching the actual image content
- [ ] Architecture diagram embedded as image (not ASCII)
- [ ] Appendix A includes all manifests and SQL
- [ ] Document exported as **PDF**
- [ ] File is named `CE408_A2_2022067.pdf`
- [ ] Phase 6 cleanup executed to stop billing

---

Good luck with your submission!
