# Week 1 Homework

## Task 1 - Docker Python Setup

**Command:**
```bash
docker run -it --rm python:3.13 bash
```

**Background:** WSL 2 installed to run Docker natively

**Output:**
```
niko@DESKTOP-CGI4JRE:/bootcamp-data-engineering-zoomcamp$ docker run -it --rm python:3.13 bash
Unable to find image 'python:3.13' locally
3.13: Pulling from library/python
2ca1bfae7ba8: Pull complete
b6513238a015: Pull complete
82e18c5e1c15: Pull complete
be442a7e0d6f: Pull complete
26d823e3848f: Pull complete
9b57076d00d4: Pull complete
ca4b54413202: Pull complete
Digest: sha256:c8b03b4e98b39cfb180a5ea13ae5ee39039a8f75ccf52fe6d5c216eed6e1be1d
Status: Downloaded newer image for python:3.13
root@65e177f38700:/# python --version
Python 3.13.11
root@65e177f38700:/# pip --version
pip 25.3 from /usr/local/lib/python3.13/site-packages/pip (python 3.13)
root@65e177f38700:/#
```

---

## Task 2 - Docker Compose

**Useful commands:**
```bash
docker compose up -d      # Start in background
docker compose down       # Stop containers
docker compose logs -f    # View logs
docker compose down -v    # Stop and delete volumes (data)
```

> **Tip:** Mount a `.sql` file to `/docker-entrypoint-initdb.d/` to auto-run SQL on init

**Output:**
```
[+] Running 5/5
 ✔ Network week1_default                 Created      0.1s
 ✔ Volume "week1_ny_taxi_postgres_data"  Created      0.0s
 ✔ Volume "week1_pgadmin_data"           Created      0.0s
 ✔ Container week1-pgadmin-1             Started      2.5s
 ✔ Container week1-pgdatabase-1          Started
```

---

## Preparation for Tasks 3-6

### Data Loaded into PostgreSQL

| Table | Rows |
|-------|------|
| `green_taxi_trips` | 46,912 |
| `taxi_zones` | 265 |

### Access Details

- **pgAdmin:** http://localhost:8085
- **Email:** `admin@admin.com`
- **Password:** `root`
- **Server connection:** host=`postgres`, port=`5432`, user=`root`, password=`root`

### Verify Tables

```bash
niko@DESKTOP-CGI4JRE:/bootcamp-data-engineering-zoomcamp/week1$ docker exec -it week1-postgres-1 psql -U root -d ny_taxi
psql (16.11 (Debian 16.11-1.pgdg13+1))
Type "help" for help.

ny_taxi-# \dt
             List of relations
 Schema |       Name       | Type  | Owner
--------+------------------+-------+-------
 public | green_taxi_trips | table | root
 public | taxi_zones       | table | root
(2 rows)
```

---

## Task 3 - Trips with Distance ≤ 1 Mile

**Query:**
```sql
SELECT COUNT(*)
FROM green_taxi_trips
WHERE trip_distance <= 1;
```

---

## Task 4 - Longest Trip Distance (< 100 miles)

**Query:**
```sql
SELECT lpep_pickup_datetime::date AS pickup_day, trip_distance
FROM green_taxi_trips
WHERE trip_distance < 100
ORDER BY trip_distance DESC
LIMIT 1;
```

---

## Task 5 - Pickup Zone with Largest Total Amount (Nov 18)

**Query:**
```sql
SELECT z."Zone", SUM(t.total_amount) as total
FROM green_taxi_trips t
JOIN taxi_zones z ON t."PULocationID" = z."LocationID"
WHERE t.lpep_pickup_datetime::date = '2025-11-18'
GROUP BY z."Zone"
ORDER BY total DESC
LIMIT 1;
```

---

## Task 6 - Largest Tip from East Harlem North

**Query:**
```sql
SELECT dz."Zone" as dropoff_zone, MAX(t.tip_amount) as max_tip
FROM green_taxi_trips t
JOIN taxi_zones pz ON t."PULocationID" = pz."LocationID"
JOIN taxi_zones dz ON t."DOLocationID" = dz."LocationID"
WHERE pz."Zone" = 'East Harlem North'
GROUP BY dz."Zone"
ORDER BY max_tip DESC
LIMIT 1;
```

---

## Task 7 - Terraform Workflow

| Step | Command | Purpose |
|------|---------|---------|
| 1 | `terraform init` | Downloads provider plugins, initializes backend |
| 2 | `terraform apply -auto-approve` | Plans and applies changes (skips confirmation) |
| 3 | `terraform destroy` | Removes all managed resources |
