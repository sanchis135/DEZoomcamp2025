## Week 1 Homework

In this homework we'll prepare the environment and practice Docker and SQL

When submitting your homework, you will also need to include a link to your GitHub repository or other public code-hosting site.

This repository should contain the code for solving the homework.

When your solution has SQL or shell commands and not code (e.g. python files) file format, include them directly in the README file of your repository.

## Question 1. Understanding docker first run

Run docker with the python:3.12.8 image in an interactive mode, use the entrypoint bash.

##### Instructions of the video:

Open Git Bash

Go to the path ~/Documents/GitHub/Data_Engineering_Zoomcamp_2025/1_intro

Create a folder:

```bash
mkdir 2_docker_sql
```
Create docker image:

```bash
winpty docker run -it --entrypoint=bash python:3.12.8
```

Install pandas library:

```bash
root@16c797683f9f:/# pip install pandas
```

Open python:

```bash
root@16c797683f9f:/# python
```

Verify the installation of pandas library:

```bash
>>> import pandas
>>> pandas.__version__
```

Create Dockerfile (using VSCode) with:

```bash
FROM python:3.12.8
RUN pip install pandas 
ENTRYPOINT [ "bash" ]
```

In Git Bash (exit of python and root):

```bash
docker build -t test:pandas .
```

```bash
winpty docker run -it test:pandas
```

```bash
root@6b50d3880917:/# python
```

Verify the installation of pandas library:

```bash
>>> import pandas
>>> pandas.__version__
```

Create pipeline.py (using VSCode) with:

```bash
import pandas as pd

print('job finished successfull')
```

Create Dockerfile (using VSCode) with:

```bash
FROM python:3.12.8

RUN pip install pandas 

WORKDIR /app
COPY  pipeline.py pipeline.py

ENTRYPOINT [ "bash" ]
```

In Git Bash (exit of python and root):

```bash
docker build -t test:pandas .
```

```bash
winpty docker run -it test:pandas
```

```bash
root@789987aed5f8:/app# python pipeline.py
```

Verify the installation of pandas library:

```bash
>>> import pandas
>>> pandas.__version__
```

##### Instructions to the question:

Open Git Bash

Go to the path ~/Documents/GitHub/Data_Engineering_Zoomcamp_2025/1_intro

Create a folder:

```bash
mkdir 2_docker_sql
```
Create docker image:

```bash
winpty docker run -it --entrypoint=bash python:3.12.8
```

Install pandas library:

```bash
root@16c797683f9f:/# pip install pip
```

```bash
Requirement already satisfied: pip in /usr/local/lib/python3.12/site-packages (24.3.1)
```

What's the version of pip in the image?

1) 24.3.1 <--
2) 24.2.1
3) 23.3.1
4) 23.2.1


## Question 2. Understanding Docker networking and docker-compose 

Open Git Bash

Go to the path ~/Documents/GitHub/Data_Engineering_Zoomcamp_2025/1_intro/2_docker_sql

Create docker-compose.yaml with the next indications.

Given the following docker-compose.yaml, what is the hostname and port that pgadmin should use to connect to the postgres database?

```bash
services:
  db:
    container_name: postgres
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: 'postgres'
      POSTGRES_PASSWORD: 'postgres'
      POSTGRES_DB: 'ny_taxi'
    ports:
      - '5433:5432'
    volumes:
      - vol-pgdata:/var/lib/postgresql/data

  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin  

volumes:
  vol-pgdata:
    name: vol-pgdata
  vol-pgadmin_data:
    name: vol-pgadmin_data
```

1) postgres:5433 <--
2) localhost:5432
3) db:5433
4) postgres:5432
5) db:5432

If there are more than one answers, select only one of them.

The port 5432 is updating to the port 5433. So, the db service is now mapped to port 5433 on the host.

Running the command: `docker-compose up`

Stopping Docker Compose. First do *ctrl + c* then run the command: `docker-compose down`

## Prepare Postgres

Run Postgres and load data as shown in the videos. We'll use the green taxi trips from October 2019:

```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz
```
In VSCode: 

Creating docker-compose.yaml

Running the next line in a Git Bash of VSCode:

```bash
winpty docker run -it -e POSTGRES_USER="root" -e POSTGRES_PASSWORD="root" -e POSTGRES_DB="ny_taxi" -v /c/Users/Sandra/Documents/GitHub/Data_Engineering_Zoomcamp_2025/1_intro/2_docker_sql/ny_taxi_postgres_data:/var/lib/postgresql/data -p 5432:5432 postgres:13
```

Open other Git Bash window of VSCode.

Go to the path ~/Documents/GitHub/Data_Engineering_Zoomcamp_2025/1_intro/2_docker_sql

Install pgcli: `pip install pgcli`

Let’s connect to it:

`pgcli -h localhost -p 5432 -u root -d ny_taxi`

And check it with: root@localhost:ny_taxi> `\dt`

In other Python terminal: 

`pip install jupyter`

`jupyter notebook`

Open a new notebook, and writing:

import pandas as pd
pd.__version__

In other Git bash window of VSCode:

```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz
```

You will also need the dataset with zones:

```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv
```
Download this data and put it into Postgres.

You can use the code from the course. It's up to you whether you want to use Jupyter or a python script.

Running homewrok.ipynb

pip install sqlalchemy
pip install psycopg2

In the last Git BAsh, checking again: root@localhost:ny_taxi> `\dt`

And later: `SELECY count(1) FROM green_taxi_data;`


## Question 3. Trip Segmentation Count

During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive), how many trips, respectively, happened:

Up to 1 mile
In between 1 (exclusive) and 3 miles (inclusive),
In between 3 (exclusive) and 7 miles (inclusive),
In between 7 (exclusive) and 10 miles (inclusive),
Over 10 miles

SQL: 
1) `SELECT COUNT(*) FROM green_taxi_data WHERE lpep_pickup_datetime >= '2019-10-01' AND lpep_dropoff_datetime < '2019-11-01' AND trip_distance <= 1.0;`
2) `SELECT COUNT(*) FROM green_taxi_data WHERE lpep_pickup_datetime >= '2019-10-01' AND lpep_dropoff_datetime < '2019-11-01' AND trip_distance > 1.0 AND trip_distance <= 3.0;`
3) `SELECT COUNT(*) FROM green_taxi_data WHERE lpep_pickup_datetime >= '2019-10-01' AND lpep_dropoff_datetime < '2019-11-01' AND trip_distance > 3.0 AND trip_distance <= 7.0;`
4) `SELECT COUNT(*) FROM green_taxi_data WHERE lpep_pickup_datetime >= '2019-10-01' AND lpep_dropoff_datetime < '2019-11-01' AND trip_distance > 7.0 AND trip_distance <= 10.0;`
5) `SELECT COUNT(*) FROM green_taxi_data WHERE lpep_pickup_datetime >= '2019-10-01' AND lpep_dropoff_datetime < '2019-11-01' AND trip_distance > 10.0`

Answers:

1) 104,802; 197,670; 110,612; 27,831; 35,281
2) 104,802; 198,924; 109,603; 27,678; 35,189 <--
3) 104,793; 201,407; 110,612; 27,831; 35,281
4) 104,793; 202,661; 109,603; 27,678; 35,189
5) 104,838; 199,013; 109,645; 27,688; 35,202

## Question 4. Longest trip for each day

Which was the pick up day with the longest trip distance? Use the pick up time for your calculations.

Tip: For every day, we only care about one single trip with the longest distance.

SQL: `SELECT DATE(lpep_pickup_datetime) FROM green_taxi_data WHERE trip_distance = (SELECT MAX(trip_distance) FROM green_taxi_data);`

1) 2019-10-11
2) 2019-10-24
3) 2019-10-26
4) 2019-10-31 <--

## Question 5. Three biggest pickup zones

Which were the top pickup locations with over 13,000 in total_amount (across all trips) for 2019-10-18?

Consider only lpep_pickup_datetime when filtering by date.

SQL: `SELECT "PULocationID" FROM green_taxi_data WHERE DATE(lpep_pickup_datetime) = '2019-10-18' GROUP BY "PULocationID" HAVING SUM(total_amount) > 13000.0`

+--------------+
| PULocationID |
|--------------|
| 74           |
| 75           |
| 166          |
+--------------+

SQL: `SELECT "Zone" FROM zones WHERE "LocationID" = '74' OR "LocationID" = '75' OR "LocationID" = '166';`

+---------------------+
| Zone                |
|---------------------|
| East Harlem North   |
| East Harlem South   |
| Morningside Heights |
+---------------------+

1) East Harlem North, East Harlem South, Morningside Heights <--
2) East Harlem North, Morningside Heights
3) Morningside Heights, Astoria Park, East Harlem South
4) Bedford, East Harlem North, Astoria Park

## Question 6. Largest tip

For the passengers picked up in October 2019 in the zone named "East Harlem North" which was the drop off zone that had the largest tip?

Note: it's tip , not trip

We need the name of the zone, not the ID.

SQL: `SELECT "LocationID" FROM zones WHERE "Zone" = 'East Harlem North'`

+------------+
| LocationID |
|------------|
| 74         |
+------------+

SQL: `SELECT "DOLocationID" FROM green_taxi_data WHERE lpep_pickup_datetime >= '2019-10-01' AND lpep_pickup_datetime < '2019-11-01' AND "PULocationID" = '74' ORDER BY "tip_amount" DESC LIMIT 1;`

+--------------+
| DOLocationID |
|--------------|
| 132          |
+--------------+

SQL: `SELECT "Zone" FROM zones WHERE "LocationID" = '132'`

+-------------+
| Zone        |
|-------------|
| JFK Airport |
+-------------+

1) Yorkville West
2) 2) JFK Airport <--
3) East Harlem North
4) East Harlem South

## Terraform

In this section homework we'll prepare the environment by creating resources in GCP with Terraform.

In your VM on GCP/Laptop/GitHub Codespace install Terraform. Copy the files from the course repo here to your VM/Laptop/GitHub Codespace.

Modify the files as necessary to create a GCP Bucket and Big Query Dataset.

Following this instructions (using Windows): https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/01-docker-terraform/1_terraform_gcp/windows.md

## Question 7. Terraform Workflow

Which of the following sequences, respectively, describes the workflow for:

Downloading the provider plugins and setting up backend,
Generating proposed changes and auto-executing the plan
Remove all resources managed by terraform`
Answers:

1) terraform import, terraform apply -y, terraform destroy
2) teraform init, terraform plan -auto-apply, terraform rm
3) terraform init, terraform run -auto-approve, terraform destroy
4) terraform init, terraform apply -auto-approve, terraform destroy <--
5) terraform import, terraform apply -y, terraform rm
