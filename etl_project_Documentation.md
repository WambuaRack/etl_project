### 📄 c:\Users\Administrator\Desktop\etl_project\dags\etl_dag.py
*Saved at: 10/13/2025, 12:53:56 PM*

**[ADDED]**
```
1     import sys
2     import os
3     from datetime import datetime, timedelta
```
**[REMOVED]**
```
(from line ~6)
from datetime import datetime
from .scripts.extract import extract_quotes

```
**[ADDED]**
```
6     
7     # 👇 Ensure the scripts folder is importable inside the Docker container
8     sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
9     
10    # Import your ETL functions
11    from scripts.extract import extract_quotes
```
**[ADDED]**
```
15    
16    # ============================
17    #   Define Python callables
18    # ============================
19    
```
**[ADDED]**
```
21        """Extract data and save to CSV"""
```
**[ADDED]**
```
24        print("✅ Extracted data saved to /tmp/quotes.csv")
```
**[ADDED]**
```
26    
```
**[ADDED]**
```
28        """Transform raw CSV into cleaned data"""
```
**[ADDED]**
```
31        print("✅ Transformed data saved to /tmp/quotes_clean.csv")
```
**[ADDED]**
```
33    
```
**[ADDED]**
```
35        """Load transformed data into Postgres"""
```
**[ADDED]**
```
37        print("✅ Data loaded into Postgres successfully!")
```
**[ADDED]**
```
39    
40    # ============================
41    #   Default DAG arguments
42    # ============================
43    
```
**[REMOVED]**
```
(from line ~45)
    "owner": "airflow",

```
**[ADDED]**
```
45        "owner": "rack",
46        "depends_on_past": False,
47        "email_on_failure": False,
48        "email_on_retry": False,
```
**[ADDED]**
```
50        "retry_delay": timedelta(minutes=2),
```
**[ADDED]**
```
53    
54    # ============================
55    #   DAG Definition
56    # ============================
57    
```
**[ADDED]**
```
60        description="Web Scraping ETL pipeline: Extract → Transform → Load into Postgres",
```
**[REMOVED]**
```
(from line ~63)
    schedule_interval="@daily",
    catchup=False

```
**[ADDED]**
```
63        schedule='@daily',           # ✅ New Airflow 2.9+ keyword
64        catchup=False,
65        tags=["etl", "web-scraping", "rack"],
```
**[REMOVED]**
```
(from line ~68)
    t1 = PythonOperator(task_id="extract", python_callable=extract_task)
    t2 = PythonOperator(task_id="transform", python_callable=transform_task)
    t3 = PythonOperator(task_id="load", python_callable=load_task)

```
**[ADDED]**
```
68        # Define tasks
69        t1 = PythonOperator(
70            task_id="extract",
71            python_callable=extract_task,
72        )
```
**[ADDED]**
```
74        t2 = PythonOperator(
75            task_id="transform",
76            python_callable=transform_task,
77        )
78    
79        t3 = PythonOperator(
80            task_id="load",
81            python_callable=load_task,
82        )
83    
84        # Task dependencies: Extract → Transform → Load
```

---

### 📄 c:\Users\Administrator\Desktop\etl_project\scripts\wait-for-postgres.sh
*Saved at: 10/13/2025, 10:00:30 AM*

**[ADDED]**
```
1     #!/bin/bash
2     set -e
3     
4     host="$1"
5     shift
6     cmd="$@"
7     
8     until pg_isready -h "$host" -U airflow; do
9       echo "Waiting for Postgres at $host..."
10      sleep 2
11    done
12    
13    echo "Postgres is ready, executing command..."
14    exec $cmd
```

---

### 📄 c:\Users\Administrator\Desktop\etl_project\docker-compose.yaml
*Saved at: 10/13/2025, 9:51:46 AM*

**[REMOVED]**
```
(from line ~1)
version: "3.9"

```
**[ADDED]**
```
1     # docker-compose.yaml
2     # Windows-ready Airflow + Celery + Postgres + Redis
```
**[REMOVED]**
```
(from line ~4)
x-airflow-common: &airflow-common
  image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:3.1.0}
  environment:
    AIRFLOW__CORE__EXECUTOR: CeleryExecutor
    AIRFLOW__CORE__LOAD_EXAMPLES: 'true'
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CELERY__BROKER_URL: redis://:@redis:6379/0
    AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://airflow:airflow@postgres/airflow
    _PIP_ADDITIONAL_REQUIREMENTS: ${_PIP_ADDITIONAL_REQUIREMENTS:-}
  volumes:
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs
    - ./plugins:/opt/airflow/plugins
    - ./config:/opt/airflow/config
  user: "0:0"
  depends_on:
    redis:
      condition: service_healthy
    postgres:
      condition: service_healthy


```
**[REMOVED]**
```
(from line ~33)
    <<: *airflow-common

```
**[ADDED]**
```
33        image: apache/airflow:3.1.0
34        user: "0:0"  # Run as root for Windows
35        volumes:
36          - ./dags:/opt/airflow/dags
37          - ./logs:/opt/airflow/logs
38          - ./plugins:/opt/airflow/plugins
39          - ./config:/opt/airflow/config
40          - ./scripts:/opt/airflow/scripts
```
**[REMOVED]**
```
(from line ~44)
      echo 'Waiting for Postgres...';
      until pg_isready -h postgres -p 5432 -U airflow; do sleep 1; done;
      echo 'Initializing Airflow DB...';
      airflow db init;
      echo 'Creating Admin User...';

```
**[ADDED]**
```
44          python /opt/airflow/scripts/wait_for_postgres.py &&
45          airflow db init &&
```
**[ADDED]**
```
48        depends_on:
49          postgres:
50            condition: service_healthy
```
**[REMOVED]**
```
(from line ~54)
    <<: *airflow-common

```
**[ADDED]**
```
54        image: apache/airflow:3.1.0
55        user: "0:0"
56        volumes:
57          - ./dags:/opt/airflow/dags
58          - ./logs:/opt/airflow/logs
59          - ./plugins:/opt/airflow/plugins
60          - ./config:/opt/airflow/config
```
**[REMOVED]**
```
(from line ~70)
    <<: *airflow-common

```
**[ADDED]**
```
70        image: apache/airflow:3.1.0
71        user: "0:0"
72        volumes:
73          - ./dags:/opt/airflow/dags
74          - ./logs:/opt/airflow/logs
75          - ./plugins:/opt/airflow/plugins
76          - ./config:/opt/airflow/config
```
**[REMOVED]**
```
(from line ~84)
    <<: *airflow-common

```
**[ADDED]**
```
84        image: apache/airflow:3.1.0
85        user: "0:0"
86        volumes:
87          - ./dags:/opt/airflow/dags
88          - ./logs:/opt/airflow/logs
89          - ./plugins:/opt/airflow/plugins
90          - ./config:/opt/airflow/config
```
**[REMOVED]**
```
(from line ~98)
    <<: *airflow-common

```
**[ADDED]**
```
98        image: apache/airflow:3.1.0
99        user: "0:0"
100       volumes:
101         - ./dags:/opt/airflow/dags
102         - ./logs:/opt/airflow/logs
103         - ./plugins:/opt/airflow/plugins
104         - ./config:/opt/airflow/config
```
**[REMOVED]**
```
(from line ~112)
    <<: *airflow-common

```
**[ADDED]**
```
112       image: apache/airflow:3.1.0
113       user: "0:0"
114       volumes:
115         - ./dags:/opt/airflow/dags
116         - ./logs:/opt/airflow/logs
117         - ./plugins:/opt/airflow/plugins
118         - ./config:/opt/airflow/config
```

---

### 📄 c:\Users\Administrator\Desktop\etl_project\docker-compose.yaml
*Saved at: 10/13/2025, 9:50:19 AM*

**[ADDED]**
```
1     version: "3.9"
```
**[REMOVED]**
```
(from line ~3)


```
**[REMOVED]**
```
(from line ~18)
  user: "0:0"  # Run as root for Windows

```
**[ADDED]**
```
18      user: "0:0"
```
**[REMOVED]**
```
(from line ~54)
  <<: *airflow-common
  entrypoint: /bin/bash
  command: >
    -c "
    echo 'Waiting for Postgres...'
    until pg_isready -h postgres -p 5432 -U airflow; do sleep 1; done
    echo 'Initializing Airflow DB...'
    airflow db init
    echo 'Creating Admin User...'
    airflow users create \
      --username admin \
      --password admin \
      --firstname Shedrack \
      --lastname Wambua \
      --role Admin \
      --email shedrack@example.com
    "
  restart: "no"

```
**[ADDED]**
```
54        <<: *airflow-common
55        entrypoint: /bin/bash
56        command: >
57          -c "
58          echo 'Waiting for Postgres...';
59          until pg_isready -h postgres -p 5432 -U airflow; do sleep 1; done;
60          echo 'Initializing Airflow DB...';
61          airflow db init;
62          echo 'Creating Admin User...';
63          airflow users create --username admin --password admin --firstname Shedrack --lastname Wambua --role Admin --email shedrack@example.com
64          "
65        restart: "no"
```
**[REMOVED]**
```
(from line ~67)


```

---

### 📄 c:\Users\Administrator\Desktop\etl_project\docker-compose.yaml
*Saved at: 10/13/2025, 9:49:30 AM*

**[REMOVED]**
```
(from line ~54)
    <<: *airflow-common
    entrypoint: /bin/bash
    command: >
      -c "
      airflow db init &&
      airflow users create --username admin --password admin --firstname Shedrack --lastname Wambua --role Admin --email shedrack@example.com
      "
    restart: "no"

```
**[ADDED]**
```
54      <<: *airflow-common
55      entrypoint: /bin/bash
56      command: >
57        -c "
58        echo 'Waiting for Postgres...'
59        until pg_isready -h postgres -p 5432 -U airflow; do sleep 1; done
60        echo 'Initializing Airflow DB...'
61        airflow db init
62        echo 'Creating Admin User...'
63        airflow users create \
64          --username admin \
65          --password admin \
66          --firstname Shedrack \
67          --lastname Wambua \
68          --role Admin \
69          --email shedrack@example.com
70        "
71      restart: "no"
```
**[ADDED]**
```
73    
```

---

### 📄 c:\Users\Administrator\Desktop\etl_project\docker-compose.yaml
*Saved at: 10/13/2025, 9:48:12 AM*

**[REMOVED]**
```
(from line ~1)
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

```
**[REMOVED]**
```
(from line ~2)
# Basic Airflow cluster configuration for CeleryExecutor with Redis and PostgreSQL.
#
# WARNING: This configuration is for local development. Do not use it in a production deployment.
#
# This configuration supports basic configuration using environment variables or an .env file
# The following variables are supported:
#
# AIRFLOW_IMAGE_NAME           - Docker image name used to run Airflow.
#                                Default: apache/airflow:3.1.0
# AIRFLOW_UID                  - User ID in Airflow containers
#                                Default: 50000
# AIRFLOW_PROJ_DIR             - Base path to which all the files will be volumed.
#                                Default: .
# Those configurations are useful mostly in case of standalone testing/running Airflow in test/try-out mode
#
# _AIRFLOW_WWW_USER_USERNAME   - Username for the administrator account (if requested).
#                                Default: airflow
# _AIRFLOW_WWW_USER_PASSWORD   - Password for the administrator account (if requested).
#                                Default: airflow
# _PIP_ADDITIONAL_REQUIREMENTS - Additional PIP requirements to add when starting all containers.
#                                Use this option ONLY for quick checks. Installing requirements at container
#                                startup is done EVERY TIME the service is started.
#                                A better way is to build a custom image or extend the official image
#                                as described in https://airflow.apache.org/docs/docker-stack/build.html.
#                                Default: ''
#
# Feel free to modify this file to suit your needs.
---
x-airflow-common:
  &airflow-common
  # In order to add custom dependencies or upgrade provider distributions you can use your extended image.
  # Comment the image line, place your Dockerfile in the directory where you placed the docker-compose.yaml
  # and uncomment the "build" line below, Then run `docker-compose build` to build the images.

```
**[ADDED]**
```
2     
3     x-airflow-common: &airflow-common
```
**[REMOVED]**
```
(from line ~5)
  # build: .

```
**[REMOVED]**
```
(from line ~6)
    &airflow-common-env

```
**[REMOVED]**
```
(from line ~7)
    AIRFLOW__CORE__AUTH_MANAGER: airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager

```
**[ADDED]**
```
7         AIRFLOW__CORE__LOAD_EXAMPLES: 'true'
8         AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
```
**[REMOVED]**
```
(from line ~10)
    AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://airflow:airflow@postgres/airflow

```
**[REMOVED]**
```
(from line ~11)
    AIRFLOW__CORE__FERNET_KEY: ''
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
    AIRFLOW__CORE__LOAD_EXAMPLES: 'true'
    AIRFLOW__CORE__EXECUTION_API_SERVER_URL: 'http://airflow-apiserver:8080/execution/'
    # yamllint disable rule:line-length
    # Use simple http server on scheduler for health checks
    # See https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/check-health.html#scheduler-health-check-server
    # yamllint enable rule:line-length
    AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK: 'true'
    # WARNING: Use _PIP_ADDITIONAL_REQUIREMENTS option ONLY for a quick checks
    # for other purpose (development, test and especially production usage) build/extend Airflow image.

```
**[ADDED]**
```
11        AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://airflow:airflow@postgres/airflow
```
**[REMOVED]**
```
(from line ~13)
    # The following line can be used to set a custom config file, stored in the local config folder
    AIRFLOW_CONFIG: '/opt/airflow/config/airflow.cfg'

```
**[REMOVED]**
```
(from line ~14)
    - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags
    - ${AIRFLOW_PROJ_DIR:-.}/logs:/opt/airflow/logs
    - ${AIRFLOW_PROJ_DIR:-.}/config:/opt/airflow/config
    - ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins
  user: "${AIRFLOW_UID:-50000}:0"

```
**[ADDED]**
```
14        - ./dags:/opt/airflow/dags
15        - ./logs:/opt/airflow/logs
16        - ./plugins:/opt/airflow/plugins
17        - ./config:/opt/airflow/config
18      user: "0:0"  # Run as root for Windows
```
**[REMOVED]**
```
(from line ~20)
    &airflow-common-depends-on

```
**[REMOVED]**
```
(from line ~42)
    # Redis is limited to 7.2-bookworm due to licencing change
    # https://redis.io/blog/redis-adopts-dual-source-available-licensing/

```
**[ADDED]**
```
53      airflow-init:
54        <<: *airflow-common
55        entrypoint: /bin/bash
56        command: >
57          -c "
58          airflow db init &&
59          airflow users create --username admin --password admin --firstname Shedrack --lastname Wambua --role Admin --email shedrack@example.com
60          "
61        restart: "no"
62    
```
**[REMOVED]**
```
(from line ~68)
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/api/v2/version"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s

```
**[REMOVED]**
```
(from line ~70)
      <<: *airflow-common-depends-on

```
**[REMOVED]**
```
(from line ~76)
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8974/health"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s

```
**[REMOVED]**
```
(from line ~78)
      <<: *airflow-common-depends-on

```
**[REMOVED]**
```
(from line ~84)
    healthcheck:
      test: ["CMD-SHELL", 'airflow jobs check --job-type DagProcessorJob --hostname "$${HOSTNAME}"']
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s

```
**[REMOVED]**
```
(from line ~86)
      <<: *airflow-common-depends-on

```
**[REMOVED]**
```
(from line ~92)
    healthcheck:
      # yamllint disable rule:line-length
      test:
        - "CMD-SHELL"
        - 'celery --app airflow.providers.celery.executors.celery_executor.app inspect ping -d "celery@$${HOSTNAME}" || celery --app airflow.executors.celery_executor.app inspect ping -d "celery@$${HOSTNAME}"'
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s
    environment:
      <<: *airflow-common-env
      # Required to handle warm shutdown of the celery workers properly
      # See https://airflow.apache.org/docs/docker-stack/entrypoint.html#signal-propagation
      DUMB_INIT_SETSID: "0"

```
**[REMOVED]**
```
(from line ~94)
      <<: *airflow-common-depends-on
      airflow-apiserver:
        condition: service_healthy

```
**[REMOVED]**
```
(from line ~100)
    healthcheck:
      test: ["CMD-SHELL", 'airflow jobs check --job-type TriggererJob --hostname "$${HOSTNAME}"']
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s

```
**[REMOVED]**
```
(from line ~102)
      <<: *airflow-common-depends-on

```
**[REMOVED]**
```
(from line ~105)
  airflow-init:
    <<: *airflow-common
    entrypoint: /bin/bash
    # yamllint disable rule:line-length
    command:
      - -c
      - |
        if [[ -z "${AIRFLOW_UID}" ]]; then
          echo
          echo -e "\033[1;33mWARNING!!!: AIRFLOW_UID not set!\e[0m"
          echo "If you are on Linux, you SHOULD follow the instructions below to set "
          echo "AIRFLOW_UID environment variable, otherwise files will be owned by root."
          echo "For other operating systems you can get rid of the warning with manually created .env file:"
          echo "    See: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#setting-the-right-airflow-user"
          echo
          export AIRFLOW_UID=$$(id -u)
        fi
        one_meg=1048576
        mem_available=$$(($$(getconf _PHYS_PAGES) * $$(getconf PAGE_SIZE) / one_meg))
        cpus_available=$$(grep -cE 'cpu[0-9]+' /proc/stat)
        disk_available=$$(df / | tail -1 | awk '{print $$4}')
        warning_resources="false"
        if (( mem_available < 4000 )) ; then
          echo
          echo -e "\033[1;33mWARNING!!!: Not enough memory available for Docker.\e[0m"
          echo "At least 4GB of memory required. You have $$(numfmt --to iec $$((mem_available * one_meg)))"
          echo
          warning_resources="true"
        fi
        if (( cpus_available < 2 )); then
          echo
          echo -e "\033[1;33mWARNING!!!: Not enough CPUS available for Docker.\e[0m"
          echo "At least 2 CPUs recommended. You have $${cpus_available}"
          echo
          warning_resources="true"
        fi
        if (( disk_available < one_meg * 10 )); then
          echo
          echo -e "\033[1;33mWARNING!!!: Not enough Disk space available for Docker.\e[0m"
          echo "At least 10 GBs recommended. You have $$(numfmt --to iec $$((disk_available * 1024 )))"
          echo
          warning_resources="true"
        fi
        if [[ $${warning_resources} == "true" ]]; then
          echo
          echo -e "\033[1;33mWARNING!!!: You have not enough resources to run Airflow (see above)!\e[0m"
          echo "Please follow the instructions to increase amount of resources available:"
          echo "   https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#before-you-begin"
          echo
        fi
        echo
        echo "Creating missing opt dirs if missing:"
        echo
        mkdir -v -p /opt/airflow/{logs,dags,plugins,config}
        echo
        echo "Airflow version:"
        /entrypoint airflow version
        echo
        echo "Files in shared volumes:"
        echo
        ls -la /opt/airflow/{logs,dags,plugins,config}
        echo
        echo "Running airflow config list to create default config file if missing."
        echo
        /entrypoint airflow config list >/dev/null
        echo
        echo "Files in shared volumes:"
        echo
        ls -la /opt/airflow/{logs,dags,plugins,config}
        echo
        echo "Change ownership of files in /opt/airflow to ${AIRFLOW_UID}:0"
        echo
        chown -R "${AIRFLOW_UID}:0" /opt/airflow/
        echo
        echo "Change ownership of files in shared volumes to ${AIRFLOW_UID}:0"
        echo
        chown -v -R "${AIRFLOW_UID}:0" /opt/airflow/{logs,dags,plugins,config}
        echo
        echo "Files in shared volumes:"
        echo
        ls -la /opt/airflow/{logs,dags,plugins,config}

    # yamllint enable rule:line-length
    environment:
      <<: *airflow-common-env
      _AIRFLOW_DB_MIGRATE: 'true'
      _AIRFLOW_WWW_USER_CREATE: 'true'
      _AIRFLOW_WWW_USER_USERNAME: ${_AIRFLOW_WWW_USER_USERNAME:-airflow}
      _AIRFLOW_WWW_USER_PASSWORD: ${_AIRFLOW_WWW_USER_PASSWORD:-airflow}
      _PIP_ADDITIONAL_REQUIREMENTS: ''
    user: "0:0"

  airflow-cli:
    <<: *airflow-common
    profiles:
      - debug
    environment:
      <<: *airflow-common-env
      CONNECTION_CHECK_MAX_COUNT: "0"
    # Workaround for entrypoint issue. See: https://github.com/apache/airflow/issues/16252
    command:
      - bash
      - -c
      - airflow
    depends_on:
      <<: *airflow-common-depends-on

  # You can enable flower by adding "--profile flower" option e.g. docker-compose --profile flower up
  # or by explicitly targeted on the command line e.g. docker-compose up flower.
  # See: https://docs.docker.com/compose/profiles/
  flower:
    <<: *airflow-common
    command: celery flower
    profiles:
      - flower
    ports:
      - "5555:5555"
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:5555/"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s
    restart: always
    depends_on:
      <<: *airflow-common-depends-on
      airflow-init:
        condition: service_completed_successfully


```

---

### 📄 c:\Users\Administrator\Desktop\etl_project\dags\etl_dag.py
*Saved at: 10/13/2025, 9:44:40 AM*

**[ADDED]**
```
1     from airflow import DAG
2     from airflow.operators.python import PythonOperator
3     from datetime import datetime
4     from scripts.extract import extract_quotes
5     from scripts.transform import transform_quotes
6     from scripts.load import load_to_postgres
7     
8     def extract_task():
9         df = extract_quotes()
10        df.to_csv("/tmp/quotes.csv", index=False)
11    
12    def transform_task():
13        df = transform_quotes("/tmp/quotes.csv")
14        df.to_csv("/tmp/quotes_clean.csv", index=False)
15    
16    def load_task():
17        load_to_postgres("/tmp/quotes_clean.csv")
18    
19    default_args = {
20        "owner": "airflow",
21        "retries": 1,
22    }
23    
24    with DAG(
25        dag_id="web_scraping_etl",
26        default_args=default_args,
27        start_date=datetime(2025, 10, 13),
28        schedule_interval="@daily",
29        catchup=False
30    ) as dag:
31    
32        t1 = PythonOperator(task_id="extract", python_callable=extract_task)
33        t2 = PythonOperator(task_id="transform", python_callable=transform_task)
34        t3 = PythonOperator(task_id="load", python_callable=load_task)
35    
36        t1 >> t2 >> t3
```

---

### 📄 c:\Users\Administrator\Desktop\etl_project\scripts\load.py
*Saved at: 10/13/2025, 9:43:55 AM*

**[ADDED]**
```
1     import pandas as pd
2     import psycopg2
3     from sqlalchemy import create_engine
4     
5     def load_to_postgres(file_path="quotes_clean.csv"):
6         df = pd.read_csv(file_path)
7     
8         # PostgreSQL credentials
9         user = "postgres"
10        password = "postgres"
11        host = "postgres"
12        port = 5432
13        database = "etl_db"
14    
15        engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")
16    
17        df.to_sql("quotes", engine, if_exists="replace", index=False)
18        print("Data loaded to PostgreSQL successfully!")
19    
20    if __name__ == "__main__":
21        load_to_postgres()
```

---

### 📄 c:\Users\Administrator\Desktop\etl_project\scripts\transform.py
*Saved at: 10/13/2025, 9:43:43 AM*

**[ADDED]**
```
1     import pandas as pd
2     
3     def transform_quotes(file_path="quotes.csv"):
4         df = pd.read_csv(file_path)
5         
6         # Example transformations
7         df["text"] = df["text"].str.strip()
8         df["author"] = df["author"].str.title()
9         
10        return df
11    
12    if __name__ == "__main__":
13        df = transform_quotes()
14        df.to_csv("quotes_clean.csv", index=False)
```

---

### 📄 c:\Users\Administrator\Desktop\etl_project\scripts\extract.py
*Saved at: 10/13/2025, 9:43:32 AM*

**[ADDED]**
```
1     import requests
2     from bs4 import BeautifulSoup
3     import pandas as pd
4     
5     def extract_quotes():
6         url = "http://quotes.toscrape.com/"
7         response = requests.get(url)
8         soup = BeautifulSoup(response.text, "html.parser")
9     
10        quotes = []
11        for quote in soup.find_all("div", class_="quote"):
12            text = quote.find("span", class_="text").get_text()
13            author = quote.find("small", class_="author").get_text()
14            tags = [tag.get_text() for tag in quote.find_all("a", class_="tag")]
15            quotes.append({"text": text, "author": author, "tags": ",".join(tags)})
16        
17        df = pd.DataFrame(quotes)
18        return df
19    
20    if __name__ == "__main__":
21        df = extract_quotes()
22        df.to_csv("quotes.csv", index=False)
```

---

