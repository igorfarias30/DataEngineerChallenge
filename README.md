# Install requirements

1. Install Docker
> docker run -it --rm -p 8080:8080 python:3.8-slim /bin/bash

2. Export AIRFLOW_HOME
> export AIRFLOW_HOME=/usr/local/airflow

3. env | grep airflow
* To check that the environment variable has been well exported


4. apt-get update -y && apt-get install -y wget libczmq-dev curl libssl-dev git inetutils-telnet bind9utils freetds-dev libkrb5-dev libsasl2-dev libffi-dev libpq-dev freetds-bin build-essential default-libmysqlclient-dev apt-utils rsync zip unzip gcc && apt-get clean
* Install all tools and dependencies that can be required by Airflow


5. useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow
* Create the user airflow, set its home directory to the value of AIRFLOW_HOME and log into it


6. cat /etc/passwd | grep airflow
* Show the file /etc/passwd to check that the airflow user has been created

7. pip install --upgrade pip

8. su - airflow

9. python -m venv .challenge

10. source .challenge/bin/activate
* Activate the virtual environment challenge

wget https://raw.githubusercontent.com/apache/airflow/constraints-2.0.2/constraints-3.8.txt
* Download the requirement file to install the right version of Airflow’s dependencies 


pip install "apache-airflow[crypto,celery,postgres,cncf.kubernetes,docker]"==2.0.2 --constraint ./constraints-3.8.txt

pip install "apache-airflow[postgres]"==2.0.2 --constraint ./constraints-3.8.txt


* Install the version 2.0.2 of apache-airflow with all subpackages defined between square brackets. (Notice that you can still add subpackages after all, you will use the same command with different subpackages even if Airflow is already installed)


airflow db init
* Initialise the metadatabase


airflow scheduler &
* Start Airflow’s scheduler in background


airflow webserver &
* Start Airflow’s webserver in background


docker build -t airflow-basic .
* Build a docker image from the Dockerfile in the current directory (airflow-materials/airflow-basic)  and name it airflow-basic

docker run --rm -d -p 8080:8080 airflow-basic

Quick Tour of Airflow CLI


docker ps
* Show running docker containers


docker exec -it container_id /bin/bash
* Execute the command /bin/bash in the container_id to get a shell session


pwd
* Print the current path where you are


airflow db init
* Initialise the metadatabase


airflow db reset
* Reinitialize the metadatabase (Drop everything)


airflow db upgrade
* Upgrade the metadatabase (Latest schemas, values, ...)


airflow webserver
* Start Airflow’s webserver


airflow scheduler
* Start Airflow’s scheduler


airflow celery worker
* Start a Celery worker (Useful in distributed mode to spread tasks among nodes - machines)


airflow dags list
* Give the list of known dags (either those in the examples folder or in dags folder)


ls
* Display the files/folders of the current directory 


airflow dags trigger example_python_operator
* Trigger the dag example_python_operator with the current date as execution date


airflow dags trigger example_python_operator -e 2021-01-01
* Trigger the dag example_python_operator with a date in the past as execution date (This won’t trigger the tasks of that dag unless you set the option catchup=True in the DAG definition)


airflow dags trigger example_python_operator -e '2021-01-01 19:04:00+00:00'
* Trigger the dag example_python_operator with a date in the future (change the date here with one having +2 minutes later than the current date displayed in the Airflow UI). The dag will be scheduled at that date.


airflow dags list-runs -d example_python_operator
* Display the history of example_python_operator’s dag runs


airflow tasks list example_python_operator
* List the tasks contained into the example_python_operator dag


airflow tasks test example_python_operator print_the_context 2021-01-01
* Allow to test a task (print_the_context) from a given dag (example_python_operator here) without taking care of dependencies and past runs. Useful for debugging.

pip install fastapi
pip install uvicorn

uvicorn main:app --host "0.0.0.0" --port 8000 --reload