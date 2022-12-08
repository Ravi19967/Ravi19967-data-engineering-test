FROM rappdw/docker-java-python:openjdk1.8.0_171-python3.6.6
SHELL ["/bin/bash", "-c"] 
COPY requirements.txt .
COPY spark-requirements.txt .
RUN python -m venv pyspark_venv && source pyspark_venv/bin/activate && pip install --upgrade pip && pip install -r spark-requirements.txt && pex $(pip freeze) -o pyspark_venv.pex
RUN python -m venv logic_venv && source logic_venv/bin/activate && pip install --upgrade pip && pip install -r requirements.txt