# Base image for Apache Airflow
FROM apache/airflow:2.10.2-python3.10


# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow
ENV GCLOUD_HOME=/home/google-cloud-sdk
ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"
ENV GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/sbti-service-cred.json

# Set the Airflow user ID
ARG AIRFLOW_UID=50000

# Switch to root to install necessary tools
USER root

# Install necessary packages (gosu, curl, vim, unzip)
RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends gosu curl vim unzip && \
    rm -rf /var/lib/apt/lists/*

# Download and install Google Cloud SDK
RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-431.0.0-linux-x86_64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
    && mkdir -p "${GCLOUD_HOME}" \
    && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
    && "${GCLOUD_HOME}/install.sh" --bash-completion=false --path-update=false --usage-reporting=false --quiet \
    && rm -rf "${TMP_DIR}" \
    && gcloud --version

# Set the working directory
WORKDIR $AIRFLOW_HOME

# Switch away from root to install pip
USER $AIRFLOW_UID 


# Copy the requirements file and install Python packages
COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Create the data directory
RUN mkdir -p $AIRFLOW_HOME/data

# Copy Google Cloud credentials
COPY .google/sbti-service-cred.json /opt/airflow/sbti-service-cred.json




