# builder step used to download and configure spark environment
FROM bitnami/spark:3.5

USER root

# Install additional packages
RUN apt-get update && \
    apt-get install -y openssh-server sudo wget

# Configure SSH
RUN sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config

# Add a user named "me" with UID 1001 and set password to "changeme"
RUN useradd --uid 1001 --create-home --shell /bin/bash --user-group me && \
    echo 'me:changeme' | chpasswd

# Add "me" to the sudoers file
RUN echo 'me ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/me

RUN wget https://jdbc.postgresql.org/download/postgresql-42.7.1.jar
RUN mv postgresql-42.7.1.jar /opt/bitnami/spark/jars

# Start SSH server in the background
CMD sudo service ssh start && /opt/bitnami/scripts/spark/run.sh

USER me