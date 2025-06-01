# Project: Webhook Automation Backend

This project is a backend system designed to emulate core functionalities of Zapier, focusing on webhook-triggered automation. It allows users to define workflows (Zaps) where incoming webhooks trigger a series of predefined actions. The system is built with a microservices architecture using Node.js, TypeScript, PostgreSQL, and Kafka to handle asynchronous task processing.

## Core Architecture

The system is designed with a microservices architecture to handle webhook processing and action execution in a scalable and resilient manner. The key components are:

*   **`primary-backend`**:
    *   **Role**: The main API for user interactions.
    *   **Responsibilities**: Handles user authentication, creation and management of Zaps (workflows), defining triggers (e.g., which webhook to listen to), and the sequence of actions for each Zap. All configurations are stored in the shared PostgreSQL database.

*   **`hooks`**:
    *   **Role**: Responsible for receiving all incoming webhooks.
    *   **Responsibilities**: When an external service sends a request to a registered webhook URL, this service captures the data. It then creates a `ZapRun` record (representing a specific instance of a Zap execution) and an entry in the `ZapRunOutbox` table in PostgreSQL. This outbox pattern ensures that the intention to run a Zap is durably stored before being passed to the asynchronous processing pipeline.

*   **`processor`**:
    *   **Role**: Acts as a bridge between the initial webhook reception and the action execution.
    *   **Responsibilities**: This service polls (or is triggered by) the `ZapRunOutbox` table. For each new entry, it retrieves the full `ZapRun` details and the corresponding `Zap` configuration (which includes the list of actions to perform). It then breaks down the Zap into individual action tasks and publishes these tasks as messages to the `zap-events` Kafka topic. Once messages are successfully sent to Kafka, the entry in `ZapRunOutbox` is typically marked as processed or removed.

*   **`worker`**:
    *   **Role**: Executes the individual actions defined in a Zap.
    *   **Responsibilities**: Workers subscribe to the `zap-events` Kafka topic where action tasks are published by the `processor`. Each worker consumes these messages and performs the designated action. This decoupled design allows for multiple workers to process actions in parallel, providing scalability.

*   **PostgreSQL Database**:
    *   A shared relational database used by `primary-backend`, `hooks`, and `processor`.
    *   Stores user information, Zap definitions (triggers, actions, sequences), `ZapRun` history, and the `ZapRunOutbox` table for reliable event handoff.

*   **Apache Kafka**:
    *   A distributed streaming platform used as a message broker.
    *   Decouples the `processor` (which decides what actions to run) from the `worker` services (which execute those actions). This improves fault tolerance and allows for independent scaling of components. The primary topic used is `zap-events`.

## Setup Instructions

This section guides you through setting up and running the entire system using Docker.

### Prerequisites

*   Docker installed and running.
*   A terminal or command prompt.

### 1. Create Docker Network

First, create a dedicated Docker network for the services to communicate with each other:

```bash
docker network create zapier-network
```

### 2. Start PostgreSQL Database

Run a PostgreSQL container:

```bash
docker run -d   --name postgres-db   --network zapier-network   -e POSTGRES_USER=myuser   -e POSTGRES_PASSWORD=mypassword   -e POSTGRES_DB=zapierdb   -p 5432:5432   -v postgres_data:/var/lib/postgresql/data   postgres:13-alpine
```

*   This command starts a PostgreSQL 13 instance.
*   `myuser`, `mypassword`, and `zapierdb` are example credentials. Change them if needed, and update the `DATABASE_URL` in subsequent steps accordingly.
*   Data will be persisted in a Docker volume named `postgres_data`.
*   The database will be accessible on `localhost:5432` from your host machine.
*   The `DATABASE_URL` for the services will be: `postgresql://myuser:mypassword@postgres-db:5432/zapierdb`

### 3. Start Kafka and ZooKeeper

**ZooKeeper (required by Kafka):**

```bash
docker run -d   --name zookeeper   --network zapier-network   -p 2181:2181   confluentinc/cp-zookeeper:latest   -e ZOOKEEPER_CLIENT_PORT=2181   -e ZOOKEEPER_TICK_TIME=2000
```

**Kafka:**

```bash
docker run -d   --name kafka   --network zapier-network   -p 9092:9092   -e KAFKA_BROKER_ID=1   -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181   -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:9092   -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT   -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT   -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1   confluentinc/cp-kafka:latest
```

*   Wait for ZooKeeper to start before starting Kafka.
*   Kafka will be accessible on `localhost:9092` from your host and as `kafka:9092` within the `zapier-network`.
*   The `KAFKA_BROKERS` environment variable for services will be `kafka:9092`.

### 4. Build and Run Node.js Services

For each of the Node.js services (`primary-backend`, `hooks`, `processor`, `worker`), you'll need a `Dockerfile`.

**Common `Dockerfile` for Node.js Services:**

Create a file named `Dockerfile` in the root directory of *each* service (`primary-backend/Dockerfile`, `hooks/Dockerfile`, etc.) with the following content:

```dockerfile
# Use an official Node.js runtime as a parent image
FROM node:18-alpine

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy package.json and package-lock.json
COPY package*.json ./

# Install project dependencies (including devDependencies for tsc and prisma)
RUN npm install

# Bundle app source
COPY . .

# Build TypeScript
RUN npx tsc -b

# Default command (will be overridden by some services if they don't expose ports)
CMD ["node", "dist/index.js"]
```

**Important Environment Variables:**

*   `DATABASE_URL`: `postgresql://myuser:mypassword@postgres-db:5432/zapierdb` (for services needing DB access)
*   `KAFKA_BROKERS`: `kafka:9092` (for services needing Kafka access)
*   `PORT`: Specific to `primary-backend` and `hooks`.
*   `JWT_PASSWORD`: For `primary-backend`.

**a. Database Initialization & Migrations**

Before starting the application services that depend on the database schema, apply Prisma migrations. You can do this using the image built for any service that includes Prisma (e.g., `primary-backend`).

First, build the `primary-backend` image (instructions below). Then run:

```bash
docker run --rm   --network zapier-network   -e DATABASE_URL="postgresql://myuser:mypassword@postgres-db:5432/zapierdb"   primary-backend-app   npx prisma migrate deploy
```
*(Ensure `primary-backend-app` image is built before running this command).*

**b. `primary-backend` Service**

*   Navigate to the `primary-backend` directory.
*   Create the `Dockerfile` as specified above.
*   Build the Docker image:
    ```bash
    docker build -t primary-backend-app .
    ```
*   Run the Docker container:
    ```bash
    docker run -d       --name primary-backend-service       --network zapier-network       -p 3001:3001       -e DATABASE_URL="postgresql://myuser:mypassword@postgres-db:5432/zapierdb"       -e JWT_PASSWORD="your_very_secret_jwt_password"       -e PORT="3001"       primary-backend-app
    ```
    *(Exposes on host port 3001, internal container port 3001)*

**c. `hooks` Service**

*   Navigate to the `hooks` directory.
*   Create the `Dockerfile` as specified above.
*   Modify the `Dockerfile`'s `CMD` to `CMD ["node", "dist/index.js"]` (if not already default) and you can add `EXPOSE 3002` for clarity.
*   Build the Docker image:
    ```bash
    docker build -t hooks-app .
    ```
*   Run the Docker container:
    ```bash
    docker run -d       --name hooks-service       --network zapier-network       -p 3002:3002       -e DATABASE_URL="postgresql://myuser:mypassword@postgres-db:5432/zapierdb"       -e PORT="3002"       hooks-app
    ```
    *(Exposes on host port 3002, internal container port 3002)*

**d. `processor` Service**

*   Navigate to the `processor` directory.
*   Create the `Dockerfile` as specified above. `CMD ["node", "dist/index.js"]` is appropriate.
*   Build the Docker image:
    ```bash
    docker build -t processor-app .
    ```
*   Run the Docker container:
    ```bash
    docker run -d       --name processor-service       --network zapier-network       -e DATABASE_URL="postgresql://myuser:mypassword@postgres-db:5432/zapierdb"       -e KAFKA_BROKERS="kafka:9092"       processor-app
    ```

**e. `worker` Service**

*   Navigate to the `worker` directory.
*   Create the `Dockerfile` as specified above. `CMD ["node", "dist/index.js"]` is appropriate.
*   Build the Docker image:
    ```bash
    docker build -t worker-app .
    ```
*   Run the Docker container:
    ```bash
    docker run -d       --name worker-service       --network zapier-network       -e KAFKA_BROKERS="kafka:9092"       worker-app
    ```

### 5. Order of Operations Summary

1.  Create `zapier-network`.
2.  Start `postgres-db`. Wait for it to be ready.
3.  Build `primary-backend-app` image.
4.  Run Prisma migrations using the `primary-backend-app` image.
5.  Start `zookeeper`. Wait for it to be ready.
6.  Start `kafka`. Wait for it to be ready.
7.  Build and run `primary-backend-service`.
8.  Build and run `hooks-service`.
9.  Build and run `processor-service`.
10. Build and run `worker-service`.

### Managing Services

*   **View logs:** `docker logs <container_name_or_id>`
*   **Stop a service:** `docker stop <container_name_or_id>`
*   **Start a stopped service:** `docker start <container_name_or_id>`
*   **Remove a service:** `docker rm <container_name_or_id>` (must be stopped first)
*   **Remove network:** `docker network rm zapier-network` (all containers using it must be stopped and removed)
*   **Remove volume (PostgreSQL data):** `docker volume rm postgres_data` (be careful, this deletes data)

This comprehensive README should provide all necessary instructions.
