
# Kinesis Stream Consumer Application

## Table of Contents
- [Introduction](#introduction)
- [Architecture Overview](#architecture-overview)
- [Features](#features)
- [Design Considerations](#design-considerations)
- [Application Structure](#application-structure)
- [Configuration](#configuration)
- [Build and Run Instructions](#build-and-run-instructions)
- [Usage](#usage)
- [Future Development](#future-development)
- [Security Considerations](#security-considerations)
- [Conclusion](#conclusion)

## Introduction
This application is a Kinesis Stream Consumer built with Spring Boot in Java. It reads events from an AWS Kinesis stream, processes them according to specified requirements, and outputs the results in a structured JSON format. The primary goals are to:

- Compute unique player logins.
- Compute unique player logins by country.

The application efficiently handles duplicates, multiple shards, and accommodates different schema versions of incoming events. It is designed with scalability, configurability, and robustness in mind, making it suitable for production environments.

## Architecture Overview
### High-Level Architecture
```
+---------------------------+
|    AWS Kinesis Stream     |
| (Multiple Shards, Events) |
+------------+--------------+
             |
             v
+---------------------------+
|    KinesisConsumerService |
| (Reads from Kinesis,      |
|  Processes Shards)        |
+------------+--------------+
             |
             v
+---------------------------+
|   RecordProcessingService |
| (Processes Records,       |
|  Handles Duplicates,      |
|  Manages Aggregations)    |
+------------+--------------+
             |
             v
+---------------------------+
|    Output Results to      |
|    JSON Files             |
+---------------------------+
```

### Components
- **KinesisConsumerService**: Handles connection to the Kinesis stream, retrieves shards, and initiates shard processing.
- **ShardProcessingService**: Processes each shard asynchronously in its own thread to handle multiple shards concurrently.
- **RecordProcessingService**: Processes individual records, handles duplicates, and aggregates data.
- **Deduplication Mechanism**: Uses a sliding window to eliminate duplicate events.
- **Scheduler**: Outputs aggregated results at regular intervals.

## Features
- **Efficient Duplicate Handling**: Implements a sliding window deduplication mechanism to filter out duplicate player login events.
- **Multiple Shard Processing**: Processes multiple shards concurrently using asynchronous methods.
- **Schema Version Support**: Supports multiple versions of event schemas using Protocol Buffers.
- **Scalability**: Designed to scale horizontally by processing shards in parallel and handling high-throughput data streams.
- **Configurability**: Offers configurable buffer sizes, time intervals, and AWS settings via `application.yml`.
- **Robustness**: Includes error handling, retries, and exception logging to ensure continuous operation.

## Design Considerations

### Handling Duplicates
- **Sliding Window Deduplication**: Utilizes a time-based sliding window to track unique player IDs within a specified timeframe.
- **Thread-Safe Collections**: Employs concurrent data structures to ensure thread safety during aggregation.

### Processing Multiple Shards
- **Asynchronous Processing**: Processes each shard asynchronously using Spring's `@Async` annotation and a custom thread pool.
- **Non-Blocking Main Thread**: Ensures the main application thread remains unblocked to handle other tasks like scheduling.

### Accommodating Different Schema Versions
- **Protocol Buffers Support**: Parses events defined in Protocol Buffers with payloads in JSON format.
- **Version Handling Logic**: Implements logic to detect and process different schema versions (e.g., `LoginMessageV1`, `LoginMessageV2`).

### Scalability
- **Asynchronous Execution**: Leverages asynchronous processing to handle high-throughput data streams.
- **Configurable Thread Pool**: Allows customization of the thread pool size to match system capabilities.
- **Buffering Mechanism**: Buffers incoming events based on size or time to optimize processing.

### Configurability
- **External Configuration**: Uses `application.yml` for setting application parameters, making it easy to adjust settings without code changes.
- **Environment Profiles**: Supports multiple environments (dev, test, preprod, prod) with profile-specific configurations.

### Robustness
- **Error Handling and Logging**: Captures and logs exceptions to prevent the application from crashing and to facilitate troubleshooting.
- **Retry Mechanisms**: Implements retry logic for shard processing in case of transient errors.

## Application Structure
### Package Overview
```
src/main/java/com/assignment/task1/
├── config/
│   ├── AppConfig.java
│   ├── AsyncConfig.java
│   └── SchedulerConfig.java
├── deduplication/
│   └── SlidingWindowDeduplication.java
├── dto/
│   ├── TotalUniquePlayerLogins.java
│   └── UniquePlayerLoginsByCountry.java
├── processor/
│   └── RecordProcessor.java
├── service/
│   ├── KinesisConsumerService.java
│   ├── RecordProcessingService.java
│   └── ShardProcessingService.java
└── SpringbootKinesisConsumerApplication.java
```

### Key Components
- **config Package**: Contains configuration classes for application settings, asynchronous execution, and scheduling.
  - `AppConfig`: Binds application properties from `application.yml`.
  - `AsyncConfig`: Configures the thread pool for asynchronous execution.
  - `SchedulerConfig`: Configures the scheduler for scheduled tasks.
- **deduplication Package**: `SlidingWindowDeduplication` implements deduplication logic using a time-based sliding window.
- **dto Package**: 
  - `TotalUniquePlayerLogins`: Data Transfer Object for total unique player logins.
  - `UniquePlayerLoginsByCountry`: DTO for unique player logins by country.
- **processor Package**: `RecordProcessor` handles parsing of different schema versions of incoming events.
- **service Package**: 
  - `KinesisConsumerService`: Manages the connection to Kinesis and initiates shard processing.
  - `ShardProcessingService`: Processes each shard asynchronously to handle multiple shards.
  - `RecordProcessingService`: Processes individual records and aggregates data.
- **SpringbootKinesisConsumerApplication**: The main application class that bootstraps the Spring Boot application.

## Configuration
Configuration is managed via `application.yml` files, with support for different environments through Spring Profiles.

### Example application.yml
```yaml
spring:
  application:
    name: kinesis-consumer-app

app:
  aws:
    stream-name: your-stream-name
    access-key: ${AWS_ACCESS_KEY}
    secret-key: ${AWS_SECRET_KEY}
    region: us-east-1
  buffer:
    size: 1000
    time-ms: 5000
  output:
    frequency-ms: 60000

logging:
  level:
    root: INFO
    com.assignment.task1: INFO
```

### Environment-Specific Configurations
Create `application-{profile}.yml` files (e.g., `application-dev.yml`, `application-prod.yml`) for different environments and activate the appropriate profile when running the application.

## Build and Run Instructions
### Prerequisites
- Java Development Kit (JDK) 17 or higher
- Apache Maven 3.6+
- AWS Credentials: Set up AWS credentials with access to the Kinesis stream.

### Building the Project
1. Clone the Repository
```bash
git clone https://github.com/4nn4M0r0z/KCAv2.git
cd KCAv2
```

2. Configure AWS Credentials
Ensure your AWS credentials and other values are set in `application.yml` 

3. Edit Configuration
Modify `application.yml` or the environment-specific configuration file with the appropriate settings.

4. Build the Project
```bash
mvn clean package
```

### Running the Application
1. Run with Maven
```bash
mvn spring-boot:run
```

## Usage
### Output Files
The application outputs results in JSON format to the output directory, organized by metric name, date, and hour.

#### Total Unique Player Logins
```
output/
└── metric_name=TotalUniquePlayerLogins/
    └── date=YYYY-MM-DD/
        └── hour=HH/
            └── total_unique_player_logins_TIMESTAMP.json
```

#### Unique Player Logins by Country
```
output/
└── metric_name=UniquePlayerLoginsByCountry/
    └── date=YYYY-MM-DD/
        └── hour=HH/
            └── unique_player_logins_by_country_TIMESTAMP.json
```

### Example of `total_unique_player_logins_TIMESTAMP.json`:
```json
{
  "date": "2024-09-30",
  "hour": "14",
  "minute": "30",
  "metricName": "TotalUniquePlayerLogins",
  "loginCount": 12345
}
```

### Example of `unique_player_logins_by_country_TIMESTAMP.json`:
```json
[
  {
    "date": "2024-09-30",
    "hour": "14",
    "minute": "30",
    "metricName": "UniquePlayerLoginsByCountry",
    "country": "US",
    "loginCount": 6789
  },
  {
    "date": "2024-09-30",
    "hour": "14",
    "minute": "30",
    "metricName": "UniquePlayerLoginsByCountry",
    "country": "CA",
    "loginCount": 1234
  }
]
```

## Future Development
- **Dynamic Scaling**: Implement auto-scaling of consumer instances based on stream throughput.
- **Monitoring and Alerting**: Integrate with monitoring tools to track application performance and set up alerts for failures or high error rates.
- **Advanced Deduplication**: Enhance the deduplication mechanism to handle more complex scenarios and larger windows.
- **Schema Evolution Handling**: Introduce a more robust system for handling evolving schemas, possibly using a schema registry.
- **Containerization**: Dockerize the application for easier deployment and orchestration.

## Security Considerations
- **Passing credentials**: Ensure and test that credentials such as keys can be passed through environment variables, which is a safer way than having them in the yml files. 
- **Credential Management**: Use AWS IAM roles and instance profiles instead of hardcoding credentials. 
- **Validation**: Implement input validation to prevent processing of malformed or malicious data.
- **Logging Sensitive Data**: Avoid logging sensitive information such as player IDs or personal data that might be appearing in future schemas

## Conclusion
This Kinesis Stream Consumer application is a robust and scalable solution for processing streaming data from AWS Kinesis. It efficiently handles duplicates, supports multiple shards and schema versions, and outputs aggregated metrics in a structured format. The application's design focuses on scalability, configurability, and robustness.
