# Kafka Demo - Asynchronous Request Processing with Apache Kafka

## Project Purpose

This project demonstrates Apache Kafka's ability to handle high concurrent user requests through asynchronous processing. Instead of processing requests synchronously, the application publishes each request to a Kafka topic, where it's consumed and processed separately. This architecture enables:

- **High throughput**: Handle thousands of requests per second
- **Decoupling**: Separate request ingestion from processing
- **Scalability**: Add more consumers without changing producers
- **Resilience**: Buffer requests during processing delays

This is Phase 1 of a larger project, designed with clean, modular architecture for easy extension.

## Architecture

```
┌─────────────┐      HTTP 202      ┌──────────────┐
│   Client    │ ──────────────────> │   REST API   │
└─────────────┘                     └──────────────┘
                                            │
                                            │ Publish
                                            ▼
┌─────────────┐      Consume      ┌──────────────┐
│   Kafka UI  │ <──────────────── │   Kafka      │
└─────────────┘                     └──────────────┘
                                            │
                                            │ Subscribe
                                            ▼
                                    ┌──────────────┐
                                    │   Consumer   │
                                    └──────────────┘
```

### Components

1. **REST API** (`UserRequestController`): Receives HTTP POST requests and returns immediately with HTTP 202 Accepted
2. **Kafka Producer** (`UserRequestProducer`): Serializes and publishes requests to Kafka topic
3. **Kafka Broker**: Apache Kafka running in Docker (KRaft mode - no Zookeeper)
4. **Kafka Consumer** (`UserRequestConsumer`): Subscribes to topic and processes messages asynchronously
5. **Kafka UI**: Web interface for monitoring Kafka clusters

## Kafka Message Flow

1. Client sends POST request to `/requests` with JSON payload
2. Controller validates and creates `UserRequest` object
3. Producer serializes object to JSON and publishes to `user-requests` topic
4. Controller returns HTTP 202 Accepted immediately (non-blocking)
5. Kafka stores message in the topic
6. Consumer polls topic and receives message
7. Consumer deserializes JSON and logs the request details

## Tech Stack

- **Java 21**: Modern Java with virtual threads support
- **Spring Boot 3.2.0**: Latest Spring Boot framework
- **Spring for Apache Kafka**: Kafka integration with Spring
- **Maven**: Dependency management and build tool
- **Docker & Docker Compose**: Container orchestration
- **Apache Kafka (KRaft mode)**: Event streaming platform without Zookeeper
- **Kafka UI**: Web-based Kafka management interface

## Folder Structure

```
kafka-demo/
├── src/
│   └── main/
│       ├── java/com/proj/kafka_demo/
│       │   ├── KafkaDemoApplication.java          # Main Spring Boot application
│       │   ├── config/
│       │   │   └── KafkaConfig.java               # Kafka topic configuration
│       │   ├── controller/
│       │   │   └── UserRequestController.java     # REST API endpoint
│       │   ├── kafka/
│       │   │   ├── producer/
│       │   │   │   └── UserRequestProducer.java   # Kafka message publisher
│       │   │   └── consumer/
│       │   │       └── UserRequestConsumer.java   # Kafka message listener
│       │   └── model/
│       │       └── UserRequest.java               # Data model
│       └── resources/
│           └── application.yml                     # Application configuration
├── docker-compose.yml                              # Kafka and Kafka UI containers
├── pom.xml                                         # Maven dependencies
├── postman-collection.json                         # API testing collection
└── README.md                                       # This file
```

## How to Run

### Prerequisites

- Java 21 installed
- Maven installed
- Docker and Docker Compose installed

### Step 1: Start Kafka Infrastructure

```powershell
docker-compose up -d
```

This starts:
- Kafka broker on port `9092` (client connections)
- Kafka controller on port `9093` (KRaft quorum)
- Kafka UI on port `8080` (web interface)

### Step 2: Verify Kafka is Running

```powershell
docker-compose ps
```

Both containers should show status "Up".

### Step 3: Access Kafka UI

Open browser: `http://localhost:8080`

You should see the "local" cluster connected.

### Step 4: Start Spring Boot Application

```powershell
mvn spring-boot:run
```

The application will start on port `8081`.

## How to Test

### Using Postman

1. Import `postman-collection.json` into Postman
2. Send a POST request to `http://localhost:8081/requests`
3. Request body:
```json
{
  "userId": 1,
  "username": "Alice",
  "action": "Login"
}
```
4. Expected response: HTTP 202 Accepted with message "Request accepted for processing"
5. Check console logs to see consumer output:
```
Received Request:
User: Alice
Action: Login
Timestamp: 2024-08-09T01:30:00
```

### Using cURL

```powershell
curl -X POST http://localhost:8081/requests ^
  -H "Content-Type: application/json" ^
  -d "{\"userId\": 1, \"username\": \"Alice\", \"action\": \"Login\"}"
```

### Using Kafka UI

1. Navigate to `http://localhost:8080`
2. Click on "local" cluster
3. Go to "Topics" tab
4. Click on "user-requests" topic
5. View messages in real-time

## Configuration

Key configuration in `application.yml`:

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092    # Kafka broker address
    producer:
      key-serializer: StringSerializer
      value-serializer: StringSerializer
    consumer:
      group-id: user-request-consumer-group
      auto-offset-reset: earliest

kafka:
  topic:
    user-requests: user-requests
```

## Future Enhancements

The architecture is designed for easy extension:

### Multiple Consumers
- Add more `@KafkaListener` methods with same or different group IDs
- Scale horizontally by running multiple application instances

### Consumer Groups
- Create different consumer groups for different processing pipelines
- Example: analytics group, notification group, persistence group

### Multiple Partitions
- Increase partition count in `KafkaConfig.java`
- Enables parallel processing by multiple consumers
- Improves throughput for high-volume scenarios

### PostgreSQL Integration
- Add Spring Data JPA dependency
- Create entity classes and repositories
- Persist user requests to database in consumer

### Redis Integration
- Add Spring Data Redis dependency
- Cache frequently accessed data
- Use for rate limiting and session management

### Notification Service
- Create separate microservice for notifications
- Add Kafka consumer to listen for user requests
- Send email/SMS/push notifications based on actions

### Analytics Service
- Create separate microservice for analytics
- Aggregate and analyze user behavior
- Generate reports and dashboards

### Monitoring with Prometheus
- Add Spring Boot Actuator and Micrometer dependencies
- Expose metrics endpoint
- Configure Prometheus to scrape metrics

### Visualization with Grafana
- Set up Grafana dashboard
- Connect to Prometheus datasource
- Create visualizations for request rates, consumer lag, etc.

### Microservices Architecture
- Split into separate services: API Gateway, User Service, Notification Service
- Use Spring Cloud for service discovery and configuration
- Implement API Gateway for routing

## Design Decisions

### KRaft Mode
- Eliminates Zookeeper dependency
- Simpler architecture with fewer components
- Better performance and reduced operational overhead

### String Serialization
- Simple approach for Phase 1
- JSON conversion handled manually with Jackson
- Future: Use JsonSerializer for direct object serialization

### Single Partition
- Sufficient for demonstration
- Maintains message ordering
- Future: Increase partitions for parallel processing

### HTTP 202 Accepted
- Demonstrates asynchronous processing
- Client doesn't wait for processing to complete
- Enables high throughput under load

### Constructor Injection
- Spring Boot best practice
- Better testability and immutability
- Clear dependency requirements

## Troubleshooting

### Kafka Container Fails to Start
```powershell
docker-compose logs kafka
```
Check if port 9092 or 9093 is already in use.

### Consumer Not Receiving Messages
- Verify Kafka is running: `docker-compose ps`
- Check topic exists in Kafka UI
- Review consumer group configuration in application.yml
- Check application logs for connection errors

### Port Conflicts
- Kafka: 9092, 9093
- Kafka UI: 8080
- Spring Boot: 8081
- Change ports in docker-compose.yml or application.yml if needed

## Stopping the Application

```powershell
# Stop Spring Boot (Ctrl+C in terminal)

# Stop Docker containers
docker-compose down

# Stop and remove volumes (clean slate)
docker-compose down -v
```

## License

This is a demo project for educational purposes.