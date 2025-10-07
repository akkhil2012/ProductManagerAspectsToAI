# Data Platform Microservices - Spring Boot Applications

This project contains six individual Spring Boot microservices for a comprehensive data platform:

## Services Overview

| Service | Port | Purpose | Description |
|---------|------|---------|-------------|
| DataIngestion | 8081 | Data Ingestion | Handles incoming data from various sources |
| DataDeduplication | 8082 | Data Deduplication | Removes duplicate records from datasets |
| DataQuality | 8083 | Data Quality | Validates and ensures data quality standards |
| DataNormalization | 8084 | Data Normalization | Standardizes and normalizes data formats |
| DataStorage | 8085 | Data Storage | Manages data storage operations |
| DataConsumption | 8086 | Data Consumption | Handles data retrieval and consumption |

## Architecture Overview

Each microservice follows the same architectural pattern:

```
├── Controller Layer (REST APIs)
├── Service Layer (Business Logic + Python API Integration)
├── Repository Layer (Data Access)
└── Model Layer (Entity Classes)
```

## Key Features

### ✅ Complete CRUD Operations
- **POST** - Create new records
- **GET** - Retrieve records (with filtering options)
- **PATCH** - Update existing records  
- **DELETE** - Remove records

### ✅ Python FastAPI Integration
Each service can call corresponding Python FastAPI endpoints:
- `/process` - Process data via Python
- `/validate` - Validate data via Python
- `/status/{recordId}` - Get processing status

### ✅ Production-Ready Features
- Comprehensive logging
- Error handling with proper HTTP status codes
- Input validation
- Health check endpoints
- Metrics and monitoring
- Docker support

## Quick Start

### Prerequisites
- Java 11+
- Maven 3.6+
- Docker (optional)

### Run All Services with Docker
```bash
docker-compose up -d
```

### Run Individual Service
```bash
cd dataingestion-service
mvn spring-boot:run
```

### Orchestrate the Services into a Pipeline

Use the Python helper found in `pipeline/run_pipeline.py` to trigger each service sequentially with a single command. The script
reads `pipeline/pipeline_config.yaml` for endpoint locations and `pipeline/sample_data.json` as example input. Run in simulate
mode to validate the wiring without starting the Java applications:

```bash
python pipeline/run_pipeline.py --simulate --log-level DEBUG
```

## API Documentation

Each service exposes RESTful APIs at `/api/v1/{service-name}`:

### Standard Endpoints (All Services)
```
POST   /api/v1/{service}/              # Create record
GET    /api/v1/{service}/              # Get all records
GET    /api/v1/{service}/{id}          # Get record by ID
PATCH  /api/v1/{service}/{id}          # Update record
DELETE /api/v1/{service}/{id}          # Delete record
```

### Python Integration Endpoints
```
POST   /api/v1/{service}/process       # Process via Python
POST   /api/v1/{service}/validate      # Validate via Python
```

### Utility Endpoints
```
GET    /api/v1/{service}/count         # Get record count
GET    /actuator/health               # Health check
```

## Service Configuration

Each service uses H2 in-memory database by default and can be configured to connect to external databases. Python FastAPI integration is configurable via `application.properties`.

## Directory Structure

```
data-platform-microservices/
├── dataingestion-service/
│   ├── src/main/java/com/dataplatform/dataingestion/
│   │   ├── controller/DataIngestionController.java
│   │   ├── service/DataIngestionService.java
│   │   ├── service/DataIngestionServiceImpl.java
│   │   ├── repository/DataIngestionRepository.java
│   │   ├── repository/DataIngestionRepositoryImpl.java
│   │   ├── model/DataIngestionRecord.java
│   │   └── config/DataIngestionConfig.java
│   ├── src/main/resources/application.properties
│   ├── pom.xml
│   ├── Dockerfile
│   └── README.md
├── (similar structure for other 5 services)
├── docker-compose.yml
└── PROJECT_OVERVIEW.md
```

## Development Guidelines

1. **Consistent API Response Format**
   ```json
   {
     "success": true,
     "message": "Operation completed successfully",
     "data": {...},
     "timestamp": "2025-09-28T12:00:00"
   }
   ```

2. **Error Handling**
   - Proper HTTP status codes
   - Detailed error messages
   - Exception logging

3. **Python Integration**
   - Configurable FastAPI endpoints
   - Retry logic for failed calls
   - Proper error propagation

## Testing

Each service includes:
- Unit tests for service layer
- Integration tests for controllers
- Repository tests with test containers

Run tests:
```bash
mvn test
```

## Monitoring & Operations

- **Health Checks**: `/actuator/health`
- **Metrics**: `/actuator/metrics`
- **Application Info**: `/actuator/info`
- **H2 Console**: `/h2-console` (development only)

## Deployment

### Local Development
```bash
mvn spring-boot:run
```

### Docker Deployment
```bash
docker-compose up -d
```

### Production Deployment
- Build with `mvn clean package`
- Deploy JAR files to your container orchestration platform
- Configure external databases and Python API endpoints

## Next Steps

1. **Database Integration**: Replace H2 with production databases (PostgreSQL, MySQL, etc.)
2. **Security**: Add authentication and authorization
3. **API Documentation**: Integrate Swagger/OpenAPI
4. **Observability**: Add distributed tracing with Zipkin/Jaeger
5. **CI/CD**: Set up automated testing and deployment pipelines

## Support

For questions and support, refer to individual service README files or contact the Data Platform Team.
