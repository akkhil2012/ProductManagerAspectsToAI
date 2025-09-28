# DataIngestion Service

This is a Spring Boot microservice for dataingestion operations in the data platform.

## Features

- RESTful API with CRUD operations
- Integration with Python FastAPI endpoints
- H2 in-memory database for development
- Comprehensive logging
- Health check endpoints
- Swagger documentation support

## API Endpoints

### Main CRUD Operations
- `POST /api/v1/dataingestion` - Create a new record
- `GET /api/v1/dataingestion` - Get all records (with optional filtering)
- `GET /api/v1/dataingestion/{id}` - Get record by ID
- `GET /api/v1/dataingestion/record/{recordId}` - Get record by record ID
- `PATCH /api/v1/dataingestion/{id}` - Update a record
- `DELETE /api/v1/dataingestion/{id}` - Delete a record

### Python Integration Endpoints
- `POST /api/v1/dataingestion/process` - Process record via Python FastAPI
- `POST /api/v1/dataingestion/validate` - Validate record via Python FastAPI

### Utility Endpoints
- `GET /api/v1/dataingestion/count?status={status}` - Get record count by status
- `GET /actuator/health` - Health check endpoint

## Running the Application

### Prerequisites
- Java 11 or higher
- Maven 3.6 or higher
- Python FastAPI service (optional, for integration features)

### Development Mode
```bash
mvn spring-boot:run
```

The application will start on port 8081.

### Building the Application
```bash
mvn clean package
```

### Running with Docker
```bash
docker build -t dataingestion-service .
docker run -p 8081:8081 dataingestion-service
```

## Configuration

### Database Configuration
The application uses H2 in-memory database by default. You can access the H2 console at:
```
http://localhost:8081/h2-console
```

Connection details:
- JDBC URL: `jdbc:h2:mem:dataingestiondb`
- Username: `sa`
- Password: `password`

### Python FastAPI Integration
Configure the Python FastAPI base URL in `application.properties`:
```properties
python.fastapi.base.url=http://localhost:8000
```

The service expects the following Python FastAPI endpoints:
- `POST /{app_name.lower()}/process` - For processing records
- `POST /{app_name.lower()}/validate` - For validating records
- `GET /{app_name.lower()}/status/{recordId}` - For getting processing status

## Sample Requests

### Create a Record
```bash
curl -X POST http://localhost:8081/api/v1/dataingestion \
  -H "Content-Type: application/json" \
  -d '{
    "recordId": "REC-001",
    "status": "PENDING",
    "dataPayload": "{\"data\": \"sample\"}"
  }'
```

### Get All Records
```bash
curl http://localhost:8081/api/v1/dataingestion
```

### Update a Record
```bash
curl -X PATCH http://localhost:8081/api/v1/dataingestion/1 \
  -H "Content-Type: application/json" \
  -d '{
    "status": "COMPLETED",
    "dataPayload": "{\"data\": \"updated\"}"
  }'
```

### Process via Python
```bash
curl -X POST http://localhost:8081/api/v1/dataingestion/process \
  -H "Content-Type: application/json" \
  -d '{
    "recordId": "REC-001",
    "dataPayload": "{\"data\": \"to_process\"}"
  }'
```

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Controller    │    │     Service      │    │   Repository    │
│                 │────│                  │────│                 │
│ - REST APIs     │    │ - Business Logic │    │ - Data Access   │
│ - Request/      │    │ - Python API     │    │ - JPA Queries   │
│   Response      │    │   Integration    │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌──────────────────┐
                       │   Python FastAPI │
                       │                  │
                       │ - Data Processing│
                       │ - Validation     │
                       │ - Status Updates │
                       └──────────────────┘
```

## Development Guidelines

1. All API responses follow a consistent format with `success`, `message`, `data`, and `timestamp` fields
2. Comprehensive error handling with appropriate HTTP status codes
3. Logging at INFO level for major operations and ERROR level for exceptions
4. Input validation using Bean Validation annotations
5. Transaction management with `@Transactional` where appropriate

## Testing

Run tests with:
```bash
mvn test
```

## Monitoring

- Health check: `GET /actuator/health`
- Application metrics: `GET /actuator/metrics`
- Application info: `GET /actuator/info`
