FROM openjdk:11-jre-slim

LABEL maintainer="Data Platform Team"
LABEL service="datanormalization-service"

# Set working directory
WORKDIR /app

# Copy the jar file
COPY target/datanormalization-service-1.0.0.jar app.jar

# Expose port
EXPOSE 8084

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=40s --retries=3 \
  CMD curl -f http://localhost:8084/actuator/health || exit 1

# Run the application
ENTRYPOINT ["java", "-jar", "app.jar"]
