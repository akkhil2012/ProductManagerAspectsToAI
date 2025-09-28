package com.dataplatform.dataingestion.controller;

import com.dataplatform.dataingestion.model.DataIngestionRecord;
import com.dataplatform.dataingestion.service.DataIngestionService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;
import java.util.Map;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RestController
@RequestMapping("/api/v1/dataingestion")
@CrossOrigin(origins = "*")
public class DataIngestionController {

    private static final Logger logger = LoggerFactory.getLogger(DataIngestionController.class);

    @Autowired
    private DataIngestionService dataingestionService;

    /**
     * Create a new dataingestion record
     * POST /api/v1/dataingestion
     */
    @PostMapping
    public ResponseEntity<Map<String, Object>> createRecord(@Valid @RequestBody DataIngestionRecord record) {
        logger.info("POST request to create dataingestion record with recordId: {}", record.getRecordId());

        try {
            DataIngestionRecord createdRecord = dataingestionService.createRecord(record);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "DataIngestion record created successfully");
            response.put("data", createdRecord);
            response.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.CREATED).body(response);

        } catch (Exception e) {
            logger.error("Error creating dataingestion record: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to create dataingestion record");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Get all dataingestion records
     * GET /api/v1/dataingestion
     */
    @GetMapping
    public ResponseEntity<Map<String, Object>> getAllRecords(
            @RequestParam(required = false) String status,
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate) {

        logger.info("GET request to fetch all dataingestion records");

        try {
            List<DataIngestionRecord> records;

            if (status != null && !status.isEmpty()) {
                records = dataingestionService.getRecordsByStatus(status);
            } else if (startDate != null && endDate != null) {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
                LocalDateTime start = LocalDateTime.parse(startDate, formatter);
                LocalDateTime end = LocalDateTime.parse(endDate, formatter);
                records = dataingestionService.getRecordsByDateRange(start, end);
            } else {
                records = dataingestionService.getAllRecords();
            }

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "Records retrieved successfully");
            response.put("data", records);
            response.put("count", records.size());
            response.put("timestamp", LocalDateTime.now());

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error fetching dataingestion records: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to fetch dataingestion records");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Get a dataingestion record by ID
     * GET /api/v1/dataingestion/{id}
     */
    @GetMapping("/{id}")
    public ResponseEntity<Map<String, Object>> getRecordById(@PathVariable Long id) {
        logger.info("GET request to fetch dataingestion record with id: {}", id);

        try {
            Optional<DataIngestionRecord> record = dataingestionService.getRecordById(id);

            if (record.isPresent()) {
                Map<String, Object> response = new HashMap<>();
                response.put("success", true);
                response.put("message", "DataIngestion record found");
                response.put("data", record.get());
                response.put("timestamp", LocalDateTime.now());

                return ResponseEntity.ok(response);
            } else {
                Map<String, Object> response = new HashMap<>();
                response.put("success", false);
                response.put("message", "DataIngestion record not found");
                response.put("timestamp", LocalDateTime.now());

                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(response);
            }

        } catch (Exception e) {
            logger.error("Error fetching dataingestion record by id: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to fetch dataingestion record");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Get a dataingestion record by record ID
     * GET /api/v1/dataingestion/record/{recordId}
     */
    @GetMapping("/record/{recordId}")
    public ResponseEntity<Map<String, Object>> getRecordByRecordId(@PathVariable String recordId) {
        logger.info("GET request to fetch dataingestion record with recordId: {}", recordId);

        try {
            Optional<DataIngestionRecord> record = dataingestionService.getRecordByRecordId(recordId);

            if (record.isPresent()) {
                Map<String, Object> response = new HashMap<>();
                response.put("success", true);
                response.put("message", "DataIngestion record found");
                response.put("data", record.get());
                response.put("timestamp", LocalDateTime.now());

                return ResponseEntity.ok(response);
            } else {
                Map<String, Object> response = new HashMap<>();
                response.put("success", false);
                response.put("message", "DataIngestion record not found");
                response.put("timestamp", LocalDateTime.now());

                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(response);
            }

        } catch (Exception e) {
            logger.error("Error fetching dataingestion record by recordId: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to fetch dataingestion record");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Update a dataingestion record
     * PATCH /api/v1/dataingestion/{id}
     */
    @PatchMapping("/{id}")
    public ResponseEntity<Map<String, Object>> updateRecord(@PathVariable Long id, @RequestBody DataIngestionRecord record) {
        logger.info("PATCH request to update dataingestion record with id: {}", id);

        try {
            DataIngestionRecord updatedRecord = dataingestionService.updateRecord(id, record);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "DataIngestion record updated successfully");
            response.put("data", updatedRecord);
            response.put("timestamp", LocalDateTime.now());

            return ResponseEntity.ok(response);

        } catch (RuntimeException e) {
            logger.error("Error updating dataingestion record: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(errorResponse);

        } catch (Exception e) {
            logger.error("Error updating dataingestion record: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to update dataingestion record");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Delete a dataingestion record
     * DELETE /api/v1/dataingestion/{id}
     */
    @DeleteMapping("/{id}")
    public ResponseEntity<Map<String, Object>> deleteRecord(@PathVariable Long id) {
        logger.info("DELETE request to delete dataingestion record with id: {}", id);

        try {
            // Check if record exists before deleting
            Optional<DataIngestionRecord> existingRecord = dataingestionService.getRecordById(id);

            if (existingRecord.isPresent()) {
                dataingestionService.deleteRecord(id);

                Map<String, Object> response = new HashMap<>();
                response.put("success", true);
                response.put("message", "DataIngestion record deleted successfully");
                response.put("timestamp", LocalDateTime.now());

                return ResponseEntity.ok(response);
            } else {
                Map<String, Object> response = new HashMap<>();
                response.put("success", false);
                response.put("message", "DataIngestion record not found");
                response.put("timestamp", LocalDateTime.now());

                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(response);
            }

        } catch (Exception e) {
            logger.error("Error deleting dataingestion record: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to delete dataingestion record");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Process record via Python FastAPI
     * POST /api/v1/dataingestion/process
     */
    @PostMapping("/process")
    public ResponseEntity<Map<String, Object>> processRecordViaPython(@RequestBody Map<String, String> request) {
        logger.info("POST request to process dataingestion record via Python");

        try {
            String recordId = request.get("recordId");
            String dataPayload = request.get("dataPayload");

            DataIngestionRecord processedRecord = dataingestionService.processRecordViaPython(recordId, dataPayload);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "DataIngestion record processed successfully via Python");
            response.put("data", processedRecord);
            response.put("timestamp", LocalDateTime.now());

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error processing dataingestion record via Python: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to process dataingestion record via Python");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Validate record via Python FastAPI
     * POST /api/v1/dataingestion/validate
     */
    @PostMapping("/validate")
    public ResponseEntity<Map<String, Object>> validateRecordViaPython(@RequestBody Map<String, String> request) {
        logger.info("POST request to validate dataingestion record via Python");

        try {
            String recordId = request.get("recordId");
            String dataPayload = request.get("dataPayload");

            boolean isValid = dataingestionService.validateRecordViaPython(recordId, dataPayload);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "DataIngestion record validation completed");
            response.put("isValid", isValid);
            response.put("timestamp", LocalDateTime.now());

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error validating dataingestion record via Python: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to validate dataingestion record via Python");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Get record count by status
     * GET /api/v1/dataingestion/count
     */
    @GetMapping("/count")
    public ResponseEntity<Map<String, Object>> getRecordCountByStatus(@RequestParam String status) {
        logger.info("GET request to get count of dataingestion records with status: {}", status);

        try {
            Long count = dataingestionService.getRecordCountByStatus(status);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "Record count retrieved successfully");
            response.put("status", status);
            response.put("count", count);
            response.put("timestamp", LocalDateTime.now());

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error getting count of dataingestion records: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to get record count");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }
}