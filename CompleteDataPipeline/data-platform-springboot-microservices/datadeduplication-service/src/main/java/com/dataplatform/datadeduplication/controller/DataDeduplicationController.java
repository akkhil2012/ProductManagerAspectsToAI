package com.dataplatform.datadeduplication.controller;

import com.dataplatform.datadeduplication.model.DataDeduplicationRecord;
import com.dataplatform.datadeduplication.service.DataDeduplicationService;

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
@RequestMapping("/api/v1/datadeduplication")
@CrossOrigin(origins = "*")
public class DataDeduplicationController {

    private static final Logger logger = LoggerFactory.getLogger(DataDeduplicationController.class);

    @Autowired
    private DataDeduplicationService datadeduplicationService;

    /**
     * Create a new datadeduplication record
     * POST /api/v1/datadeduplication
     */
    @PostMapping
    public ResponseEntity<Map<String, Object>> createRecord(@Valid @RequestBody DataDeduplicationRecord record) {
        logger.info("POST request to create datadeduplication record with recordId: {}", record.getRecordId());

        try {
            DataDeduplicationRecord createdRecord = datadeduplicationService.createRecord(record);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "DataDeduplication record created successfully");
            response.put("data", createdRecord);
            response.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.CREATED).body(response);

        } catch (Exception e) {
            logger.error("Error creating datadeduplication record: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to create datadeduplication record");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Get all datadeduplication records
     * GET /api/v1/datadeduplication
     */
    @GetMapping
    public ResponseEntity<Map<String, Object>> getAllRecords(
            @RequestParam(required = false) String status,
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate) {

        logger.info("GET request to fetch all datadeduplication records");

        try {
            List<DataDeduplicationRecord> records;

            if (status != null && !status.isEmpty()) {
                records = datadeduplicationService.getRecordsByStatus(status);
            } else if (startDate != null && endDate != null) {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
                LocalDateTime start = LocalDateTime.parse(startDate, formatter);
                LocalDateTime end = LocalDateTime.parse(endDate, formatter);
                records = datadeduplicationService.getRecordsByDateRange(start, end);
            } else {
                records = datadeduplicationService.getAllRecords();
            }

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "Records retrieved successfully");
            response.put("data", records);
            response.put("count", records.size());
            response.put("timestamp", LocalDateTime.now());

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error fetching datadeduplication records: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to fetch datadeduplication records");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Get a datadeduplication record by ID
     * GET /api/v1/datadeduplication/{id}
     */
    @GetMapping("/{id}")
    public ResponseEntity<Map<String, Object>> getRecordById(@PathVariable Long id) {
        logger.info("GET request to fetch datadeduplication record with id: {}", id);

        try {
            Optional<DataDeduplicationRecord> record = datadeduplicationService.getRecordById(id);

            if (record.isPresent()) {
                Map<String, Object> response = new HashMap<>();
                response.put("success", true);
                response.put("message", "DataDeduplication record found");
                response.put("data", record.get());
                response.put("timestamp", LocalDateTime.now());

                return ResponseEntity.ok(response);
            } else {
                Map<String, Object> response = new HashMap<>();
                response.put("success", false);
                response.put("message", "DataDeduplication record not found");
                response.put("timestamp", LocalDateTime.now());

                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(response);
            }

        } catch (Exception e) {
            logger.error("Error fetching datadeduplication record by id: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to fetch datadeduplication record");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Get a datadeduplication record by record ID
     * GET /api/v1/datadeduplication/record/{recordId}
     */
    @GetMapping("/record/{recordId}")
    public ResponseEntity<Map<String, Object>> getRecordByRecordId(@PathVariable String recordId) {
        logger.info("GET request to fetch datadeduplication record with recordId: {}", recordId);

        try {
            Optional<DataDeduplicationRecord> record = datadeduplicationService.getRecordByRecordId(recordId);

            if (record.isPresent()) {
                Map<String, Object> response = new HashMap<>();
                response.put("success", true);
                response.put("message", "DataDeduplication record found");
                response.put("data", record.get());
                response.put("timestamp", LocalDateTime.now());

                return ResponseEntity.ok(response);
            } else {
                Map<String, Object> response = new HashMap<>();
                response.put("success", false);
                response.put("message", "DataDeduplication record not found");
                response.put("timestamp", LocalDateTime.now());

                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(response);
            }

        } catch (Exception e) {
            logger.error("Error fetching datadeduplication record by recordId: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to fetch datadeduplication record");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Update a datadeduplication record
     * PATCH /api/v1/datadeduplication/{id}
     */
    @PatchMapping("/{id}")
    public ResponseEntity<Map<String, Object>> updateRecord(@PathVariable Long id, @RequestBody DataDeduplicationRecord record) {
        logger.info("PATCH request to update datadeduplication record with id: {}", id);

        try {
            DataDeduplicationRecord updatedRecord = datadeduplicationService.updateRecord(id, record);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "DataDeduplication record updated successfully");
            response.put("data", updatedRecord);
            response.put("timestamp", LocalDateTime.now());

            return ResponseEntity.ok(response);

        } catch (RuntimeException e) {
            logger.error("Error updating datadeduplication record: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(errorResponse);

        } catch (Exception e) {
            logger.error("Error updating datadeduplication record: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to update datadeduplication record");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Delete a datadeduplication record
     * DELETE /api/v1/datadeduplication/{id}
     */
    @DeleteMapping("/{id}")
    public ResponseEntity<Map<String, Object>> deleteRecord(@PathVariable Long id) {
        logger.info("DELETE request to delete datadeduplication record with id: {}", id);

        try {
            // Check if record exists before deleting
            Optional<DataDeduplicationRecord> existingRecord = datadeduplicationService.getRecordById(id);

            if (existingRecord.isPresent()) {
                datadeduplicationService.deleteRecord(id);

                Map<String, Object> response = new HashMap<>();
                response.put("success", true);
                response.put("message", "DataDeduplication record deleted successfully");
                response.put("timestamp", LocalDateTime.now());

                return ResponseEntity.ok(response);
            } else {
                Map<String, Object> response = new HashMap<>();
                response.put("success", false);
                response.put("message", "DataDeduplication record not found");
                response.put("timestamp", LocalDateTime.now());

                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(response);
            }

        } catch (Exception e) {
            logger.error("Error deleting datadeduplication record: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to delete datadeduplication record");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Process record via Python FastAPI
     * POST /api/v1/datadeduplication/process
     */
    @PostMapping("/process")
    public ResponseEntity<Map<String, Object>> processRecordViaPython(@RequestBody Map<String, String> request) {
        logger.info("POST request to process datadeduplication record via Python");

        try {
            String recordId = request.get("recordId");
            String dataPayload = request.get("dataPayload");

            DataDeduplicationRecord processedRecord = datadeduplicationService.processRecordViaPython(recordId, dataPayload);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "DataDeduplication record processed successfully via Python");
            response.put("data", processedRecord);
            response.put("timestamp", LocalDateTime.now());

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error processing datadeduplication record via Python: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to process datadeduplication record via Python");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Validate record via Python FastAPI
     * POST /api/v1/datadeduplication/validate
     */
    @PostMapping("/validate")
    public ResponseEntity<Map<String, Object>> validateRecordViaPython(@RequestBody Map<String, String> request) {
        logger.info("POST request to validate datadeduplication record via Python");

        try {
            String recordId = request.get("recordId");
            String dataPayload = request.get("dataPayload");

            boolean isValid = datadeduplicationService.validateRecordViaPython(recordId, dataPayload);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "DataDeduplication record validation completed");
            response.put("isValid", isValid);
            response.put("timestamp", LocalDateTime.now());

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error validating datadeduplication record via Python: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to validate datadeduplication record via Python");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Get record count by status
     * GET /api/v1/datadeduplication/count
     */
    @GetMapping("/count")
    public ResponseEntity<Map<String, Object>> getRecordCountByStatus(@RequestParam String status) {
        logger.info("GET request to get count of datadeduplication records with status: {}", status);

        try {
            Long count = datadeduplicationService.getRecordCountByStatus(status);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "Record count retrieved successfully");
            response.put("status", status);
            response.put("count", count);
            response.put("timestamp", LocalDateTime.now());

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error getting count of datadeduplication records: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to get record count");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }
}