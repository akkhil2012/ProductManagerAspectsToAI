package com.dataplatform.datastorage.controller;

import com.dataplatform.datastorage.model.DataStorageRecord;
import com.dataplatform.datastorage.service.DataStorageService;

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
@RequestMapping("/api/v1/datastorage")
@CrossOrigin(origins = "*")
public class DataStorageController {

    private static final Logger logger = LoggerFactory.getLogger(DataStorageController.class);

    @Autowired
    private DataStorageService datastorageService;

    /**
     * Create a new datastorage record
     * POST /api/v1/datastorage
     */
    @PostMapping
    public ResponseEntity<Map<String, Object>> createRecord(@Valid @RequestBody DataStorageRecord record) {
        logger.info("POST request to create datastorage record with recordId: {}", record.getRecordId());

        try {
            DataStorageRecord createdRecord = datastorageService.createRecord(record);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "DataStorage record created successfully");
            response.put("data", createdRecord);
            response.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.CREATED).body(response);

        } catch (Exception e) {
            logger.error("Error creating datastorage record: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to create datastorage record");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Get all datastorage records
     * GET /api/v1/datastorage
     */
    @GetMapping
    public ResponseEntity<Map<String, Object>> getAllRecords(
            @RequestParam(required = false) String status,
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate) {

        logger.info("GET request to fetch all datastorage records");

        try {
            List<DataStorageRecord> records;

            if (status != null && !status.isEmpty()) {
                records = datastorageService.getRecordsByStatus(status);
            } else if (startDate != null && endDate != null) {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
                LocalDateTime start = LocalDateTime.parse(startDate, formatter);
                LocalDateTime end = LocalDateTime.parse(endDate, formatter);
                records = datastorageService.getRecordsByDateRange(start, end);
            } else {
                records = datastorageService.getAllRecords();
            }

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "Records retrieved successfully");
            response.put("data", records);
            response.put("count", records.size());
            response.put("timestamp", LocalDateTime.now());

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error fetching datastorage records: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to fetch datastorage records");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Get a datastorage record by ID
     * GET /api/v1/datastorage/{id}
     */
    @GetMapping("/{id}")
    public ResponseEntity<Map<String, Object>> getRecordById(@PathVariable Long id) {
        logger.info("GET request to fetch datastorage record with id: {}", id);

        try {
            Optional<DataStorageRecord> record = datastorageService.getRecordById(id);

            if (record.isPresent()) {
                Map<String, Object> response = new HashMap<>();
                response.put("success", true);
                response.put("message", "DataStorage record found");
                response.put("data", record.get());
                response.put("timestamp", LocalDateTime.now());

                return ResponseEntity.ok(response);
            } else {
                Map<String, Object> response = new HashMap<>();
                response.put("success", false);
                response.put("message", "DataStorage record not found");
                response.put("timestamp", LocalDateTime.now());

                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(response);
            }

        } catch (Exception e) {
            logger.error("Error fetching datastorage record by id: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to fetch datastorage record");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Get a datastorage record by record ID
     * GET /api/v1/datastorage/record/{recordId}
     */
    @GetMapping("/record/{recordId}")
    public ResponseEntity<Map<String, Object>> getRecordByRecordId(@PathVariable String recordId) {
        logger.info("GET request to fetch datastorage record with recordId: {}", recordId);

        try {
            Optional<DataStorageRecord> record = datastorageService.getRecordByRecordId(recordId);

            if (record.isPresent()) {
                Map<String, Object> response = new HashMap<>();
                response.put("success", true);
                response.put("message", "DataStorage record found");
                response.put("data", record.get());
                response.put("timestamp", LocalDateTime.now());

                return ResponseEntity.ok(response);
            } else {
                Map<String, Object> response = new HashMap<>();
                response.put("success", false);
                response.put("message", "DataStorage record not found");
                response.put("timestamp", LocalDateTime.now());

                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(response);
            }

        } catch (Exception e) {
            logger.error("Error fetching datastorage record by recordId: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to fetch datastorage record");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Update a datastorage record
     * PATCH /api/v1/datastorage/{id}
     */
    @PatchMapping("/{id}")
    public ResponseEntity<Map<String, Object>> updateRecord(@PathVariable Long id, @RequestBody DataStorageRecord record) {
        logger.info("PATCH request to update datastorage record with id: {}", id);

        try {
            DataStorageRecord updatedRecord = datastorageService.updateRecord(id, record);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "DataStorage record updated successfully");
            response.put("data", updatedRecord);
            response.put("timestamp", LocalDateTime.now());

            return ResponseEntity.ok(response);

        } catch (RuntimeException e) {
            logger.error("Error updating datastorage record: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(errorResponse);

        } catch (Exception e) {
            logger.error("Error updating datastorage record: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to update datastorage record");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Delete a datastorage record
     * DELETE /api/v1/datastorage/{id}
     */
    @DeleteMapping("/{id}")
    public ResponseEntity<Map<String, Object>> deleteRecord(@PathVariable Long id) {
        logger.info("DELETE request to delete datastorage record with id: {}", id);

        try {
            // Check if record exists before deleting
            Optional<DataStorageRecord> existingRecord = datastorageService.getRecordById(id);

            if (existingRecord.isPresent()) {
                datastorageService.deleteRecord(id);

                Map<String, Object> response = new HashMap<>();
                response.put("success", true);
                response.put("message", "DataStorage record deleted successfully");
                response.put("timestamp", LocalDateTime.now());

                return ResponseEntity.ok(response);
            } else {
                Map<String, Object> response = new HashMap<>();
                response.put("success", false);
                response.put("message", "DataStorage record not found");
                response.put("timestamp", LocalDateTime.now());

                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(response);
            }

        } catch (Exception e) {
            logger.error("Error deleting datastorage record: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to delete datastorage record");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Process record via Python FastAPI
     * POST /api/v1/datastorage/process
     */
    @PostMapping("/process")
    public ResponseEntity<Map<String, Object>> processRecordViaPython(@RequestBody Map<String, String> request) {
        logger.info("POST request to process datastorage record via Python");

        try {
            String recordId = request.get("recordId");
            String dataPayload = request.get("dataPayload");

            DataStorageRecord processedRecord = datastorageService.processRecordViaPython(recordId, dataPayload);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "DataStorage record processed successfully via Python");
            response.put("data", processedRecord);
            response.put("timestamp", LocalDateTime.now());

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error processing datastorage record via Python: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to process datastorage record via Python");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Validate record via Python FastAPI
     * POST /api/v1/datastorage/validate
     */
    @PostMapping("/validate")
    public ResponseEntity<Map<String, Object>> validateRecordViaPython(@RequestBody Map<String, String> request) {
        logger.info("POST request to validate datastorage record via Python");

        try {
            String recordId = request.get("recordId");
            String dataPayload = request.get("dataPayload");

            boolean isValid = datastorageService.validateRecordViaPython(recordId, dataPayload);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "DataStorage record validation completed");
            response.put("isValid", isValid);
            response.put("timestamp", LocalDateTime.now());

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error validating datastorage record via Python: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to validate datastorage record via Python");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Get record count by status
     * GET /api/v1/datastorage/count
     */
    @GetMapping("/count")
    public ResponseEntity<Map<String, Object>> getRecordCountByStatus(@RequestParam String status) {
        logger.info("GET request to get count of datastorage records with status: {}", status);

        try {
            Long count = datastorageService.getRecordCountByStatus(status);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "Record count retrieved successfully");
            response.put("status", status);
            response.put("count", count);
            response.put("timestamp", LocalDateTime.now());

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error getting count of datastorage records: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to get record count");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }
}