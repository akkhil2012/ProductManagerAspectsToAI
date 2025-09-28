package com.dataplatform.dataquality.controller;

import com.dataplatform.dataquality.model.DataQualityRecord;
import com.dataplatform.dataquality.service.DataQualityService;

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
@RequestMapping("/api/v1/dataquality")
@CrossOrigin(origins = "*")
public class DataQualityController {

    private static final Logger logger = LoggerFactory.getLogger(DataQualityController.class);

    @Autowired
    private DataQualityService dataqualityService;

    /**
     * Create a new dataquality record
     * POST /api/v1/dataquality
     */
    @PostMapping
    public ResponseEntity<Map<String, Object>> createRecord(@Valid @RequestBody DataQualityRecord record) {
        logger.info("POST request to create dataquality record with recordId: {}", record.getRecordId());

        try {
            DataQualityRecord createdRecord = dataqualityService.createRecord(record);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "DataQuality record created successfully");
            response.put("data", createdRecord);
            response.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.CREATED).body(response);

        } catch (Exception e) {
            logger.error("Error creating dataquality record: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to create dataquality record");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Get all dataquality records
     * GET /api/v1/dataquality
     */
    @GetMapping
    public ResponseEntity<Map<String, Object>> getAllRecords(
            @RequestParam(required = false) String status,
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate) {

        logger.info("GET request to fetch all dataquality records");

        try {
            List<DataQualityRecord> records;

            if (status != null && !status.isEmpty()) {
                records = dataqualityService.getRecordsByStatus(status);
            } else if (startDate != null && endDate != null) {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
                LocalDateTime start = LocalDateTime.parse(startDate, formatter);
                LocalDateTime end = LocalDateTime.parse(endDate, formatter);
                records = dataqualityService.getRecordsByDateRange(start, end);
            } else {
                records = dataqualityService.getAllRecords();
            }

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "Records retrieved successfully");
            response.put("data", records);
            response.put("count", records.size());
            response.put("timestamp", LocalDateTime.now());

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error fetching dataquality records: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to fetch dataquality records");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Get a dataquality record by ID
     * GET /api/v1/dataquality/{id}
     */
    @GetMapping("/{id}")
    public ResponseEntity<Map<String, Object>> getRecordById(@PathVariable Long id) {
        logger.info("GET request to fetch dataquality record with id: {}", id);

        try {
            Optional<DataQualityRecord> record = dataqualityService.getRecordById(id);

            if (record.isPresent()) {
                Map<String, Object> response = new HashMap<>();
                response.put("success", true);
                response.put("message", "DataQuality record found");
                response.put("data", record.get());
                response.put("timestamp", LocalDateTime.now());

                return ResponseEntity.ok(response);
            } else {
                Map<String, Object> response = new HashMap<>();
                response.put("success", false);
                response.put("message", "DataQuality record not found");
                response.put("timestamp", LocalDateTime.now());

                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(response);
            }

        } catch (Exception e) {
            logger.error("Error fetching dataquality record by id: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to fetch dataquality record");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Get a dataquality record by record ID
     * GET /api/v1/dataquality/record/{recordId}
     */
    @GetMapping("/record/{recordId}")
    public ResponseEntity<Map<String, Object>> getRecordByRecordId(@PathVariable String recordId) {
        logger.info("GET request to fetch dataquality record with recordId: {}", recordId);

        try {
            Optional<DataQualityRecord> record = dataqualityService.getRecordByRecordId(recordId);

            if (record.isPresent()) {
                Map<String, Object> response = new HashMap<>();
                response.put("success", true);
                response.put("message", "DataQuality record found");
                response.put("data", record.get());
                response.put("timestamp", LocalDateTime.now());

                return ResponseEntity.ok(response);
            } else {
                Map<String, Object> response = new HashMap<>();
                response.put("success", false);
                response.put("message", "DataQuality record not found");
                response.put("timestamp", LocalDateTime.now());

                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(response);
            }

        } catch (Exception e) {
            logger.error("Error fetching dataquality record by recordId: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to fetch dataquality record");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Update a dataquality record
     * PATCH /api/v1/dataquality/{id}
     */
    @PatchMapping("/{id}")
    public ResponseEntity<Map<String, Object>> updateRecord(@PathVariable Long id, @RequestBody DataQualityRecord record) {
        logger.info("PATCH request to update dataquality record with id: {}", id);

        try {
            DataQualityRecord updatedRecord = dataqualityService.updateRecord(id, record);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "DataQuality record updated successfully");
            response.put("data", updatedRecord);
            response.put("timestamp", LocalDateTime.now());

            return ResponseEntity.ok(response);

        } catch (RuntimeException e) {
            logger.error("Error updating dataquality record: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(errorResponse);

        } catch (Exception e) {
            logger.error("Error updating dataquality record: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to update dataquality record");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Delete a dataquality record
     * DELETE /api/v1/dataquality/{id}
     */
    @DeleteMapping("/{id}")
    public ResponseEntity<Map<String, Object>> deleteRecord(@PathVariable Long id) {
        logger.info("DELETE request to delete dataquality record with id: {}", id);

        try {
            // Check if record exists before deleting
            Optional<DataQualityRecord> existingRecord = dataqualityService.getRecordById(id);

            if (existingRecord.isPresent()) {
                dataqualityService.deleteRecord(id);

                Map<String, Object> response = new HashMap<>();
                response.put("success", true);
                response.put("message", "DataQuality record deleted successfully");
                response.put("timestamp", LocalDateTime.now());

                return ResponseEntity.ok(response);
            } else {
                Map<String, Object> response = new HashMap<>();
                response.put("success", false);
                response.put("message", "DataQuality record not found");
                response.put("timestamp", LocalDateTime.now());

                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(response);
            }

        } catch (Exception e) {
            logger.error("Error deleting dataquality record: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to delete dataquality record");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Process record via Python FastAPI
     * POST /api/v1/dataquality/process
     */
    @PostMapping("/process")
    public ResponseEntity<Map<String, Object>> processRecordViaPython(@RequestBody Map<String, String> request) {
        logger.info("POST request to process dataquality record via Python");

        try {
            String recordId = request.get("recordId");
            String dataPayload = request.get("dataPayload");

            DataQualityRecord processedRecord = dataqualityService.processRecordViaPython(recordId, dataPayload);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "DataQuality record processed successfully via Python");
            response.put("data", processedRecord);
            response.put("timestamp", LocalDateTime.now());

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error processing dataquality record via Python: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to process dataquality record via Python");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Validate record via Python FastAPI
     * POST /api/v1/dataquality/validate
     */
    @PostMapping("/validate")
    public ResponseEntity<Map<String, Object>> validateRecordViaPython(@RequestBody Map<String, String> request) {
        logger.info("POST request to validate dataquality record via Python");

        try {
            String recordId = request.get("recordId");
            String dataPayload = request.get("dataPayload");

            boolean isValid = dataqualityService.validateRecordViaPython(recordId, dataPayload);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "DataQuality record validation completed");
            response.put("isValid", isValid);
            response.put("timestamp", LocalDateTime.now());

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error validating dataquality record via Python: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to validate dataquality record via Python");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Get record count by status
     * GET /api/v1/dataquality/count
     */
    @GetMapping("/count")
    public ResponseEntity<Map<String, Object>> getRecordCountByStatus(@RequestParam String status) {
        logger.info("GET request to get count of dataquality records with status: {}", status);

        try {
            Long count = dataqualityService.getRecordCountByStatus(status);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "Record count retrieved successfully");
            response.put("status", status);
            response.put("count", count);
            response.put("timestamp", LocalDateTime.now());

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error getting count of dataquality records: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to get record count");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }
}