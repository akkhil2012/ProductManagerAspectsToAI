package com.dataplatform.dataconsumption.controller;

import com.dataplatform.dataconsumption.model.DataConsumptionRecord;
import com.dataplatform.dataconsumption.service.DataConsumptionService;

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
@RequestMapping("/api/v1/dataconsumption")
@CrossOrigin(origins = "*")
public class DataConsumptionController {

    private static final Logger logger = LoggerFactory.getLogger(DataConsumptionController.class);

    @Autowired
    private DataConsumptionService dataconsumptionService;

    /**
     * Create a new dataconsumption record
     * POST /api/v1/dataconsumption
     */
    @PostMapping
    public ResponseEntity<Map<String, Object>> createRecord(@Valid @RequestBody DataConsumptionRecord record) {
        logger.info("POST request to create dataconsumption record with recordId: {}", record.getRecordId());

        try {
            DataConsumptionRecord createdRecord = dataconsumptionService.createRecord(record);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "DataConsumption record created successfully");
            response.put("data", createdRecord);
            response.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.CREATED).body(response);

        } catch (Exception e) {
            logger.error("Error creating dataconsumption record: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to create dataconsumption record");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Get all dataconsumption records
     * GET /api/v1/dataconsumption
     */
    @GetMapping
    public ResponseEntity<Map<String, Object>> getAllRecords(
            @RequestParam(required = false) String status,
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate) {

        logger.info("GET request to fetch all dataconsumption records");

        try {
            List<DataConsumptionRecord> records;

            if (status != null && !status.isEmpty()) {
                records = dataconsumptionService.getRecordsByStatus(status);
            } else if (startDate != null && endDate != null) {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
                LocalDateTime start = LocalDateTime.parse(startDate, formatter);
                LocalDateTime end = LocalDateTime.parse(endDate, formatter);
                records = dataconsumptionService.getRecordsByDateRange(start, end);
            } else {
                records = dataconsumptionService.getAllRecords();
            }

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "Records retrieved successfully");
            response.put("data", records);
            response.put("count", records.size());
            response.put("timestamp", LocalDateTime.now());

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error fetching dataconsumption records: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to fetch dataconsumption records");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Get a dataconsumption record by ID
     * GET /api/v1/dataconsumption/{id}
     */
    @GetMapping("/{id}")
    public ResponseEntity<Map<String, Object>> getRecordById(@PathVariable Long id) {
        logger.info("GET request to fetch dataconsumption record with id: {}", id);

        try {
            Optional<DataConsumptionRecord> record = dataconsumptionService.getRecordById(id);

            if (record.isPresent()) {
                Map<String, Object> response = new HashMap<>();
                response.put("success", true);
                response.put("message", "DataConsumption record found");
                response.put("data", record.get());
                response.put("timestamp", LocalDateTime.now());

                return ResponseEntity.ok(response);
            } else {
                Map<String, Object> response = new HashMap<>();
                response.put("success", false);
                response.put("message", "DataConsumption record not found");
                response.put("timestamp", LocalDateTime.now());

                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(response);
            }

        } catch (Exception e) {
            logger.error("Error fetching dataconsumption record by id: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to fetch dataconsumption record");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Get a dataconsumption record by record ID
     * GET /api/v1/dataconsumption/record/{recordId}
     */
    @GetMapping("/record/{recordId}")
    public ResponseEntity<Map<String, Object>> getRecordByRecordId(@PathVariable String recordId) {
        logger.info("GET request to fetch dataconsumption record with recordId: {}", recordId);

        try {
            Optional<DataConsumptionRecord> record = dataconsumptionService.getRecordByRecordId(recordId);

            if (record.isPresent()) {
                Map<String, Object> response = new HashMap<>();
                response.put("success", true);
                response.put("message", "DataConsumption record found");
                response.put("data", record.get());
                response.put("timestamp", LocalDateTime.now());

                return ResponseEntity.ok(response);
            } else {
                Map<String, Object> response = new HashMap<>();
                response.put("success", false);
                response.put("message", "DataConsumption record not found");
                response.put("timestamp", LocalDateTime.now());

                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(response);
            }

        } catch (Exception e) {
            logger.error("Error fetching dataconsumption record by recordId: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to fetch dataconsumption record");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Update a dataconsumption record
     * PATCH /api/v1/dataconsumption/{id}
     */
    @PatchMapping("/{id}")
    public ResponseEntity<Map<String, Object>> updateRecord(@PathVariable Long id, @RequestBody DataConsumptionRecord record) {
        logger.info("PATCH request to update dataconsumption record with id: {}", id);

        try {
            DataConsumptionRecord updatedRecord = dataconsumptionService.updateRecord(id, record);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "DataConsumption record updated successfully");
            response.put("data", updatedRecord);
            response.put("timestamp", LocalDateTime.now());

            return ResponseEntity.ok(response);

        } catch (RuntimeException e) {
            logger.error("Error updating dataconsumption record: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(errorResponse);

        } catch (Exception e) {
            logger.error("Error updating dataconsumption record: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to update dataconsumption record");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Delete a dataconsumption record
     * DELETE /api/v1/dataconsumption/{id}
     */
    @DeleteMapping("/{id}")
    public ResponseEntity<Map<String, Object>> deleteRecord(@PathVariable Long id) {
        logger.info("DELETE request to delete dataconsumption record with id: {}", id);

        try {
            // Check if record exists before deleting
            Optional<DataConsumptionRecord> existingRecord = dataconsumptionService.getRecordById(id);

            if (existingRecord.isPresent()) {
                dataconsumptionService.deleteRecord(id);

                Map<String, Object> response = new HashMap<>();
                response.put("success", true);
                response.put("message", "DataConsumption record deleted successfully");
                response.put("timestamp", LocalDateTime.now());

                return ResponseEntity.ok(response);
            } else {
                Map<String, Object> response = new HashMap<>();
                response.put("success", false);
                response.put("message", "DataConsumption record not found");
                response.put("timestamp", LocalDateTime.now());

                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(response);
            }

        } catch (Exception e) {
            logger.error("Error deleting dataconsumption record: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to delete dataconsumption record");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Process record via Python FastAPI
     * POST /api/v1/dataconsumption/process
     */
    @PostMapping("/process")
    public ResponseEntity<Map<String, Object>> processRecordViaPython(@RequestBody Map<String, String> request) {
        logger.info("POST request to process dataconsumption record via Python");

        try {
            String recordId = request.get("recordId");
            String dataPayload = request.get("dataPayload");

            DataConsumptionRecord processedRecord = dataconsumptionService.processRecordViaPython(recordId, dataPayload);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "DataConsumption record processed successfully via Python");
            response.put("data", processedRecord);
            response.put("timestamp", LocalDateTime.now());

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error processing dataconsumption record via Python: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to process dataconsumption record via Python");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Validate record via Python FastAPI
     * POST /api/v1/dataconsumption/validate
     */
    @PostMapping("/validate")
    public ResponseEntity<Map<String, Object>> validateRecordViaPython(@RequestBody Map<String, String> request) {
        logger.info("POST request to validate dataconsumption record via Python");

        try {
            String recordId = request.get("recordId");
            String dataPayload = request.get("dataPayload");

            boolean isValid = dataconsumptionService.validateRecordViaPython(recordId, dataPayload);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "DataConsumption record validation completed");
            response.put("isValid", isValid);
            response.put("timestamp", LocalDateTime.now());

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error validating dataconsumption record via Python: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to validate dataconsumption record via Python");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Get record count by status
     * GET /api/v1/dataconsumption/count
     */
    @GetMapping("/count")
    public ResponseEntity<Map<String, Object>> getRecordCountByStatus(@RequestParam String status) {
        logger.info("GET request to get count of dataconsumption records with status: {}", status);

        try {
            Long count = dataconsumptionService.getRecordCountByStatus(status);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "Record count retrieved successfully");
            response.put("status", status);
            response.put("count", count);
            response.put("timestamp", LocalDateTime.now());

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error getting count of dataconsumption records: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to get record count");
            errorResponse.put("error", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }
}