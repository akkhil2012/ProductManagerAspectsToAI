package com.dataplatform.datadeduplication.service;

import com.dataplatform.datadeduplication.model.DataDeduplicationRecord;
import com.dataplatform.datadeduplication.repository.DataDeduplicationRepository;
import com.dataplatform.datadeduplication.repository.DataDeduplicationRepositoryImpl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.client.RestClientException;

import javax.transaction.Transactional;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
@Transactional
public class DataDeduplicationServiceImpl implements DataDeduplicationService {

    private static final Logger logger = LoggerFactory.getLogger(DataDeduplicationServiceImpl.class);

    @Autowired
    private DataDeduplicationRepository datadeduplicationRepository;

    @Autowired
    private DataDeduplicationRepositoryImpl datadeduplicationRepositoryImpl;

    @Autowired
    private RestTemplate restTemplate;

    @Value("${python.fastapi.base.url:http://localhost:8000}")
    private String pythonFastApiBaseUrl;

    @Override
    public DataDeduplicationRecord createRecord(DataDeduplicationRecord record) {
        logger.info("Creating new datadeduplication record with recordId: {}", record.getRecordId());
        record.setCreatedAt(LocalDateTime.now());
        record.setUpdatedAt(LocalDateTime.now());
        return datadeduplicationRepository.save(record);
    }

    @Override
    public DataDeduplicationRecord updateRecord(Long id, DataDeduplicationRecord record) {
        logger.info("Updating datadeduplication record with id: {}", id);
        Optional<DataDeduplicationRecord> existingRecord = datadeduplicationRepository.findById(id);

        if (existingRecord.isPresent()) {
            DataDeduplicationRecord recordToUpdate = existingRecord.get();
            recordToUpdate.setStatus(record.getStatus());
            recordToUpdate.setDataPayload(record.getDataPayload());
            recordToUpdate.setErrorMessage(record.getErrorMessage());
            recordToUpdate.setUpdatedAt(LocalDateTime.now());
            return datadeduplicationRepository.save(recordToUpdate);
        }

        throw new RuntimeException("DataDeduplication record not found with id: " + id);
    }

    @Override
    public void deleteRecord(Long id) {
        logger.info("Deleting datadeduplication record with id: {}", id);
        datadeduplicationRepository.deleteById(id);
    }

    @Override
    public Optional<DataDeduplicationRecord> getRecordById(Long id) {
        logger.info("Fetching datadeduplication record with id: {}", id);
        return datadeduplicationRepository.findById(id);
    }

    @Override
    public Optional<DataDeduplicationRecord> getRecordByRecordId(String recordId) {
        logger.info("Fetching datadeduplication record with recordId: {}", recordId);
        return datadeduplicationRepository.findByRecordId(recordId);
    }

    @Override
    public List<DataDeduplicationRecord> getAllRecords() {
        logger.info("Fetching all datadeduplication records");
        return datadeduplicationRepository.findAll();
    }

    @Override
    public List<DataDeduplicationRecord> getRecordsByStatus(String status) {
        logger.info("Fetching datadeduplication records with status: {}", status);
        return datadeduplicationRepository.findByStatus(status);
    }

    @Override
    public List<DataDeduplicationRecord> getRecordsByDateRange(LocalDateTime startDate, LocalDateTime endDate) {
        logger.info("Fetching datadeduplication records between dates: {} and {}", startDate, endDate);
        return datadeduplicationRepository.findByCreatedAtBetween(startDate, endDate);
    }

    @Override
    public DataDeduplicationRecord processRecordViaPython(String recordId, String dataPayload) {
        logger.info("Processing datadeduplication record {} via Python FastAPI", recordId);

        try {
            // Prepare request payload for Python FastAPI
            Map<String, Object> requestPayload = new HashMap<>();
            requestPayload.put("recordId", recordId);
            requestPayload.put("dataPayload", dataPayload);
            requestPayload.put("processType", "datadeduplication");

            // Set headers
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<Map<String, Object>> entity = new HttpEntity<>(requestPayload, headers);

            // Call Python FastAPI endpoint
            String pythonEndpoint = pythonFastApiBaseUrl + "/datadeduplication/process";
            ResponseEntity<Map> response = restTemplate.postForEntity(pythonEndpoint, entity, Map.class);

            if (response.getStatusCode() == HttpStatus.OK && response.getBody() != null) {
                Map<String, Object> responseBody = response.getBody();

                // Update record with Python processing results
                Optional<DataDeduplicationRecord> existingRecord = getRecordByRecordId(recordId);
                if (existingRecord.isPresent()) {
                    DataDeduplicationRecord record = existingRecord.get();
                    record.setStatus((String) responseBody.get("status"));
                    record.setDataPayload((String) responseBody.get("processedData"));
                    record.setProcessingTimestamp(LocalDateTime.now());
                    if (responseBody.containsKey("errorMessage")) {
                        record.setErrorMessage((String) responseBody.get("errorMessage"));
                    }
                    return updateRecord(record.getId(), record);
                }
            }

        } catch (RestClientException e) {
            logger.error("Error calling Python FastAPI for datadeduplication processing: {}", e.getMessage());
            throw new RuntimeException("Failed to process record via Python API", e);
        }

        throw new RuntimeException("Failed to process datadeduplication record via Python API");
    }

    @Override
    public boolean validateRecordViaPython(String recordId, String dataPayload) {
        logger.info("Validating datadeduplication record {} via Python FastAPI", recordId);

        try {
            // Prepare request payload for Python FastAPI
            Map<String, Object> requestPayload = new HashMap<>();
            requestPayload.put("recordId", recordId);
            requestPayload.put("dataPayload", dataPayload);

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<Map<String, Object>> entity = new HttpEntity<>(requestPayload, headers);

            // Call Python FastAPI validation endpoint
            String pythonEndpoint = pythonFastApiBaseUrl + "/datadeduplication/validate";
            ResponseEntity<Map> response = restTemplate.postForEntity(pythonEndpoint, entity, Map.class);

            if (response.getStatusCode() == HttpStatus.OK && response.getBody() != null) {
                Map<String, Object> responseBody = response.getBody();
                return (Boolean) responseBody.getOrDefault("isValid", false);
            }

        } catch (RestClientException e) {
            logger.error("Error calling Python FastAPI for datadeduplication validation: {}", e.getMessage());
        }

        return false;
    }

    @Override
    public String getProcessingStatusFromPython(String recordId) {
        logger.info("Getting processing status for datadeduplication record {} from Python FastAPI", recordId);

        try {
            // Call Python FastAPI status endpoint
            String pythonEndpoint = pythonFastApiBaseUrl + "/datadeduplication/status/" + recordId;
            ResponseEntity<Map> response = restTemplate.getForEntity(pythonEndpoint, Map.class);

            if (response.getStatusCode() == HttpStatus.OK && response.getBody() != null) {
                Map<String, Object> responseBody = response.getBody();
                return (String) responseBody.getOrDefault("status", "UNKNOWN");
            }

        } catch (RestClientException e) {
            logger.error("Error calling Python FastAPI for datadeduplication status: {}", e.getMessage());
        }

        return "ERROR";
    }

    @Override
    public void updateRecordStatus(String recordId, String newStatus) {
        logger.info("Updating status for datadeduplication record {} to {}", recordId, newStatus);
        datadeduplicationRepositoryImpl.updateRecordStatus(recordId, newStatus);
    }

    @Override
    public Long getRecordCountByStatus(String status) {
        logger.info("Getting count of datadeduplication records with status: {}", status);
        return datadeduplicationRepository.countByStatus(status);
    }
}