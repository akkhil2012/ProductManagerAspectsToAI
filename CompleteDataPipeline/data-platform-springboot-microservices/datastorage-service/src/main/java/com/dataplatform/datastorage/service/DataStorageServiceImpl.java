package com.dataplatform.datastorage.service;

import com.dataplatform.datastorage.model.DataStorageRecord;
import com.dataplatform.datastorage.repository.DataStorageRepository;
import com.dataplatform.datastorage.repository.DataStorageRepositoryImpl;

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
public class DataStorageServiceImpl implements DataStorageService {

    private static final Logger logger = LoggerFactory.getLogger(DataStorageServiceImpl.class);

    @Autowired
    private DataStorageRepository datastorageRepository;

    @Autowired
    private DataStorageRepositoryImpl datastorageRepositoryImpl;

    @Autowired
    private RestTemplate restTemplate;

    @Value("${python.fastapi.base.url:http://localhost:8000}")
    private String pythonFastApiBaseUrl;

    @Override
    public DataStorageRecord createRecord(DataStorageRecord record) {
        logger.info("Creating new datastorage record with recordId: {}", record.getRecordId());
        record.setCreatedAt(LocalDateTime.now());
        record.setUpdatedAt(LocalDateTime.now());
        return datastorageRepository.save(record);
    }

    @Override
    public DataStorageRecord updateRecord(Long id, DataStorageRecord record) {
        logger.info("Updating datastorage record with id: {}", id);
        Optional<DataStorageRecord> existingRecord = datastorageRepository.findById(id);

        if (existingRecord.isPresent()) {
            DataStorageRecord recordToUpdate = existingRecord.get();
            recordToUpdate.setStatus(record.getStatus());
            recordToUpdate.setDataPayload(record.getDataPayload());
            recordToUpdate.setErrorMessage(record.getErrorMessage());
            recordToUpdate.setUpdatedAt(LocalDateTime.now());
            return datastorageRepository.save(recordToUpdate);
        }

        throw new RuntimeException("DataStorage record not found with id: " + id);
    }

    @Override
    public void deleteRecord(Long id) {
        logger.info("Deleting datastorage record with id: {}", id);
        datastorageRepository.deleteById(id);
    }

    @Override
    public Optional<DataStorageRecord> getRecordById(Long id) {
        logger.info("Fetching datastorage record with id: {}", id);
        return datastorageRepository.findById(id);
    }

    @Override
    public Optional<DataStorageRecord> getRecordByRecordId(String recordId) {
        logger.info("Fetching datastorage record with recordId: {}", recordId);
        return datastorageRepository.findByRecordId(recordId);
    }

    @Override
    public List<DataStorageRecord> getAllRecords() {
        logger.info("Fetching all datastorage records");
        return datastorageRepository.findAll();
    }

    @Override
    public List<DataStorageRecord> getRecordsByStatus(String status) {
        logger.info("Fetching datastorage records with status: {}", status);
        return datastorageRepository.findByStatus(status);
    }

    @Override
    public List<DataStorageRecord> getRecordsByDateRange(LocalDateTime startDate, LocalDateTime endDate) {
        logger.info("Fetching datastorage records between dates: {} and {}", startDate, endDate);
        return datastorageRepository.findByCreatedAtBetween(startDate, endDate);
    }

    @Override
    public DataStorageRecord processRecordViaPython(String recordId, String dataPayload) {
        logger.info("Processing datastorage record {} via Python FastAPI", recordId);

        try {
            // Prepare request payload for Python FastAPI
            Map<String, Object> requestPayload = new HashMap<>();
            requestPayload.put("recordId", recordId);
            requestPayload.put("dataPayload", dataPayload);
            requestPayload.put("processType", "datastorage");

            // Set headers
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<Map<String, Object>> entity = new HttpEntity<>(requestPayload, headers);

            // Call Python FastAPI endpoint
            String pythonEndpoint = pythonFastApiBaseUrl + "/datastorage/process";
            ResponseEntity<Map> response = restTemplate.postForEntity(pythonEndpoint, entity, Map.class);

            if (response.getStatusCode() == HttpStatus.OK && response.getBody() != null) {
                Map<String, Object> responseBody = response.getBody();

                // Update record with Python processing results
                Optional<DataStorageRecord> existingRecord = getRecordByRecordId(recordId);
                if (existingRecord.isPresent()) {
                    DataStorageRecord record = existingRecord.get();
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
            logger.error("Error calling Python FastAPI for datastorage processing: {}", e.getMessage());
            throw new RuntimeException("Failed to process record via Python API", e);
        }

        throw new RuntimeException("Failed to process datastorage record via Python API");
    }

    @Override
    public boolean validateRecordViaPython(String recordId, String dataPayload) {
        logger.info("Validating datastorage record {} via Python FastAPI", recordId);

        try {
            // Prepare request payload for Python FastAPI
            Map<String, Object> requestPayload = new HashMap<>();
            requestPayload.put("recordId", recordId);
            requestPayload.put("dataPayload", dataPayload);

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<Map<String, Object>> entity = new HttpEntity<>(requestPayload, headers);

            // Call Python FastAPI validation endpoint
            String pythonEndpoint = pythonFastApiBaseUrl + "/datastorage/validate";
            ResponseEntity<Map> response = restTemplate.postForEntity(pythonEndpoint, entity, Map.class);

            if (response.getStatusCode() == HttpStatus.OK && response.getBody() != null) {
                Map<String, Object> responseBody = response.getBody();
                return (Boolean) responseBody.getOrDefault("isValid", false);
            }

        } catch (RestClientException e) {
            logger.error("Error calling Python FastAPI for datastorage validation: {}", e.getMessage());
        }

        return false;
    }

    @Override
    public String getProcessingStatusFromPython(String recordId) {
        logger.info("Getting processing status for datastorage record {} from Python FastAPI", recordId);

        try {
            // Call Python FastAPI status endpoint
            String pythonEndpoint = pythonFastApiBaseUrl + "/datastorage/status/" + recordId;
            ResponseEntity<Map> response = restTemplate.getForEntity(pythonEndpoint, Map.class);

            if (response.getStatusCode() == HttpStatus.OK && response.getBody() != null) {
                Map<String, Object> responseBody = response.getBody();
                return (String) responseBody.getOrDefault("status", "UNKNOWN");
            }

        } catch (RestClientException e) {
            logger.error("Error calling Python FastAPI for datastorage status: {}", e.getMessage());
        }

        return "ERROR";
    }

    @Override
    public void updateRecordStatus(String recordId, String newStatus) {
        logger.info("Updating status for datastorage record {} to {}", recordId, newStatus);
        datastorageRepositoryImpl.updateRecordStatus(recordId, newStatus);
    }

    @Override
    public Long getRecordCountByStatus(String status) {
        logger.info("Getting count of datastorage records with status: {}", status);
        return datastorageRepository.countByStatus(status);
    }
}