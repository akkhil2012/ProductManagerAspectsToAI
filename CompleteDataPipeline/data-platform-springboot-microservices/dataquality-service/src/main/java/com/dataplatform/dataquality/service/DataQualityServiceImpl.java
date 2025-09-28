package com.dataplatform.dataquality.service;

import com.dataplatform.dataquality.model.DataQualityRecord;
import com.dataplatform.dataquality.repository.DataQualityRepository;
import com.dataplatform.dataquality.repository.DataQualityRepositoryImpl;

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
public class DataQualityServiceImpl implements DataQualityService {

    private static final Logger logger = LoggerFactory.getLogger(DataQualityServiceImpl.class);

    @Autowired
    private DataQualityRepository dataqualityRepository;

    @Autowired
    private DataQualityRepositoryImpl dataqualityRepositoryImpl;

    @Autowired
    private RestTemplate restTemplate;

    @Value("${python.fastapi.base.url:http://localhost:8000}")
    private String pythonFastApiBaseUrl;

    @Override
    public DataQualityRecord createRecord(DataQualityRecord record) {
        logger.info("Creating new dataquality record with recordId: {}", record.getRecordId());
        record.setCreatedAt(LocalDateTime.now());
        record.setUpdatedAt(LocalDateTime.now());
        return dataqualityRepository.save(record);
    }

    @Override
    public DataQualityRecord updateRecord(Long id, DataQualityRecord record) {
        logger.info("Updating dataquality record with id: {}", id);
        Optional<DataQualityRecord> existingRecord = dataqualityRepository.findById(id);

        if (existingRecord.isPresent()) {
            DataQualityRecord recordToUpdate = existingRecord.get();
            recordToUpdate.setStatus(record.getStatus());
            recordToUpdate.setDataPayload(record.getDataPayload());
            recordToUpdate.setErrorMessage(record.getErrorMessage());
            recordToUpdate.setUpdatedAt(LocalDateTime.now());
            return dataqualityRepository.save(recordToUpdate);
        }

        throw new RuntimeException("DataQuality record not found with id: " + id);
    }

    @Override
    public void deleteRecord(Long id) {
        logger.info("Deleting dataquality record with id: {}", id);
        dataqualityRepository.deleteById(id);
    }

    @Override
    public Optional<DataQualityRecord> getRecordById(Long id) {
        logger.info("Fetching dataquality record with id: {}", id);
        return dataqualityRepository.findById(id);
    }

    @Override
    public Optional<DataQualityRecord> getRecordByRecordId(String recordId) {
        logger.info("Fetching dataquality record with recordId: {}", recordId);
        return dataqualityRepository.findByRecordId(recordId);
    }

    @Override
    public List<DataQualityRecord> getAllRecords() {
        logger.info("Fetching all dataquality records");
        return dataqualityRepository.findAll();
    }

    @Override
    public List<DataQualityRecord> getRecordsByStatus(String status) {
        logger.info("Fetching dataquality records with status: {}", status);
        return dataqualityRepository.findByStatus(status);
    }

    @Override
    public List<DataQualityRecord> getRecordsByDateRange(LocalDateTime startDate, LocalDateTime endDate) {
        logger.info("Fetching dataquality records between dates: {} and {}", startDate, endDate);
        return dataqualityRepository.findByCreatedAtBetween(startDate, endDate);
    }

    @Override
    public DataQualityRecord processRecordViaPython(String recordId, String dataPayload) {
        logger.info("Processing dataquality record {} via Python FastAPI", recordId);

        try {
            // Prepare request payload for Python FastAPI
            Map<String, Object> requestPayload = new HashMap<>();
            requestPayload.put("recordId", recordId);
            requestPayload.put("dataPayload", dataPayload);
            requestPayload.put("processType", "dataquality");

            // Set headers
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<Map<String, Object>> entity = new HttpEntity<>(requestPayload, headers);

            // Call Python FastAPI endpoint
            String pythonEndpoint = pythonFastApiBaseUrl + "/dataquality/process";
            ResponseEntity<Map> response = restTemplate.postForEntity(pythonEndpoint, entity, Map.class);

            if (response.getStatusCode() == HttpStatus.OK && response.getBody() != null) {
                Map<String, Object> responseBody = response.getBody();

                // Update record with Python processing results
                Optional<DataQualityRecord> existingRecord = getRecordByRecordId(recordId);
                if (existingRecord.isPresent()) {
                    DataQualityRecord record = existingRecord.get();
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
            logger.error("Error calling Python FastAPI for dataquality processing: {}", e.getMessage());
            throw new RuntimeException("Failed to process record via Python API", e);
        }

        throw new RuntimeException("Failed to process dataquality record via Python API");
    }

    @Override
    public boolean validateRecordViaPython(String recordId, String dataPayload) {
        logger.info("Validating dataquality record {} via Python FastAPI", recordId);

        try {
            // Prepare request payload for Python FastAPI
            Map<String, Object> requestPayload = new HashMap<>();
            requestPayload.put("recordId", recordId);
            requestPayload.put("dataPayload", dataPayload);

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<Map<String, Object>> entity = new HttpEntity<>(requestPayload, headers);

            // Call Python FastAPI validation endpoint
            String pythonEndpoint = pythonFastApiBaseUrl + "/dataquality/validate";
            ResponseEntity<Map> response = restTemplate.postForEntity(pythonEndpoint, entity, Map.class);

            if (response.getStatusCode() == HttpStatus.OK && response.getBody() != null) {
                Map<String, Object> responseBody = response.getBody();
                return (Boolean) responseBody.getOrDefault("isValid", false);
            }

        } catch (RestClientException e) {
            logger.error("Error calling Python FastAPI for dataquality validation: {}", e.getMessage());
        }

        return false;
    }

    @Override
    public String getProcessingStatusFromPython(String recordId) {
        logger.info("Getting processing status for dataquality record {} from Python FastAPI", recordId);

        try {
            // Call Python FastAPI status endpoint
            String pythonEndpoint = pythonFastApiBaseUrl + "/dataquality/status/" + recordId;
            ResponseEntity<Map> response = restTemplate.getForEntity(pythonEndpoint, Map.class);

            if (response.getStatusCode() == HttpStatus.OK && response.getBody() != null) {
                Map<String, Object> responseBody = response.getBody();
                return (String) responseBody.getOrDefault("status", "UNKNOWN");
            }

        } catch (RestClientException e) {
            logger.error("Error calling Python FastAPI for dataquality status: {}", e.getMessage());
        }

        return "ERROR";
    }

    @Override
    public void updateRecordStatus(String recordId, String newStatus) {
        logger.info("Updating status for dataquality record {} to {}", recordId, newStatus);
        dataqualityRepositoryImpl.updateRecordStatus(recordId, newStatus);
    }

    @Override
    public Long getRecordCountByStatus(String status) {
        logger.info("Getting count of dataquality records with status: {}", status);
        return dataqualityRepository.countByStatus(status);
    }
}