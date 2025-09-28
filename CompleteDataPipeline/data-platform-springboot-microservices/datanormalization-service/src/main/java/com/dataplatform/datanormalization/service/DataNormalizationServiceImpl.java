package com.dataplatform.datanormalization.service;

import com.dataplatform.datanormalization.model.DataNormalizationRecord;
import com.dataplatform.datanormalization.repository.DataNormalizationRepository;
import com.dataplatform.datanormalization.repository.DataNormalizationRepositoryImpl;

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
public class DataNormalizationServiceImpl implements DataNormalizationService {

    private static final Logger logger = LoggerFactory.getLogger(DataNormalizationServiceImpl.class);

    @Autowired
    private DataNormalizationRepository datanormalizationRepository;

    @Autowired
    private DataNormalizationRepositoryImpl datanormalizationRepositoryImpl;

    @Autowired
    private RestTemplate restTemplate;

    @Value("${python.fastapi.base.url:http://localhost:8000}")
    private String pythonFastApiBaseUrl;

    @Override
    public DataNormalizationRecord createRecord(DataNormalizationRecord record) {
        logger.info("Creating new datanormalization record with recordId: {}", record.getRecordId());
        record.setCreatedAt(LocalDateTime.now());
        record.setUpdatedAt(LocalDateTime.now());
        return datanormalizationRepository.save(record);
    }

    @Override
    public DataNormalizationRecord updateRecord(Long id, DataNormalizationRecord record) {
        logger.info("Updating datanormalization record with id: {}", id);
        Optional<DataNormalizationRecord> existingRecord = datanormalizationRepository.findById(id);

        if (existingRecord.isPresent()) {
            DataNormalizationRecord recordToUpdate = existingRecord.get();
            recordToUpdate.setStatus(record.getStatus());
            recordToUpdate.setDataPayload(record.getDataPayload());
            recordToUpdate.setErrorMessage(record.getErrorMessage());
            recordToUpdate.setUpdatedAt(LocalDateTime.now());
            return datanormalizationRepository.save(recordToUpdate);
        }

        throw new RuntimeException("DataNormalization record not found with id: " + id);
    }

    @Override
    public void deleteRecord(Long id) {
        logger.info("Deleting datanormalization record with id: {}", id);
        datanormalizationRepository.deleteById(id);
    }

    @Override
    public Optional<DataNormalizationRecord> getRecordById(Long id) {
        logger.info("Fetching datanormalization record with id: {}", id);
        return datanormalizationRepository.findById(id);
    }

    @Override
    public Optional<DataNormalizationRecord> getRecordByRecordId(String recordId) {
        logger.info("Fetching datanormalization record with recordId: {}", recordId);
        return datanormalizationRepository.findByRecordId(recordId);
    }

    @Override
    public List<DataNormalizationRecord> getAllRecords() {
        logger.info("Fetching all datanormalization records");
        return datanormalizationRepository.findAll();
    }

    @Override
    public List<DataNormalizationRecord> getRecordsByStatus(String status) {
        logger.info("Fetching datanormalization records with status: {}", status);
        return datanormalizationRepository.findByStatus(status);
    }

    @Override
    public List<DataNormalizationRecord> getRecordsByDateRange(LocalDateTime startDate, LocalDateTime endDate) {
        logger.info("Fetching datanormalization records between dates: {} and {}", startDate, endDate);
        return datanormalizationRepository.findByCreatedAtBetween(startDate, endDate);
    }

    @Override
    public DataNormalizationRecord processRecordViaPython(String recordId, String dataPayload) {
        logger.info("Processing datanormalization record {} via Python FastAPI", recordId);

        try {
            // Prepare request payload for Python FastAPI
            Map<String, Object> requestPayload = new HashMap<>();
            requestPayload.put("recordId", recordId);
            requestPayload.put("dataPayload", dataPayload);
            requestPayload.put("processType", "datanormalization");

            // Set headers
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<Map<String, Object>> entity = new HttpEntity<>(requestPayload, headers);

            // Call Python FastAPI endpoint
            String pythonEndpoint = pythonFastApiBaseUrl + "/datanormalization/process";
            ResponseEntity<Map> response = restTemplate.postForEntity(pythonEndpoint, entity, Map.class);

            if (response.getStatusCode() == HttpStatus.OK && response.getBody() != null) {
                Map<String, Object> responseBody = response.getBody();

                // Update record with Python processing results
                Optional<DataNormalizationRecord> existingRecord = getRecordByRecordId(recordId);
                if (existingRecord.isPresent()) {
                    DataNormalizationRecord record = existingRecord.get();
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
            logger.error("Error calling Python FastAPI for datanormalization processing: {}", e.getMessage());
            throw new RuntimeException("Failed to process record via Python API", e);
        }

        throw new RuntimeException("Failed to process datanormalization record via Python API");
    }

    @Override
    public boolean validateRecordViaPython(String recordId, String dataPayload) {
        logger.info("Validating datanormalization record {} via Python FastAPI", recordId);

        try {
            // Prepare request payload for Python FastAPI
            Map<String, Object> requestPayload = new HashMap<>();
            requestPayload.put("recordId", recordId);
            requestPayload.put("dataPayload", dataPayload);

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<Map<String, Object>> entity = new HttpEntity<>(requestPayload, headers);

            // Call Python FastAPI validation endpoint
            String pythonEndpoint = pythonFastApiBaseUrl + "/datanormalization/validate";
            ResponseEntity<Map> response = restTemplate.postForEntity(pythonEndpoint, entity, Map.class);

            if (response.getStatusCode() == HttpStatus.OK && response.getBody() != null) {
                Map<String, Object> responseBody = response.getBody();
                return (Boolean) responseBody.getOrDefault("isValid", false);
            }

        } catch (RestClientException e) {
            logger.error("Error calling Python FastAPI for datanormalization validation: {}", e.getMessage());
        }

        return false;
    }

    @Override
    public String getProcessingStatusFromPython(String recordId) {
        logger.info("Getting processing status for datanormalization record {} from Python FastAPI", recordId);

        try {
            // Call Python FastAPI status endpoint
            String pythonEndpoint = pythonFastApiBaseUrl + "/datanormalization/status/" + recordId;
            ResponseEntity<Map> response = restTemplate.getForEntity(pythonEndpoint, Map.class);

            if (response.getStatusCode() == HttpStatus.OK && response.getBody() != null) {
                Map<String, Object> responseBody = response.getBody();
                return (String) responseBody.getOrDefault("status", "UNKNOWN");
            }

        } catch (RestClientException e) {
            logger.error("Error calling Python FastAPI for datanormalization status: {}", e.getMessage());
        }

        return "ERROR";
    }

    @Override
    public void updateRecordStatus(String recordId, String newStatus) {
        logger.info("Updating status for datanormalization record {} to {}", recordId, newStatus);
        datanormalizationRepositoryImpl.updateRecordStatus(recordId, newStatus);
    }

    @Override
    public Long getRecordCountByStatus(String status) {
        logger.info("Getting count of datanormalization records with status: {}", status);
        return datanormalizationRepository.countByStatus(status);
    }
}