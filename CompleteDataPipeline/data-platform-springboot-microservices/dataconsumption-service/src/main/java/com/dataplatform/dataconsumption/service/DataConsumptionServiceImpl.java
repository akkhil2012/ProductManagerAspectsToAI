package com.dataplatform.dataconsumption.service;

import com.dataplatform.dataconsumption.model.DataConsumptionRecord;
import com.dataplatform.dataconsumption.repository.DataConsumptionRepository;
import com.dataplatform.dataconsumption.repository.DataConsumptionRepositoryImpl;

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
public class DataConsumptionServiceImpl implements DataConsumptionService {

    private static final Logger logger = LoggerFactory.getLogger(DataConsumptionServiceImpl.class);

    @Autowired
    private DataConsumptionRepository dataconsumptionRepository;

    @Autowired
    private DataConsumptionRepositoryImpl dataconsumptionRepositoryImpl;

    @Autowired
    private RestTemplate restTemplate;

    @Value("${python.fastapi.base.url:http://localhost:8000}")
    private String pythonFastApiBaseUrl;

    @Override
    public DataConsumptionRecord createRecord(DataConsumptionRecord record) {
        logger.info("Creating new dataconsumption record with recordId: {}", record.getRecordId());
        record.setCreatedAt(LocalDateTime.now());
        record.setUpdatedAt(LocalDateTime.now());
        return dataconsumptionRepository.save(record);
    }

    @Override
    public DataConsumptionRecord updateRecord(Long id, DataConsumptionRecord record) {
        logger.info("Updating dataconsumption record with id: {}", id);
        Optional<DataConsumptionRecord> existingRecord = dataconsumptionRepository.findById(id);

        if (existingRecord.isPresent()) {
            DataConsumptionRecord recordToUpdate = existingRecord.get();
            recordToUpdate.setStatus(record.getStatus());
            recordToUpdate.setDataPayload(record.getDataPayload());
            recordToUpdate.setErrorMessage(record.getErrorMessage());
            recordToUpdate.setUpdatedAt(LocalDateTime.now());
            return dataconsumptionRepository.save(recordToUpdate);
        }

        throw new RuntimeException("DataConsumption record not found with id: " + id);
    }

    @Override
    public void deleteRecord(Long id) {
        logger.info("Deleting dataconsumption record with id: {}", id);
        dataconsumptionRepository.deleteById(id);
    }

    @Override
    public Optional<DataConsumptionRecord> getRecordById(Long id) {
        logger.info("Fetching dataconsumption record with id: {}", id);
        return dataconsumptionRepository.findById(id);
    }

    @Override
    public Optional<DataConsumptionRecord> getRecordByRecordId(String recordId) {
        logger.info("Fetching dataconsumption record with recordId: {}", recordId);
        return dataconsumptionRepository.findByRecordId(recordId);
    }

    @Override
    public List<DataConsumptionRecord> getAllRecords() {
        logger.info("Fetching all dataconsumption records");
        return dataconsumptionRepository.findAll();
    }

    @Override
    public List<DataConsumptionRecord> getRecordsByStatus(String status) {
        logger.info("Fetching dataconsumption records with status: {}", status);
        return dataconsumptionRepository.findByStatus(status);
    }

    @Override
    public List<DataConsumptionRecord> getRecordsByDateRange(LocalDateTime startDate, LocalDateTime endDate) {
        logger.info("Fetching dataconsumption records between dates: {} and {}", startDate, endDate);
        return dataconsumptionRepository.findByCreatedAtBetween(startDate, endDate);
    }

    @Override
    public DataConsumptionRecord processRecordViaPython(String recordId, String dataPayload) {
        logger.info("Processing dataconsumption record {} via Python FastAPI", recordId);

        try {
            // Prepare request payload for Python FastAPI
            Map<String, Object> requestPayload = new HashMap<>();
            requestPayload.put("recordId", recordId);
            requestPayload.put("dataPayload", dataPayload);
            requestPayload.put("processType", "dataconsumption");

            // Set headers
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<Map<String, Object>> entity = new HttpEntity<>(requestPayload, headers);

            // Call Python FastAPI endpoint
            String pythonEndpoint = pythonFastApiBaseUrl + "/dataconsumption/process";
            ResponseEntity<Map> response = restTemplate.postForEntity(pythonEndpoint, entity, Map.class);

            if (response.getStatusCode() == HttpStatus.OK && response.getBody() != null) {
                Map<String, Object> responseBody = response.getBody();

                // Update record with Python processing results
                Optional<DataConsumptionRecord> existingRecord = getRecordByRecordId(recordId);
                if (existingRecord.isPresent()) {
                    DataConsumptionRecord record = existingRecord.get();
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
            logger.error("Error calling Python FastAPI for dataconsumption processing: {}", e.getMessage());
            throw new RuntimeException("Failed to process record via Python API", e);
        }

        throw new RuntimeException("Failed to process dataconsumption record via Python API");
    }

    @Override
    public boolean validateRecordViaPython(String recordId, String dataPayload) {
        logger.info("Validating dataconsumption record {} via Python FastAPI", recordId);

        try {
            // Prepare request payload for Python FastAPI
            Map<String, Object> requestPayload = new HashMap<>();
            requestPayload.put("recordId", recordId);
            requestPayload.put("dataPayload", dataPayload);

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<Map<String, Object>> entity = new HttpEntity<>(requestPayload, headers);

            // Call Python FastAPI validation endpoint
            String pythonEndpoint = pythonFastApiBaseUrl + "/dataconsumption/validate";
            ResponseEntity<Map> response = restTemplate.postForEntity(pythonEndpoint, entity, Map.class);

            if (response.getStatusCode() == HttpStatus.OK && response.getBody() != null) {
                Map<String, Object> responseBody = response.getBody();
                return (Boolean) responseBody.getOrDefault("isValid", false);
            }

        } catch (RestClientException e) {
            logger.error("Error calling Python FastAPI for dataconsumption validation: {}", e.getMessage());
        }

        return false;
    }

    @Override
    public String getProcessingStatusFromPython(String recordId) {
        logger.info("Getting processing status for dataconsumption record {} from Python FastAPI", recordId);

        try {
            // Call Python FastAPI status endpoint
            String pythonEndpoint = pythonFastApiBaseUrl + "/dataconsumption/status/" + recordId;
            ResponseEntity<Map> response = restTemplate.getForEntity(pythonEndpoint, Map.class);

            if (response.getStatusCode() == HttpStatus.OK && response.getBody() != null) {
                Map<String, Object> responseBody = response.getBody();
                return (String) responseBody.getOrDefault("status", "UNKNOWN");
            }

        } catch (RestClientException e) {
            logger.error("Error calling Python FastAPI for dataconsumption status: {}", e.getMessage());
        }

        return "ERROR";
    }

    @Override
    public void updateRecordStatus(String recordId, String newStatus) {
        logger.info("Updating status for dataconsumption record {} to {}", recordId, newStatus);
        dataconsumptionRepositoryImpl.updateRecordStatus(recordId, newStatus);
    }

    @Override
    public Long getRecordCountByStatus(String status) {
        logger.info("Getting count of dataconsumption records with status: {}", status);
        return dataconsumptionRepository.countByStatus(status);
    }
}