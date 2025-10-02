package com.dataplatform.dataingestion.service;

import com.dataplatform.dataingestion.model.DataIngestionRecord;
import com.dataplatform.dataingestion.repository.DataIngestionRepository;
import com.dataplatform.dataingestion.repository.DataIngestionRepositoryImpl;

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
public class DataIngestionServiceImpl implements DataIngestionService {

    private static final Logger logger = LoggerFactory.getLogger(DataIngestionServiceImpl.class);

    @Autowired
    private DataIngestionRepository dataingestionRepository;

    @Autowired
    private DataIngestionRepositoryImpl dataingestionRepositoryImpl;

    @Autowired
    private RestTemplate restTemplate;

    @Value("${python.fastapi.base.url:http://localhost:8000}")
    private String pythonFastApiBaseUrl;

    @Override
    public DataIngestionRecord createRecord(DataIngestionRecord record) {
        logger.info("Creating new dataingestion record with recordId: {}", record.getRecordId());
        record.setCreatedAt(LocalDateTime.now());
        record.setUpdatedAt(LocalDateTime.now());
        return dataingestionRepository.save(record);
    }

    @Override
    public DataIngestionRecord updateRecord(Long id, DataIngestionRecord record) {
        logger.info("Updating dataingestion record with id: {}", id);
        Optional<DataIngestionRecord> existingRecord = dataingestionRepository.findById(id);

        if (existingRecord.isPresent()) {
            DataIngestionRecord recordToUpdate = existingRecord.get();
            recordToUpdate.setStatus(record.getStatus());
            recordToUpdate.setDataPayload(record.getDataPayload());
            recordToUpdate.setErrorMessage(record.getErrorMessage());
            recordToUpdate.setUpdatedAt(LocalDateTime.now());
            return dataingestionRepository.save(recordToUpdate);
        }

        throw new RuntimeException("DataIngestion record not found with id: " + id);
    }

    @Override
    public void deleteRecord(Long id) {
        logger.info("Deleting dataingestion record with id: {}", id);
        dataingestionRepository.deleteById(id);
    }

    @Override
    public Optional<DataIngestionRecord> getRecordById(Long id) {
        logger.info("Fetching dataingestion record with id: {}", id);
        return dataingestionRepository.findById(id);
    }

    @Override
    public Optional<DataIngestionRecord> getRecordByRecordId(String recordId) {
        logger.info("Fetching dataingestion record with recordId: {}", recordId);
        return dataingestionRepository.findByRecordId(recordId);
    }

    @Override
    public List<DataIngestionRecord> getAllRecords() {
        logger.info("Fetching all dataingestion records");
        return dataingestionRepository.findAll();
    }

    @Override
    public List<DataIngestionRecord> getRecordsByStatus(String status) {
        logger.info("Fetching dataingestion records with status: {}", status);
        return dataingestionRepository.findByStatus(status);
    }

    @Override
    public List<DataIngestionRecord> getRecordsByDateRange(LocalDateTime startDate, LocalDateTime endDate) {
        logger.info("Fetching dataingestion records between dates: {} and {}", startDate, endDate);
        return dataingestionRepository.findByCreatedAtBetween(startDate, endDate);
    }

    @Override
    public DataIngestionRecord processRecordViaPython(String recordId, String dataPayload) {
        logger.info("Processing dataingestion record {} via Python FastAPI", recordId);

        try {
            // Prepare request payload for Python FastAPI
            Map<String, Object> requestPayload = new HashMap<>();
            requestPayload.put("recordId", recordId);
            requestPayload.put("dataPayload", dataPayload);
            requestPayload.put("processType", "dataingestion");

            // Set headers
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<Map<String, Object>> entity = new HttpEntity<>(requestPayload, headers);

            // Call Python FastAPI endpoint
            String pythonEndpoint = pythonFastApiBaseUrl+"/dataingestion/process";
            ResponseEntity<Map> response = restTemplate.postForEntity(pythonEndpoint, entity, Map.class);

            if (response.getStatusCode() == HttpStatus.OK && response.getBody() != null) {
                Map<String, Object> responseBody = response.getBody();

                // Update record with Python processing results
                Optional<DataIngestionRecord> existingRecord = getRecordByRecordId(recordId);
                if (existingRecord.isPresent()) {
                    DataIngestionRecord record = existingRecord.get();
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
            logger.error("Error calling Python FastAPI for dataingestion processing: {}", e.getMessage());
            throw new RuntimeException("Failed to process record via Python API", e);
        }

        throw new RuntimeException("Failed to process dataingestion record via Python API");
    }

    @Override
    public boolean validateRecordViaPython(String recordId, String dataPayload) {
        logger.info("Validating dataingestion record {} via Python FastAPI", recordId);

        try {
            // Prepare request payload for Python FastAPI
            Map<String, Object> requestPayload = new HashMap<>();
            requestPayload.put("recordId", recordId);
            requestPayload.put("dataPayload", dataPayload);

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<Map<String, Object>> entity = new HttpEntity<>(requestPayload, headers);

            // Call Python FastAPI validation endpoint
            String pythonEndpoint = pythonFastApiBaseUrl+"/dataingestion/validate";
            ResponseEntity<Map> response = restTemplate.postForEntity(pythonEndpoint, entity, Map.class);

            if (response.getStatusCode() == HttpStatus.OK && response.getBody() != null) {
                Map<String, Object> responseBody = response.getBody();
                return (Boolean) responseBody.getOrDefault("isValid", false);
            }

        } catch (RestClientException e) {
            logger.error("Error calling Python FastAPI for dataingestion validation: {}", e.getMessage());
        }

        return false;
    }

    @Override
    public String getProcessingStatusFromPython(String recordId) {
        logger.info("Getting processing status for dataingestion record {} from Python FastAPI", recordId);

        try {
            // Call Python FastAPI status endpoint
            String pythonEndpoint = pythonFastApiBaseUrl+"/dataingestion/status/"+recordId;
            ResponseEntity<Map> response = restTemplate.getForEntity(pythonEndpoint, Map.class);

            if (response.getStatusCode() == HttpStatus.OK && response.getBody() != null) {
                Map<String, Object> responseBody = response.getBody();
                return (String) responseBody.getOrDefault("status", "UNKNOWN");
            }

        } catch (RestClientException e) {
            logger.error("Error calling Python FastAPI for dataingestion status: {}", e.getMessage());
        }

        return "ERROR";
    }

    @Override
    public void updateRecordStatus(String recordId, String newStatus) {
        logger.info("Updating status for dataingestion record {} to {}", recordId, newStatus);
        dataingestionRepositoryImpl.updateRecordStatus(recordId, newStatus);
    }

    @Override
    public Long getRecordCountByStatus(String status) {
        logger.info("Getting count of dataingestion records with status: {}", status);
        return dataingestionRepository.countByStatus(status);
    }

       @Override
       public Map<String, Object> classifyText(String text, Boolean hadFile) {
           logger.info("Classifying text via Python FastAPI (hadFile={})", hadFile);
   
           try {
               Map<String, Object> requestPayload = new HashMap<>();
               requestPayload.put("text", text);
               if (hadFile != null) {
                   requestPayload.put("had_file", hadFile);
               }
   
               HttpHeaders headers = new HttpHeaders();
               headers.setContentType(MediaType.APPLICATION_JSON);
               HttpEntity<Map<String, Object>> entity = new HttpEntity<>(requestPayload, headers);
   
               String pythonEndpoint = pythonFastApiBaseUrl+"/classify";
               ResponseEntity<Map> response = restTemplate.postForEntity(pythonEndpoint, entity, Map.class);
   
               if (response.getStatusCode() == HttpStatus.OK && response.getBody() != null) {
                   //noinspection unchecked
                   return response.getBody();
               }
   
               throw new RuntimeException("Non-OK response from Python classify API: "+response.getStatusCode());
           } catch (RestClientException e) {
               logger.error("Error calling Python FastAPI /classify: {}", e.getMessage());
               throw new RuntimeException("Failed to classify text via Python API", e);
           }
       }
}