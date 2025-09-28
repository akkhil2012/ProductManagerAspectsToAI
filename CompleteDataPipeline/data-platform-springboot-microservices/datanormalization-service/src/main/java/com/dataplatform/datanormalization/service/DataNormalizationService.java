package com.dataplatform.datanormalization.service;

import com.dataplatform.datanormalization.model.DataNormalizationRecord;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

public interface DataNormalizationService {

    /**
     * Create a new datanormalization record
     */
    DataNormalizationRecord createRecord(DataNormalizationRecord record);

    /**
     * Update an existing datanormalization record
     */
    DataNormalizationRecord updateRecord(Long id, DataNormalizationRecord record);

    /**
     * Delete a datanormalization record by ID
     */
    void deleteRecord(Long id);

    /**
     * Get a datanormalization record by ID
     */
    Optional<DataNormalizationRecord> getRecordById(Long id);

    /**
     * Get a datanormalization record by record ID
     */
    Optional<DataNormalizationRecord> getRecordByRecordId(String recordId);

    /**
     * Get all datanormalization records
     */
    List<DataNormalizationRecord> getAllRecords();

    /**
     * Get records by status
     */
    List<DataNormalizationRecord> getRecordsByStatus(String status);

    /**
     * Get records created between specific dates
     */
    List<DataNormalizationRecord> getRecordsByDateRange(LocalDateTime startDate, LocalDateTime endDate);

    /**
     * Process record using Python FastAPI endpoint
     */
    DataNormalizationRecord processRecordViaPython(String recordId, String dataPayload);

    /**
     * Validate record using Python FastAPI endpoint
     */
    boolean validateRecordViaPython(String recordId, String dataPayload);

    /**
     * Get processing status from Python FastAPI endpoint
     */
    String getProcessingStatusFromPython(String recordId);

    /**
     * Update record status
     */
    void updateRecordStatus(String recordId, String newStatus);

    /**
     * Get count of records by status
     */
    Long getRecordCountByStatus(String status);
}