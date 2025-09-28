package com.dataplatform.dataquality.service;

import com.dataplatform.dataquality.model.DataQualityRecord;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

public interface DataQualityService {

    /**
     * Create a new dataquality record
     */
    DataQualityRecord createRecord(DataQualityRecord record);

    /**
     * Update an existing dataquality record
     */
    DataQualityRecord updateRecord(Long id, DataQualityRecord record);

    /**
     * Delete a dataquality record by ID
     */
    void deleteRecord(Long id);

    /**
     * Get a dataquality record by ID
     */
    Optional<DataQualityRecord> getRecordById(Long id);

    /**
     * Get a dataquality record by record ID
     */
    Optional<DataQualityRecord> getRecordByRecordId(String recordId);

    /**
     * Get all dataquality records
     */
    List<DataQualityRecord> getAllRecords();

    /**
     * Get records by status
     */
    List<DataQualityRecord> getRecordsByStatus(String status);

    /**
     * Get records created between specific dates
     */
    List<DataQualityRecord> getRecordsByDateRange(LocalDateTime startDate, LocalDateTime endDate);

    /**
     * Process record using Python FastAPI endpoint
     */
    DataQualityRecord processRecordViaPython(String recordId, String dataPayload);

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