package com.dataplatform.datadeduplication.service;

import com.dataplatform.datadeduplication.model.DataDeduplicationRecord;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

public interface DataDeduplicationService {

    /**
     * Create a new datadeduplication record
     */
    DataDeduplicationRecord createRecord(DataDeduplicationRecord record);

    /**
     * Update an existing datadeduplication record
     */
    DataDeduplicationRecord updateRecord(Long id, DataDeduplicationRecord record);

    /**
     * Delete a datadeduplication record by ID
     */
    void deleteRecord(Long id);

    /**
     * Get a datadeduplication record by ID
     */
    Optional<DataDeduplicationRecord> getRecordById(Long id);

    /**
     * Get a datadeduplication record by record ID
     */
    Optional<DataDeduplicationRecord> getRecordByRecordId(String recordId);

    /**
     * Get all datadeduplication records
     */
    List<DataDeduplicationRecord> getAllRecords();

    /**
     * Get records by status
     */
    List<DataDeduplicationRecord> getRecordsByStatus(String status);

    /**
     * Get records created between specific dates
     */
    List<DataDeduplicationRecord> getRecordsByDateRange(LocalDateTime startDate, LocalDateTime endDate);

    /**
     * Process record using Python FastAPI endpoint
     */
    DataDeduplicationRecord processRecordViaPython(String recordId, String dataPayload);

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