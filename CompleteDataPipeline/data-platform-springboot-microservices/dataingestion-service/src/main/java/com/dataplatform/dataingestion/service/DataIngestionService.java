package com.dataplatform.dataingestion.service;

import com.dataplatform.dataingestion.model.DataIngestionRecord;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

public interface DataIngestionService {

    /**
     * Create a new dataingestion record
     */
    DataIngestionRecord createRecord(DataIngestionRecord record);

    /**
     * Update an existing dataingestion record
     */
    DataIngestionRecord updateRecord(Long id, DataIngestionRecord record);

    /**
     * Delete a dataingestion record by ID
     */
    void deleteRecord(Long id);

    /**
     * Get a dataingestion record by ID
     */
    Optional<DataIngestionRecord> getRecordById(Long id);

    /**
     * Get a dataingestion record by record ID
     */
    Optional<DataIngestionRecord> getRecordByRecordId(String recordId);

    /**
     * Get all dataingestion records
     */
    List<DataIngestionRecord> getAllRecords();

    /**
     * Get records by status
     */
    List<DataIngestionRecord> getRecordsByStatus(String status);

    /**
     * Get records created between specific dates
     */
    List<DataIngestionRecord> getRecordsByDateRange(LocalDateTime startDate, LocalDateTime endDate);

    /**
     * Process record using Python FastAPI endpoint
     */
    DataIngestionRecord processRecordViaPython(String recordId, String dataPayload);

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