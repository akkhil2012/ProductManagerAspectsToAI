package com.dataplatform.datastorage.service;

import com.dataplatform.datastorage.model.DataStorageRecord;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

public interface DataStorageService {

    /**
     * Create a new datastorage record
     */
    DataStorageRecord createRecord(DataStorageRecord record);

    /**
     * Update an existing datastorage record
     */
    DataStorageRecord updateRecord(Long id, DataStorageRecord record);

    /**
     * Delete a datastorage record by ID
     */
    void deleteRecord(Long id);

    /**
     * Get a datastorage record by ID
     */
    Optional<DataStorageRecord> getRecordById(Long id);

    /**
     * Get a datastorage record by record ID
     */
    Optional<DataStorageRecord> getRecordByRecordId(String recordId);

    /**
     * Get all datastorage records
     */
    List<DataStorageRecord> getAllRecords();

    /**
     * Get records by status
     */
    List<DataStorageRecord> getRecordsByStatus(String status);

    /**
     * Get records created between specific dates
     */
    List<DataStorageRecord> getRecordsByDateRange(LocalDateTime startDate, LocalDateTime endDate);

    /**
     * Process record using Python FastAPI endpoint
     */
    DataStorageRecord processRecordViaPython(String recordId, String dataPayload);

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