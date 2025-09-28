package com.dataplatform.dataconsumption.service;

import com.dataplatform.dataconsumption.model.DataConsumptionRecord;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

public interface DataConsumptionService {

    /**
     * Create a new dataconsumption record
     */
    DataConsumptionRecord createRecord(DataConsumptionRecord record);

    /**
     * Update an existing dataconsumption record
     */
    DataConsumptionRecord updateRecord(Long id, DataConsumptionRecord record);

    /**
     * Delete a dataconsumption record by ID
     */
    void deleteRecord(Long id);

    /**
     * Get a dataconsumption record by ID
     */
    Optional<DataConsumptionRecord> getRecordById(Long id);

    /**
     * Get a dataconsumption record by record ID
     */
    Optional<DataConsumptionRecord> getRecordByRecordId(String recordId);

    /**
     * Get all dataconsumption records
     */
    List<DataConsumptionRecord> getAllRecords();

    /**
     * Get records by status
     */
    List<DataConsumptionRecord> getRecordsByStatus(String status);

    /**
     * Get records created between specific dates
     */
    List<DataConsumptionRecord> getRecordsByDateRange(LocalDateTime startDate, LocalDateTime endDate);

    /**
     * Process record using Python FastAPI endpoint
     */
    DataConsumptionRecord processRecordViaPython(String recordId, String dataPayload);

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