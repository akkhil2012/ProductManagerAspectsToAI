package com.dataplatform.dataconsumption.repository;

import com.dataplatform.dataconsumption.model.DataConsumptionRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface DataConsumptionRepository extends JpaRepository<DataConsumptionRecord, Long> {

    Optional<DataConsumptionRecord> findByRecordId(String recordId);

    List<DataConsumptionRecord> findByStatus(String status);

    List<DataConsumptionRecord> findByCreatedAtBetween(LocalDateTime startDate, LocalDateTime endDate);

    @Query("SELECT r FROM DataConsumptionRecord r WHERE r.processingTimestamp >= :timestamp")
    List<DataConsumptionRecord> findRecentRecords(@Param("timestamp") LocalDateTime timestamp);

    @Query("SELECT COUNT(r) FROM DataConsumptionRecord r WHERE r.status = :status")
    Long countByStatus(@Param("status") String status);

    void deleteByRecordId(String recordId);
}