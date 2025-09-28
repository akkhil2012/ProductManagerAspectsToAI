package com.dataplatform.dataquality.repository;

import com.dataplatform.dataquality.model.DataQualityRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface DataQualityRepository extends JpaRepository<DataQualityRecord, Long> {

    Optional<DataQualityRecord> findByRecordId(String recordId);

    List<DataQualityRecord> findByStatus(String status);

    List<DataQualityRecord> findByCreatedAtBetween(LocalDateTime startDate, LocalDateTime endDate);

    @Query("SELECT r FROM DataQualityRecord r WHERE r.processingTimestamp >= :timestamp")
    List<DataQualityRecord> findRecentRecords(@Param("timestamp") LocalDateTime timestamp);

    @Query("SELECT COUNT(r) FROM DataQualityRecord r WHERE r.status = :status")
    Long countByStatus(@Param("status") String status);

    void deleteByRecordId(String recordId);
}