package com.dataplatform.datanormalization.repository;

import com.dataplatform.datanormalization.model.DataNormalizationRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface DataNormalizationRepository extends JpaRepository<DataNormalizationRecord, Long> {

    Optional<DataNormalizationRecord> findByRecordId(String recordId);

    List<DataNormalizationRecord> findByStatus(String status);

    List<DataNormalizationRecord> findByCreatedAtBetween(LocalDateTime startDate, LocalDateTime endDate);

    @Query("SELECT r FROM DataNormalizationRecord r WHERE r.processingTimestamp >= :timestamp")
    List<DataNormalizationRecord> findRecentRecords(@Param("timestamp") LocalDateTime timestamp);

    @Query("SELECT COUNT(r) FROM DataNormalizationRecord r WHERE r.status = :status")
    Long countByStatus(@Param("status") String status);

    void deleteByRecordId(String recordId);
}