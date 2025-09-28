package com.dataplatform.datadeduplication.repository;

import com.dataplatform.datadeduplication.model.DataDeduplicationRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface DataDeduplicationRepository extends JpaRepository<DataDeduplicationRecord, Long> {

    Optional<DataDeduplicationRecord> findByRecordId(String recordId);

    List<DataDeduplicationRecord> findByStatus(String status);

    List<DataDeduplicationRecord> findByCreatedAtBetween(LocalDateTime startDate, LocalDateTime endDate);

    @Query("SELECT r FROM DataDeduplicationRecord r WHERE r.processingTimestamp >= :timestamp")
    List<DataDeduplicationRecord> findRecentRecords(@Param("timestamp") LocalDateTime timestamp);

    @Query("SELECT COUNT(r) FROM DataDeduplicationRecord r WHERE r.status = :status")
    Long countByStatus(@Param("status") String status);

    void deleteByRecordId(String recordId);
}