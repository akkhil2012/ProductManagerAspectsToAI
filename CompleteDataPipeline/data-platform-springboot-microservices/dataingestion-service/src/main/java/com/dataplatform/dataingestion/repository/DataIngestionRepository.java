package com.dataplatform.dataingestion.repository;

import com.dataplatform.dataingestion.model.DataIngestionRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface DataIngestionRepository extends JpaRepository<DataIngestionRecord, Long> {

    Optional<DataIngestionRecord> findByRecordId(String recordId);

    List<DataIngestionRecord> findByStatus(String status);

    List<DataIngestionRecord> findByCreatedAtBetween(LocalDateTime startDate, LocalDateTime endDate);

    @Query("SELECT r FROM DataIngestionRecord r WHERE r.processingTimestamp >= :timestamp")
    List<DataIngestionRecord> findRecentRecords(@Param("timestamp") LocalDateTime timestamp);

    @Query("SELECT COUNT(r) FROM DataIngestionRecord r WHERE r.status = :status")
    Long countByStatus(@Param("status") String status);

    void deleteByRecordId(String recordId);
}