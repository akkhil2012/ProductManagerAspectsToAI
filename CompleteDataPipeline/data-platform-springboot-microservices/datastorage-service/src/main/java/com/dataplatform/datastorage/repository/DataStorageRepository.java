package com.dataplatform.datastorage.repository;

import com.dataplatform.datastorage.model.DataStorageRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface DataStorageRepository extends JpaRepository<DataStorageRecord, Long> {

    Optional<DataStorageRecord> findByRecordId(String recordId);

    List<DataStorageRecord> findByStatus(String status);

    List<DataStorageRecord> findByCreatedAtBetween(LocalDateTime startDate, LocalDateTime endDate);

    @Query("SELECT r FROM DataStorageRecord r WHERE r.processingTimestamp >= :timestamp")
    List<DataStorageRecord> findRecentRecords(@Param("timestamp") LocalDateTime timestamp);

    @Query("SELECT COUNT(r) FROM DataStorageRecord r WHERE r.status = :status")
    Long countByStatus(@Param("status") String status);

    void deleteByRecordId(String recordId);
}