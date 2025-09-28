package com.dataplatform.datastorage.repository;

import com.dataplatform.datastorage.model.DataStorageRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.TypedQuery;
import javax.transaction.Transactional;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
@Transactional
public class DataStorageRepositoryImpl {

    @PersistenceContext
    private EntityManager entityManager;

    @Autowired
    private DataStorageRepository datastorageRepository;

    public List<DataStorageRecord> findRecordsWithCustomCriteria(String status, LocalDateTime fromDate) {
        String jpql = "SELECT r FROM DataStorageRecord r WHERE r.status = :status AND r.createdAt >= :fromDate ORDER BY r.createdAt DESC";
        TypedQuery<DataStorageRecord> query = entityManager.createQuery(jpql, DataStorageRecord.class);
        query.setParameter("status", status);
        query.setParameter("fromDate", fromDate);
        return query.getResultList();
    }

    public Optional<DataStorageRecord> findLatestRecord() {
        String jpql = "SELECT r FROM DataStorageRecord r ORDER BY r.createdAt DESC";
        TypedQuery<DataStorageRecord> query = entityManager.createQuery(jpql, DataStorageRecord.class);
        query.setMaxResults(1);
        List<DataStorageRecord> results = query.getResultList();
        return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
    }

    public void updateRecordStatus(String recordId, String newStatus) {
        String jpql = "UPDATE DataStorageRecord r SET r.status = :newStatus, r.updatedAt = :updatedAt WHERE r.recordId = :recordId";
        entityManager.createQuery(jpql)
            .setParameter("newStatus", newStatus)
            .setParameter("updatedAt", LocalDateTime.now())
            .setParameter("recordId", recordId)
            .executeUpdate();
    }

    public List<DataStorageRecord> findRecordsByStatusAndDateRange(String status, LocalDateTime startDate, LocalDateTime endDate) {
        String jpql = "SELECT r FROM DataStorageRecord r WHERE r.status = :status AND r.createdAt BETWEEN :startDate AND :endDate";
        TypedQuery<DataStorageRecord> query = entityManager.createQuery(jpql, DataStorageRecord.class);
        query.setParameter("status", status);
        query.setParameter("startDate", startDate);
        query.setParameter("endDate", endDate);
        return query.getResultList();
    }
}