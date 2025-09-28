package com.dataplatform.dataquality.repository;

import com.dataplatform.dataquality.model.DataQualityRecord;
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
public class DataQualityRepositoryImpl {

    @PersistenceContext
    private EntityManager entityManager;

    @Autowired
    private DataQualityRepository dataqualityRepository;

    public List<DataQualityRecord> findRecordsWithCustomCriteria(String status, LocalDateTime fromDate) {
        String jpql = "SELECT r FROM DataQualityRecord r WHERE r.status = :status AND r.createdAt >= :fromDate ORDER BY r.createdAt DESC";
        TypedQuery<DataQualityRecord> query = entityManager.createQuery(jpql, DataQualityRecord.class);
        query.setParameter("status", status);
        query.setParameter("fromDate", fromDate);
        return query.getResultList();
    }

    public Optional<DataQualityRecord> findLatestRecord() {
        String jpql = "SELECT r FROM DataQualityRecord r ORDER BY r.createdAt DESC";
        TypedQuery<DataQualityRecord> query = entityManager.createQuery(jpql, DataQualityRecord.class);
        query.setMaxResults(1);
        List<DataQualityRecord> results = query.getResultList();
        return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
    }

    public void updateRecordStatus(String recordId, String newStatus) {
        String jpql = "UPDATE DataQualityRecord r SET r.status = :newStatus, r.updatedAt = :updatedAt WHERE r.recordId = :recordId";
        entityManager.createQuery(jpql)
            .setParameter("newStatus", newStatus)
            .setParameter("updatedAt", LocalDateTime.now())
            .setParameter("recordId", recordId)
            .executeUpdate();
    }

    public List<DataQualityRecord> findRecordsByStatusAndDateRange(String status, LocalDateTime startDate, LocalDateTime endDate) {
        String jpql = "SELECT r FROM DataQualityRecord r WHERE r.status = :status AND r.createdAt BETWEEN :startDate AND :endDate";
        TypedQuery<DataQualityRecord> query = entityManager.createQuery(jpql, DataQualityRecord.class);
        query.setParameter("status", status);
        query.setParameter("startDate", startDate);
        query.setParameter("endDate", endDate);
        return query.getResultList();
    }
}