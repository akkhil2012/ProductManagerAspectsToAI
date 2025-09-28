package com.dataplatform.datadeduplication.repository;

import com.dataplatform.datadeduplication.model.DataDeduplicationRecord;
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
public class DataDeduplicationRepositoryImpl {

    @PersistenceContext
    private EntityManager entityManager;

    @Autowired
    private DataDeduplicationRepository datadeduplicationRepository;

    public List<DataDeduplicationRecord> findRecordsWithCustomCriteria(String status, LocalDateTime fromDate) {
        String jpql = "SELECT r FROM DataDeduplicationRecord r WHERE r.status = :status AND r.createdAt >= :fromDate ORDER BY r.createdAt DESC";
        TypedQuery<DataDeduplicationRecord> query = entityManager.createQuery(jpql, DataDeduplicationRecord.class);
        query.setParameter("status", status);
        query.setParameter("fromDate", fromDate);
        return query.getResultList();
    }

    public Optional<DataDeduplicationRecord> findLatestRecord() {
        String jpql = "SELECT r FROM DataDeduplicationRecord r ORDER BY r.createdAt DESC";
        TypedQuery<DataDeduplicationRecord> query = entityManager.createQuery(jpql, DataDeduplicationRecord.class);
        query.setMaxResults(1);
        List<DataDeduplicationRecord> results = query.getResultList();
        return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
    }

    public void updateRecordStatus(String recordId, String newStatus) {
        String jpql = "UPDATE DataDeduplicationRecord r SET r.status = :newStatus, r.updatedAt = :updatedAt WHERE r.recordId = :recordId";
        entityManager.createQuery(jpql)
            .setParameter("newStatus", newStatus)
            .setParameter("updatedAt", LocalDateTime.now())
            .setParameter("recordId", recordId)
            .executeUpdate();
    }

    public List<DataDeduplicationRecord> findRecordsByStatusAndDateRange(String status, LocalDateTime startDate, LocalDateTime endDate) {
        String jpql = "SELECT r FROM DataDeduplicationRecord r WHERE r.status = :status AND r.createdAt BETWEEN :startDate AND :endDate";
        TypedQuery<DataDeduplicationRecord> query = entityManager.createQuery(jpql, DataDeduplicationRecord.class);
        query.setParameter("status", status);
        query.setParameter("startDate", startDate);
        query.setParameter("endDate", endDate);
        return query.getResultList();
    }
}