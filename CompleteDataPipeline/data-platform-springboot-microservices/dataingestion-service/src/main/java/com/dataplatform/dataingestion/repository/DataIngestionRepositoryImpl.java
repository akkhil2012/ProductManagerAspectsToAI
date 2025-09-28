package com.dataplatform.dataingestion.repository;

import com.dataplatform.dataingestion.model.DataIngestionRecord;
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
public class DataIngestionRepositoryImpl {

    @PersistenceContext
    private EntityManager entityManager;

    @Autowired
    private DataIngestionRepository dataingestionRepository;

    public List<DataIngestionRecord> findRecordsWithCustomCriteria(String status, LocalDateTime fromDate) {
        String jpql = "SELECT r FROM DataIngestionRecord r WHERE r.status = :status AND r.createdAt >= :fromDate ORDER BY r.createdAt DESC";
        TypedQuery<DataIngestionRecord> query = entityManager.createQuery(jpql, DataIngestionRecord.class);
        query.setParameter("status", status);
        query.setParameter("fromDate", fromDate);
        return query.getResultList();
    }

    public Optional<DataIngestionRecord> findLatestRecord() {
        String jpql = "SELECT r FROM DataIngestionRecord r ORDER BY r.createdAt DESC";
        TypedQuery<DataIngestionRecord> query = entityManager.createQuery(jpql, DataIngestionRecord.class);
        query.setMaxResults(1);
        List<DataIngestionRecord> results = query.getResultList();
        return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
    }

    public void updateRecordStatus(String recordId, String newStatus) {
        String jpql = "UPDATE DataIngestionRecord r SET r.status = :newStatus, r.updatedAt = :updatedAt WHERE r.recordId = :recordId";
        entityManager.createQuery(jpql)
            .setParameter("newStatus", newStatus)
            .setParameter("updatedAt", LocalDateTime.now())
            .setParameter("recordId", recordId)
            .executeUpdate();
    }

    public List<DataIngestionRecord> findRecordsByStatusAndDateRange(String status, LocalDateTime startDate, LocalDateTime endDate) {
        String jpql = "SELECT r FROM DataIngestionRecord r WHERE r.status = :status AND r.createdAt BETWEEN :startDate AND :endDate";
        TypedQuery<DataIngestionRecord> query = entityManager.createQuery(jpql, DataIngestionRecord.class);
        query.setParameter("status", status);
        query.setParameter("startDate", startDate);
        query.setParameter("endDate", endDate);
        return query.getResultList();
    }
}