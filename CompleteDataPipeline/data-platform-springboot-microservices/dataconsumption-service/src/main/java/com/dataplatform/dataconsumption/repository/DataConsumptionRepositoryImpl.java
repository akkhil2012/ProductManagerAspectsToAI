package com.dataplatform.dataconsumption.repository;

import com.dataplatform.dataconsumption.model.DataConsumptionRecord;
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
public class DataConsumptionRepositoryImpl {

    @PersistenceContext
    private EntityManager entityManager;

    @Autowired
    private DataConsumptionRepository dataconsumptionRepository;

    public List<DataConsumptionRecord> findRecordsWithCustomCriteria(String status, LocalDateTime fromDate) {
        String jpql = "SELECT r FROM DataConsumptionRecord r WHERE r.status = :status AND r.createdAt >= :fromDate ORDER BY r.createdAt DESC";
        TypedQuery<DataConsumptionRecord> query = entityManager.createQuery(jpql, DataConsumptionRecord.class);
        query.setParameter("status", status);
        query.setParameter("fromDate", fromDate);
        return query.getResultList();
    }

    public Optional<DataConsumptionRecord> findLatestRecord() {
        String jpql = "SELECT r FROM DataConsumptionRecord r ORDER BY r.createdAt DESC";
        TypedQuery<DataConsumptionRecord> query = entityManager.createQuery(jpql, DataConsumptionRecord.class);
        query.setMaxResults(1);
        List<DataConsumptionRecord> results = query.getResultList();
        return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
    }

    public void updateRecordStatus(String recordId, String newStatus) {
        String jpql = "UPDATE DataConsumptionRecord r SET r.status = :newStatus, r.updatedAt = :updatedAt WHERE r.recordId = :recordId";
        entityManager.createQuery(jpql)
            .setParameter("newStatus", newStatus)
            .setParameter("updatedAt", LocalDateTime.now())
            .setParameter("recordId", recordId)
            .executeUpdate();
    }

    public List<DataConsumptionRecord> findRecordsByStatusAndDateRange(String status, LocalDateTime startDate, LocalDateTime endDate) {
        String jpql = "SELECT r FROM DataConsumptionRecord r WHERE r.status = :status AND r.createdAt BETWEEN :startDate AND :endDate";
        TypedQuery<DataConsumptionRecord> query = entityManager.createQuery(jpql, DataConsumptionRecord.class);
        query.setParameter("status", status);
        query.setParameter("startDate", startDate);
        query.setParameter("endDate", endDate);
        return query.getResultList();
    }
}