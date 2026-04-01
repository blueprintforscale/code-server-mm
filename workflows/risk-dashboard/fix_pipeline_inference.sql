-- Update v_lead_pipeline: infer inspection_scheduled_at when status = 'needs scheduling'
-- but downstream activity proves inspection happened
-- Also add inspection_scheduled_inferred flag

DROP VIEW IF EXISTS v_lead_pipeline;

CREATE VIEW v_lead_pipeline AS
SELECT hcp_customer_id,
   customer_id,
   first_name,
   last_name,
   callrail_id,
   -- Lead date from call or form
   COALESCE(
     (SELECT min(c.start_time) FROM calls c WHERE c.callrail_id = hc.callrail_id),
     (SELECT min(f.submitted_at) FROM form_submissions f WHERE f.callrail_id = hc.callrail_id)
   ) AS lead_at,
   -- Inspection scheduled: actual date OR inferred from downstream activity
   COALESCE(
     -- Actual scheduled_at from inspections
     (SELECT min(i.scheduled_at) FROM hcp_inspections i
       WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.record_status = 'active'
       AND i.scheduled_at IS NOT NULL),
     -- Inferred: 'needs scheduling' but has estimate sent or inspection invoice
     CASE WHEN EXISTS (
       SELECT 1 FROM hcp_inspections i
       WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.record_status = 'active'
     ) AND (
       EXISTS (SELECT 1 FROM hcp_estimates e WHERE e.hcp_customer_id = hc.hcp_customer_id AND e.sent_at IS NOT NULL AND e.record_status = 'active')
       OR EXISTS (SELECT 1 FROM hcp_invoices inv WHERE inv.hcp_customer_id = hc.hcp_customer_id AND inv.invoice_type = 'inspection' AND inv.status != 'canceled')
       OR EXISTS (SELECT 1 FROM hcp_jobs j WHERE j.hcp_customer_id = hc.hcp_customer_id AND j.record_status = 'active')
     ) THEN COALESCE(
       (SELECT min(i.hcp_created_at) FROM hcp_inspections i WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.record_status = 'active'),
       (SELECT min(e.sent_at) FROM hcp_estimates e WHERE e.hcp_customer_id = hc.hcp_customer_id AND e.record_status = 'active' AND e.sent_at IS NOT NULL),
       (SELECT min(inv.created_at) FROM hcp_invoices inv WHERE inv.hcp_customer_id = hc.hcp_customer_id AND inv.invoice_type = 'inspection' AND inv.status != 'canceled')
     )
     ELSE NULL END
   ) AS inspection_scheduled_at,
   -- Flag: was it inferred?
   CASE WHEN (SELECT min(i.scheduled_at) FROM hcp_inspections i
     WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.record_status = 'active') IS NOT NULL
     THEN false
     WHEN EXISTS (SELECT 1 FROM hcp_inspections i WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.record_status = 'active')
       AND (EXISTS (SELECT 1 FROM hcp_estimates e WHERE e.hcp_customer_id = hc.hcp_customer_id AND e.sent_at IS NOT NULL)
         OR EXISTS (SELECT 1 FROM hcp_invoices inv WHERE inv.hcp_customer_id = hc.hcp_customer_id AND inv.invoice_type = 'inspection' AND inv.status != 'canceled')
         OR EXISTS (SELECT 1 FROM hcp_jobs j WHERE j.hcp_customer_id = hc.hcp_customer_id AND j.record_status = 'active'))
     THEN true
     ELSE NULL
   END AS inspection_scheduled_inferred,
   -- Inspection completed (existing logic + inference)
   COALESCE(
     (SELECT min(i.completed_at) FROM hcp_inspections i
       WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.record_status = 'active' AND i.completed_at IS NOT NULL),
     (SELECT min(e.sent_at) FROM hcp_estimates e
       WHERE e.hcp_customer_id = hc.hcp_customer_id AND e.record_status = 'active' AND e.sent_at IS NOT NULL
       AND EXISTS (SELECT 1 FROM hcp_inspections i2 WHERE i2.hcp_customer_id = hc.hcp_customer_id AND i2.record_status = 'active' AND i2.completed_at IS NULL)),
     (SELECT min(inv.invoice_date) FROM hcp_invoices inv
       WHERE inv.hcp_customer_id = hc.hcp_customer_id AND inv.invoice_type = 'inspection' AND inv.status != 'canceled'
       AND EXISTS (SELECT 1 FROM hcp_inspections i3 WHERE i3.hcp_customer_id = hc.hcp_customer_id AND i3.record_status = 'active' AND i3.completed_at IS NULL))
   ) AS inspection_completed_at,
   CASE
     WHEN (SELECT min(i.completed_at) FROM hcp_inspections i
       WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.record_status = 'active') IS NOT NULL THEN false
     WHEN (SELECT min(e.sent_at) FROM hcp_estimates e
       WHERE e.hcp_customer_id = hc.hcp_customer_id AND e.record_status = 'active') IS NOT NULL
       OR (SELECT count(*) FROM hcp_invoices inv
       WHERE inv.hcp_customer_id = hc.hcp_customer_id AND inv.invoice_type = 'inspection' AND inv.status != 'canceled') > 0 THEN true
     ELSE NULL
   END AS inspection_completed_inferred,
   (SELECT min(inv.paid_at) FROM hcp_invoices inv
     WHERE inv.hcp_customer_id = hc.hcp_customer_id AND inv.invoice_type = 'inspection' AND inv.status != 'canceled' AND inv.paid_at IS NOT NULL) AS inspection_paid_at,
   (SELECT min(e.sent_at) FROM hcp_estimates e
     WHERE e.hcp_customer_id = hc.hcp_customer_id AND e.record_status = 'active' AND e.sent_at IS NOT NULL) AS estimate_sent_at,
   COALESCE(
     (SELECT min(e.approved_at) FROM hcp_estimates e
       WHERE e.hcp_customer_id = hc.hcp_customer_id AND e.status = 'approved' AND e.record_status = 'active' AND e.approved_at IS NOT NULL),
     (SELECT min(e.sent_at) FROM hcp_estimates e
       WHERE e.hcp_customer_id = hc.hcp_customer_id AND e.status = 'approved' AND e.record_status = 'active'),
     (SELECT min(j.scheduled_at) FROM hcp_jobs j
       WHERE j.hcp_customer_id = hc.hcp_customer_id AND j.record_status = 'active' AND j.status NOT IN ('user canceled','pro canceled')
       AND EXISTS (SELECT 1 FROM hcp_estimates e2 WHERE e2.hcp_customer_id = hc.hcp_customer_id AND e2.status = 'approved'))
   ) AS estimate_approved_at,
   (SELECT min(j.scheduled_at) FROM hcp_jobs j
     WHERE j.hcp_customer_id = hc.hcp_customer_id AND j.record_status = 'active' AND j.status NOT IN ('user canceled','pro canceled')) AS job_scheduled_at,
   COALESCE(
     (SELECT min(j.completed_at) FROM hcp_jobs j
       WHERE j.hcp_customer_id = hc.hcp_customer_id AND j.record_status = 'active' AND j.completed_at IS NOT NULL),
     (SELECT min(inv.invoice_date) FROM hcp_invoices inv
       WHERE inv.hcp_customer_id = hc.hcp_customer_id AND inv.invoice_type = 'treatment' AND inv.status != 'canceled'
       AND EXISTS (SELECT 1 FROM hcp_jobs j2 WHERE j2.hcp_customer_id = hc.hcp_customer_id AND j2.record_status = 'active' AND j2.completed_at IS NULL))
   ) AS job_completed_at,
   CASE
     WHEN (SELECT min(j.completed_at) FROM hcp_jobs j WHERE j.hcp_customer_id = hc.hcp_customer_id AND j.record_status = 'active') IS NOT NULL THEN false
     WHEN (SELECT count(*) FROM hcp_invoices inv WHERE inv.hcp_customer_id = hc.hcp_customer_id AND inv.invoice_type = 'treatment' AND inv.status != 'canceled') > 0 THEN true
     ELSE NULL
   END AS job_completed_inferred,
   (SELECT min(inv.paid_at) FROM hcp_invoices inv
     WHERE inv.hcp_customer_id = hc.hcp_customer_id AND inv.invoice_type = 'treatment' AND inv.status != 'canceled' AND inv.paid_at IS NOT NULL) AS job_paid_at,
   CASE
     WHEN (SELECT min(inv.paid_at) FROM hcp_invoices inv WHERE inv.hcp_customer_id = hc.hcp_customer_id AND inv.invoice_type = 'treatment' AND inv.status != 'canceled' AND inv.paid_at IS NOT NULL) IS NOT NULL THEN 'job_paid'
     WHEN COALESCE((SELECT min(j.completed_at) FROM hcp_jobs j WHERE j.hcp_customer_id = hc.hcp_customer_id AND j.record_status = 'active'),
       (SELECT min(inv.invoice_date) FROM hcp_invoices inv WHERE inv.hcp_customer_id = hc.hcp_customer_id AND inv.invoice_type = 'treatment' AND inv.status != 'canceled')) IS NOT NULL THEN 'job_completed'
     WHEN (SELECT min(j.scheduled_at) FROM hcp_jobs j WHERE j.hcp_customer_id = hc.hcp_customer_id AND j.record_status = 'active' AND j.status NOT IN ('user canceled','pro canceled')) IS NOT NULL THEN 'job_scheduled'
     WHEN (SELECT count(*) FROM hcp_estimates e WHERE e.hcp_customer_id = hc.hcp_customer_id AND e.status = 'approved' AND e.record_status = 'active') > 0 THEN 'estimate_approved'
     WHEN (SELECT min(e.sent_at) FROM hcp_estimates e WHERE e.hcp_customer_id = hc.hcp_customer_id AND e.record_status = 'active') IS NOT NULL THEN 'estimate_sent'
     WHEN COALESCE((SELECT min(i.completed_at) FROM hcp_inspections i WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.record_status = 'active'),
       (SELECT min(e.sent_at) FROM hcp_estimates e WHERE e.hcp_customer_id = hc.hcp_customer_id AND e.record_status = 'active'),
       (SELECT min(inv.invoice_date) FROM hcp_invoices inv WHERE inv.hcp_customer_id = hc.hcp_customer_id AND inv.invoice_type = 'inspection' AND inv.status != 'canceled')) IS NOT NULL THEN 'inspection_completed'
     WHEN (SELECT min(inv.paid_at) FROM hcp_invoices inv WHERE inv.hcp_customer_id = hc.hcp_customer_id AND inv.invoice_type = 'inspection' AND inv.status != 'canceled' AND inv.paid_at IS NOT NULL) IS NOT NULL THEN 'inspection_paid'
     WHEN COALESCE(
       (SELECT min(i.scheduled_at) FROM hcp_inspections i WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.record_status = 'active'),
       CASE WHEN EXISTS (SELECT 1 FROM hcp_inspections i WHERE i.hcp_customer_id = hc.hcp_customer_id AND i.record_status = 'active')
         AND (EXISTS (SELECT 1 FROM hcp_estimates e WHERE e.hcp_customer_id = hc.hcp_customer_id AND e.sent_at IS NOT NULL)
           OR EXISTS (SELECT 1 FROM hcp_invoices inv WHERE inv.hcp_customer_id = hc.hcp_customer_id AND inv.invoice_type = 'inspection' AND inv.status != 'canceled'))
         THEN CURRENT_TIMESTAMP ELSE NULL END
     ) IS NOT NULL THEN 'inspection_scheduled'
     WHEN callrail_id IS NOT NULL THEN 'lead'
     ELSE 'unknown'
   END AS current_stage
  FROM hcp_customers hc
  WHERE callrail_id IS NOT NULL;
