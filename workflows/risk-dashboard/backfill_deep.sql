-- Deep backfill: 180 days for clients currently in Risk or Flag
-- Only fills gaps (skips dates already snapshotted)
DO $$
DECLARE
  d INT;
  snap_date DATE;
  p_start DATE;
  p_end DATE;
BEGIN
  FOR d IN 31..180 LOOP
    snap_date := CURRENT_DATE - d;
    p_end := snap_date;
    p_start := snap_date - 30;

    INSERT INTO risk_status_snapshots (customer_id, snapshot_date, status, risk_type, risk_triggers, flag_triggers, flag_count)
    SELECT r.customer_id, snap_date, r.status, r.risk_type, r.risk_triggers, r.flag_triggers, r.flag_count
    FROM get_dashboard_with_risk(p_start, p_end) r
    WHERE r.customer_id IN (
      SELECT customer_id FROM get_dashboard_with_risk() WHERE status IN ('Risk', 'Flag')
    )
    ON CONFLICT (customer_id, snapshot_date) DO NOTHING;

    -- Progress marker every 30 days
    IF d % 30 = 0 THEN
      RAISE NOTICE 'Backfilled to % (day %)', snap_date, d;
    END IF;
  END LOOP;
END $$;
