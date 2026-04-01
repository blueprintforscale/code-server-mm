-- Backfill 30 days of risk status snapshots
DO $$
DECLARE
  d INT;
  snap_date DATE;
  p_start DATE;
  p_end DATE;
BEGIN
  FOR d IN 0..30 LOOP
    snap_date := CURRENT_DATE - d;
    p_end := snap_date;
    p_start := snap_date - 30;
    INSERT INTO risk_status_snapshots (customer_id, snapshot_date, status, risk_type, risk_triggers, flag_triggers, flag_count)
    SELECT customer_id, snap_date, status, risk_type, risk_triggers, flag_triggers, flag_count
    FROM get_dashboard_with_risk(p_start, p_end)
    ON CONFLICT (customer_id, snapshot_date) DO NOTHING;
  END LOOP;
END $$;
