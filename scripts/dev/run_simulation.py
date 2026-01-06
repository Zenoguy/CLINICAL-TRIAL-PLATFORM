from storage.supabase_writer import insert_rows

insert_rows(
    table_name="cpid_metric_snapshots",
    rows=[
        {
            "entity_type": "subject",
            "entity_id": "TEST_SUBJECT",
            "site_id": "TEST_SITE",
            "metric_name": "test__metric",
            "metric_value": 1.0,
            "snapshot_time": "2026-01-01T00:00:00+00:00",
            "source": "TEST",
        }
    ],
)
