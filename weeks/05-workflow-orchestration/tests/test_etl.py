"""Small integration fixtures; run only against a disposable week5_review DB."""
import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

import pandas as pd
from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ingest_months import load_month
from transform_month import transform_file
from load_month import load_file
from etl_month import run
from validate_month import validate_month
import check_source


class ETLTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.engine = create_engine(os.environ['TEST_DATABASE_URL'])
        if cls.engine.url.database != 'week5_review':
            raise RuntimeError('Tests require a disposable database named week5_review.')

    @classmethod
    def tearDownClass(cls):
        cls.engine.dispose()

    def setUp(self):
        with self.engine.begin() as conn:
            conn.execute(text('DROP TABLE IF EXISTS public.taxi_trips_etl'))
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.source = Path(self.directory.name) / 'source.parquet'
        self.csv = Path(self.directory.name) / 'transformed.csv'
        pickup = pd.Timestamp('2024-01-01 12:00')
        frame = pd.DataFrame({
            'tpep_pickup_datetime': [pickup] * 5,
            'tpep_dropoff_datetime': pd.to_datetime(['2024-01-01 12:10', '2024-01-01 12:20', '2024-01-01 11:59', None, '2024-01-01 12:10']),
            'trip_distance': [1.0, 2.0, 0.5, None, 1.0],
            'PULocationID': pd.array([1, 2, 3, None, 1], dtype='Int64'),
            'DOLocationID': pd.array([2, 3, 4, None, 2], dtype='Int64'),
            'fare_amount': [10.0, 20.0, -1.0, None, 10.0],
        })
        frame.to_parquet(self.source, index=False)
        transform_file(self.source, self.csv)

    def count(self):
        with self.engine.connect() as conn:
            return conn.scalar(text('SELECT COUNT(*) FROM public.taxi_trips_etl'))

    def test_categories_nulls_and_deduplication(self):
        self.assertEqual(pd.read_csv(self.csv).trip_category.tolist(), ['short', 'long', 'invalid', 'unknown', 'short'])
        self.assertEqual(load_file(self.csv, 2024, 1, self.engine), 4)
        self.assertEqual(load_file(self.csv, 2024, 1, self.engine), 0)
        self.assertEqual(self.count(), 4)
        validate_month(2024, 1, self.engine)
        with self.assertRaises(ValueError):
            validate_month(2024, 2, self.engine)

    def test_single_task_and_split_task_share_hashes(self):
        run(self.source, 2024, 1, self.engine)
        self.assertEqual(load_file(self.csv, 2024, 1, self.engine), 0)
        self.assertEqual(self.count(), 4)

    def test_failed_later_batch_rolls_back_earlier_inserts(self):
        load_file(self.csv, 2024, 1, self.engine)
        # Fail after a full successful insert batch, not before the transaction.
        frame = pd.read_csv(self.csv).iloc[[0]].copy()
        frame = pd.concat([frame] * 1001, ignore_index=True)
        frame['fare_amount_usd'] = range(100, 1101)
        frame.loc[1000, 'trip_category'] = None
        frame.to_csv(self.csv, index=False)
        with self.assertRaises(Exception):
            load_file(self.csv, 2024, 2, self.engine)
        self.assertEqual(self.count(), 4)

    def test_legacy_rows_are_rejected(self):
        load_file(self.csv, 2024, 1, self.engine)
        with self.engine.begin() as conn:
            conn.execute(text('UPDATE public.taxi_trips_etl SET row_hash = NULL'))
        with self.assertRaisesRegex(ValueError, 'no hash'):
            load_file(self.csv, 2024, 1, self.engine)
        self.assertEqual(self.count(), 4)

    def test_unexpected_category_fails_validation(self):
        load_file(self.csv, 2024, 1, self.engine)
        with self.engine.begin() as conn:
            conn.execute(text("UPDATE public.taxi_trips_etl SET trip_category = 'wrong'"))
        with self.assertRaisesRegex(ValueError, 'categories'):
            validate_month(2024, 1, self.engine)

    def test_monthly_loader_replaces_only_requested_month(self):
        frame = pd.read_parquet(self.source)
        frame['VendorID'] = 1
        frame['passenger_count'] = 1
        frame['total_amount'] = frame['fare_amount']
        frame.to_parquet(self.source, index=False)
        with self.engine.begin() as conn:
            conn.execute(text('DROP TABLE IF EXISTS public.taxi_trips_monthly'))
        for month in (1, 2, 1):
            self.assertEqual(load_month(self.source, 2024, month, self.engine), 5)
        with self.engine.connect() as conn:
            self.assertEqual(conn.scalar(text('SELECT COUNT(*) FROM public.taxi_trips_monthly')), 10)

    def test_sample_setup_is_repeatable(self):
        script = (Path(__file__).resolve().parents[1] / 'sql/prepare-trip-sample.sql').read_text()
        for _ in range(2):
            # This teaching file includes its own BEGIN and COMMIT.
            with self.engine.connect().execution_options(isolation_level='AUTOCOMMIT') as conn:
                conn.exec_driver_sql(script)
                self.assertEqual(conn.scalar(text('SELECT COUNT(*) FROM public.taxi_trip_sample')), 100)

    def test_unavailable_source_has_clear_failure(self):
        with patch.object(sys, 'argv', ['check_source.py', '--year', '2024', '--month', '1']):
            with patch.object(check_source, 'urlopen', side_effect=HTTPError('test', 404, 'missing', {}, None)):
                with self.assertRaisesRegex(SystemExit, 'HTTP 404'):
                    check_source.main()


if __name__ == '__main__':
    unittest.main()
