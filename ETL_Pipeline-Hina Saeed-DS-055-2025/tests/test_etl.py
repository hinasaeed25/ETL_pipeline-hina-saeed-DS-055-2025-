import unittest
import pandas as pd
from etl_pipeline import clean_data, standardize_timestamps, feature_engineering

class TestETLPipeline(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame({
            'Formatted Date': ['2006-04-01 00:00:00.000 +0200'],
            'Temperature (C)': [9.47],
            'Humidity': [0.89],
            'Wind Speed (km/h)': [14.12],
            'Pressure (millibars)': [1015.13]
        })

    def test_clean_data(self):
        cleaned_df = clean_data(self.df)
        self.assertFalse(cleaned_df.isnull().values.any())
        self.assertEqual(len(cleaned_df), 1)

    def test_standardize_timestamps(self):
        timestamp_df = standardize_timestamps(self.df)
        self.assertEqual(timestamp_df['timestamp_utc'].iloc[0], '2006-03-31T22:00:00Z')

    def test_feature_engineering(self):
        enriched_df = feature_engineering(self.df)
        expected_score = 9.47 * 0.4 + 0.89 * 0.3 + 14.12 * 0.3
        self.assertAlmostEqual(enriched_df['weather_impact_score'].iloc[0], expected_score, places=2)

if __name__ == '__main__':
    unittest.main()
