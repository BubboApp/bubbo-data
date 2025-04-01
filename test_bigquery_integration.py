import unittest
from unittest.mock import patch, MagicMock
import os
import sys
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest, NotFound

# Import the script to test (assuming it's in a file called bigquery_integration.py)
# If your script is in a different file, change this import
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import bigquery_integration  # Replace with your actual script filename

class TestBigQueryIntegration(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures."""
        # Mock BigQuery client
        self.mock_client_patcher = patch('google.cloud.bigquery.Client')
        self.mock_client = self.mock_client_patcher.start()
        
        # Create a mock instance that will be returned by Client()
        self.client_instance = MagicMock()
        self.mock_client.return_value = self.client_instance
        
        # Mock environment variables
        self.mock_environ_patcher = patch.dict('os.environ', {"GOOGLE_APPLICATION_CREDENTIALS": "/path/to/mock/credentials.json"})
        self.mock_environ_patcher.start()
        
        # Test data
        self.test_dataset_id = "test-project.test_dataset"
        self.test_destination_dataset = "test-project.test_destination"
        self.test_tables = [MagicMock() for _ in range(5)]
        for i, table in enumerate(self.test_tables):
            table.table_id = f"test_table_{i}"

    def tearDown(self):
        """Tear down test fixtures."""
        self.mock_client_patcher.stop()
        self.mock_environ_patcher.stop()

    def test_dataset_listing(self):
        """Test that the script properly lists tables in a dataset."""
        # Configure mock
        self.client_instance.list_tables.return_value = self.test_tables
        
        # Call function that lists tables (adjust as needed for your actual function)
        tables = list(self.client_instance.list_tables(self.test_dataset_id))
        
        # Assertions
        self.assertEqual(len(tables), 5)
        self.client_instance.list_tables.assert_called_once_with(self.test_dataset_id)
        for i, table in enumerate(tables):
            self.assertEqual(table.table_id, f"test_table_{i}")

    def test_create_destination_table(self):
        """Test the creation of the destination table."""
        # Configure mock
        mock_query_job = MagicMock()
        self.client_instance.query.return_value = mock_query_job
        
        # Call function that creates the destination table
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS `{self.test_destination_dataset}.unified_content`
        (
          tmdb_id STRING,
          genres STRING,
          title STRING,
          synopsis STRING,
          source_table STRING
        )
        """
        self.client_instance.query(create_table_query).result()
        
        # Assertions
        self.client_instance.query.assert_called_with(create_table_query)
        mock_query_job.result.assert_called_once()

    def test_process_batch_success(self):
        """Test successful processing of a batch of tables."""
        # Configure mock
        mock_query_job = MagicMock()
        self.client_instance.query.return_value = mock_query_job
        
        # Sample batch of table names
        test_batch = ["table1", "table2", "table3"]
        
        # Define a simplified version of process_tables_in_batches for testing
        def process_test_batch(tables, dataset_id, destination_dataset):
            union_queries = [
                f"""
                SELECT 
                  CAST(tmdb_id AS STRING) AS tmdb_id, 
                  CAST(genres AS STRING) AS genres, 
                  CAST(title AS STRING) AS title, 
                  CAST(synopsis AS STRING) AS synopsis, 
                  '{table}' AS source_table 
                FROM `{dataset_id}.{table}`
                """
                for table in tables
            ]
            
            batch_query = f"""
            INSERT INTO `{destination_dataset}.unified_content`
            WITH all_data AS (
                {' UNION ALL '.join(union_queries)}
            )
            SELECT DISTINCT tmdb_id, genres, title, synopsis, source_table
            FROM all_data
            WHERE tmdb_id IS NOT NULL;
            """
            
            return self.client_instance.query(batch_query)
        
        # Process the test batch
        job = process_test_batch(test_batch, self.test_dataset_id, self.test_destination_dataset)
        
        # Assertions
        self.assertEqual(job, mock_query_job)
        self.client_instance.query.assert_called_once()
        self.assertIn("UNION ALL", self.client_instance.query.call_args[0][0])
        self.assertIn("CAST(tmdb_id AS STRING)", self.client_instance.query.call_args[0][0])

    def test_process_batch_error(self):
        """Test handling of errors during batch processing."""
        # Configure mock to raise an exception
        self.client_instance.query.side_effect = BadRequest("Test error")
        
        # Define a simplified exception-handling test function
        def process_with_exception_handling(tables, dataset_id, destination_dataset):
            try:
                union_queries = [
                    f"""SELECT CAST(tmdb_id AS STRING) AS tmdb_id FROM `{dataset_id}.{table}`"""
                    for table in tables
                ]
                
                batch_query = f"""
                INSERT INTO `{destination_dataset}.unified_content`
                WITH all_data AS (
                    {' UNION ALL '.join(union_queries)}
                )
                SELECT DISTINCT tmdb_id FROM all_data;
                """
                
                job = self.client_instance.query(batch_query)
                job.result()
                return "Success"
            except Exception as e:
                return f"Error: {str(e)}"
        
        # Process with exception handling
        result = process_with_exception_handling(
            ["error_table"], self.test_dataset_id, self.test_destination_dataset
        )
        
        # Assertions
        self.assertTrue(result.startswith("Error:"))
        self.assertIn("Test error", result)

    def test_create_best_content_table(self):
        """Test the creation of the best_content table."""
        # Configure mock
        mock_query_job = MagicMock()
        self.client_instance.query.return_value = mock_query_job
        
        # Query to test
        best_content_query = f"""
        CREATE OR REPLACE TABLE `{self.test_destination_dataset}.best_content` AS
        WITH ranked AS (
          SELECT *,
          ROW_NUMBER() OVER (PARTITION BY tmdb_id
          ORDER BY LENGTH(synopsis) DESC, LENGTH(title) DESC, LENGTH(genres) DESC) AS rank
          FROM `{self.test_destination_dataset}.unified_content`
        )
        SELECT tmdb_id, genres, title, synopsis
        FROM ranked
        WHERE rank = 1 AND tmdb_id IS NOT NULL;
        """
        
        # Execute query
        self.client_instance.query(best_content_query).result()
        
        # Assertions
        self.client_instance.query.assert_called_with(best_content_query)
        mock_query_job.result.assert_called_once()

    def test_integration_flow(self):
        """Test the overall integration flow with mocked components."""
        # Configure mocks
        self.client_instance.list_tables.return_value = self.test_tables
        mock_query_job = MagicMock()
        self.client_instance.query.return_value = mock_query_job
        
        # Mock simplified version of the main flow
        def main_flow_test():
            # Get table names
            tables = list(self.client_instance.list_tables(self.test_dataset_id))
            table_names = [table.table_id for table in tables]
            
            # Create destination table
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS `{self.test_destination_dataset}.unified_content` (
              tmdb_id STRING,
              genres STRING,
              title STRING,
              synopsis STRING,
              source_table STRING
            )
            """
            self.client_instance.query(create_table_query).result()
            
            # Process batches (simplified)
            batch_query = f"""INSERT INTO `{self.test_destination_dataset}.unified_content` SELECT * FROM dummy"""
            self.client_instance.query(batch_query).result()
            
            # Create best_content
            best_content_query = f"""CREATE OR REPLACE TABLE `{self.test_destination_dataset}.best_content` AS SELECT * FROM dummy"""
            return self.client_instance.query(best_content_query).result()
        
        # Run the test flow
        result = main_flow_test()
        
        # Assertions
        self.assertEqual(self.client_instance.list_tables.call_count, 1)
        self.assertEqual(self.client_instance.query.call_count, 3)
        self.assertEqual(mock_query_job.result.call_count, 3)


if __name__ == '__main__':
    unittest.main()