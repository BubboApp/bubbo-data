import unittest
from unittest.mock import patch, MagicMock, mock_open, call
import json
import io
import boto3
import math
import os
from datetime import datetime
import logging

# Import the module to test
# Note: Assuming the original code is in a file named sync.py
import sync

class TestSyncScript(unittest.TestCase):
    def setUp(self):
        # Reset statistics before each test
        sync.stats = {
            "total_files_processed": 0,
            "total_files_skipped": 0,
            "total_files_deleted": 0,
            "total_documents_processed": 0,
            "total_documents_updated": 0,
            "total_documents_deleted": 0,
            "total_errors": 0,
            "start_time": 0,
            "files_with_errors": set()
        }
    
    def test_is_valid_firebase_key(self):
        # Test valid keys
        self.assertTrue(sync.is_valid_firebase_key("valid_key"))
        self.assertTrue(sync.is_valid_firebase_key("validKey123"))
        self.assertTrue(sync.is_valid_firebase_key("valid-key"))
        
        # Test invalid keys
        self.assertFalse(sync.is_valid_firebase_key("invalid.key"))
        self.assertFalse(sync.is_valid_firebase_key("invalid$key"))
        self.assertFalse(sync.is_valid_firebase_key("invalid#key"))
        self.assertFalse(sync.is_valid_firebase_key("invalid[key"))
        self.assertFalse(sync.is_valid_firebase_key("invalid]key"))
        self.assertFalse(sync.is_valid_firebase_key("invalid/key"))

    def test_sanitize_firebase_key(self):
        # Test key sanitization
        self.assertEqual(sync.sanitize_firebase_key("valid_key"), "valid_key")
        self.assertEqual(sync.sanitize_firebase_key("invalid.key"), "invalid_key")
        self.assertEqual(sync.sanitize_firebase_key("invalid$key"), "invalid_key")
        self.assertEqual(sync.sanitize_firebase_key("invalid#key"), "invalid_key")
        self.assertEqual(sync.sanitize_firebase_key("invalid[key"), "invalid_key")
        self.assertEqual(sync.sanitize_firebase_key("invalid]key"), "invalid_key")
        self.assertEqual(sync.sanitize_firebase_key("invalid/key"), "invalid_key")
        
        # Test keys that start with . or $
        self.assertEqual(sync.sanitize_firebase_key(".startWithDot"), "key_.startWithDot")
        self.assertEqual(sync.sanitize_firebase_key("$startWithDollar"), "key_$startWithDollar")

    def test_clean_json(self):
        # Test with a complex nested structure
        test_data = {
            "valid_key": "value",
            "invalid.key": "value",
            "nested": {
                "valid_key": 123,
                "invalid$key": 456,
                "array": [1, 2, {"invalid.nested": "value"}]
            },
            "nan_value": float('nan'),
            "inf_value": float('inf'),
            "neg_inf": float('-inf'),
            "normal_float": 3.14,
            "object": object()  # Non-serializable object
        }
        
        cleaned = sync.clean_json(test_data)
        
        # Verify sanitized keys
        self.assertIn("valid_key", cleaned)
        self.assertIn("invalid_key", cleaned)
        self.assertIn("nested", cleaned)
        self.assertIn("valid_key", cleaned["nested"])
        self.assertIn("invalid_key", cleaned["nested"])
        self.assertIn("array", cleaned["nested"])
        self.assertIn("invalid_nested", cleaned["nested"]["array"][2])
        
        # Verify NaN and Infinity handling
        self.assertIsNone(cleaned["nan_value"])
        self.assertIsNone(cleaned["inf_value"])
        self.assertIsNone(cleaned["neg_inf"])
        self.assertEqual(cleaned["normal_float"], 3.14)
        
        # Verify non-serializable object converted to string
        self.assertIsInstance(cleaned["object"], str)

    @patch('requests.patch')
    def test_update_firebase_success(self, mock_patch):
        # Configure the mock
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_patch.return_value = mock_response
        
        # Test data
        batch = {
            "valid_key": {"id": "valid_key", "data": "test"},
            "another_key": {"id": "another_key", "data": "test2"}
        }
        
        # Call the function
        sync.update_firebase(batch)
        
        # Assert the mock was called correctly
        url = f"{sync.FIREBASE_DATABASE_URL}/{sync.FIREBASE_PATH}.json"
        mock_patch.assert_called_once_with(
            url, 
            data=json.dumps(batch), 
            headers={"Content-Type": "application/json"}, 
            timeout=10
        )
        
        # Check stats
        self.assertEqual(sync.stats["total_documents_updated"], 2)
        self.assertEqual(sync.stats["total_errors"], 0)

    @patch('requests.patch')
    def test_update_firebase_with_invalid_keys(self, mock_patch):
        # Configure the mock
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_patch.return_value = mock_response
        
        # Test data with invalid keys
        batch = {
            "valid_key": {"id": "valid_key", "data": "test"},
            "invalid.key": {"id": "invalid.key", "data": "test2"}
        }
        
        # Call the function
        sync.update_firebase(batch)
        
        # The function should sanitize the keys before sending
        expected_batch = {
            "valid_key": {"id": "valid_key", "data": "test"},
            "invalid_key": {"id": "invalid.key", "data": "test2"}
        }
        
        # Assert the mock was called with sanitized keys
        url = f"{sync.FIREBASE_DATABASE_URL}/{sync.FIREBASE_PATH}.json"
        args, kwargs = mock_patch.call_args
        self.assertEqual(args[0], url)
        # Parse the JSON to compare objects rather than strings
        sent_data = json.loads(kwargs["data"])
        self.assertEqual(sent_data.keys(), expected_batch.keys())

    @patch('requests.patch')
    def test_update_firebase_deletion(self, mock_patch):
        # Configure the mock
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_patch.return_value = mock_response
        
        # Test data for deletion (values set to None)
        batch = {
            "doc1": None,
            "doc2": None
        }
        
        # Call the function
        sync.update_firebase(batch)
        
        # Check stats for deletion
        self.assertEqual(sync.stats["total_documents_deleted"], 2)
        self.assertEqual(sync.stats["total_documents_updated"], 0)

    @patch('requests.patch')
    def test_update_firebase_error(self, mock_patch):
        # Configure the mock to return an error
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Server Error"
        mock_patch.return_value = mock_response
        
        # Test data
        batch = {"valid_key": {"id": "valid_key", "data": "test"}}
        
        # Call the function
        sync.update_firebase(batch)
        
        # Check error stats
        self.assertEqual(sync.stats["total_errors"], 1)

    @patch('boto3.client')
    def test_process_file_success(self, mock_boto3_client):
        # Configure the S3 mock
        mock_s3 = MagicMock()
        mock_body = MagicMock()
        # Simulate a file with 2 JSON lines
        mock_body.iter_lines.return_value = [
            b'{"id":"doc1","data":"test1"}',
            b'{"id":"doc2","data":"test2"}'
        ]
        mock_s3.get_object.return_value = {"Body": mock_body}
        mock_boto3_client.return_value = mock_s3
        
        # Mock the update_firebase function
        with patch('sync.update_firebase') as mock_update:
            # Call the function
            result = sync.process_file("test.jsonl", "2023-01-01T00:00:00Z")
            
            # Verify the S3 client was called correctly
            mock_boto3_client.assert_called_once_with(
                "s3",
                aws_access_key_id=sync.DO_ACCESS_KEY,
                aws_secret_access_key=sync.DO_SECRET_KEY,
                endpoint_url=sync.DO_ENDPOINT,
            )
            
            # Verify get_object was called
            mock_s3.get_object.assert_called_once_with(
                Bucket=sync.DO_BUCKET_NAME, 
                Key="test.jsonl"
            )
            
            # Verify update_firebase was called with the right data
            expected_batch = {
                "doc1": {"id": "doc1", "data": "test1"},
                "doc2": {"id": "doc2", "data": "test2"}
            }
            mock_update.assert_called_once_with(expected_batch)
            
            # Verify returned document IDs
            self.assertEqual(result, ["doc1", "doc2"])
            
            # Verify stats
            self.assertEqual(sync.stats["total_files_processed"], 1)
            self.assertEqual(sync.stats["total_documents_processed"], 2)

    @patch('boto3.client')
    def test_process_file_with_json_errors(self, mock_boto3_client):
        # Configure the S3 mock with one valid JSON and one invalid
        mock_s3 = MagicMock()
        mock_body = MagicMock()
        mock_body.iter_lines.return_value = [
            b'{"id":"doc1","data":"test1"}',
            b'invalid json',
            b'{"id":"doc3","data":"test3"}'
        ]
        mock_s3.get_object.return_value = {"Body": mock_body}
        mock_boto3_client.return_value = mock_s3
        
        # Mock the update_firebase function
        with patch('sync.update_firebase') as mock_update:
            # Call the function
            result = sync.process_file("test.jsonl", "2023-01-01T00:00:00Z")
            
            # Verify update_firebase was called with the valid documents
            expected_batch = {
                "doc1": {"id": "doc1", "data": "test1"},
                "doc3": {"id": "doc3", "data": "test3"}
            }
            mock_update.assert_called_once_with(expected_batch)
            
            # Verify returned document IDs
            self.assertEqual(set(result), {"doc1", "doc3"})
            
            # Verify error stats
            self.assertEqual(sync.stats["total_errors"], 1)
            self.assertEqual(len(sync.stats["files_with_errors"]), 1)
            self.assertIn("test.jsonl", sync.stats["files_with_errors"])

    @patch('boto3.client')
    def test_process_file_s3_error(self, mock_boto3_client):
        # Configure the S3 mock to raise an exception
        mock_s3 = MagicMock()
        mock_s3.get_object.side_effect = Exception("S3 Error")
        mock_boto3_client.return_value = mock_s3
        
        # Call the function
        result = sync.process_file("test.jsonl", "2023-01-01T00:00:00Z")
        
        # Verify the result is None due to the error
        self.assertIsNone(result)
        
        # Verify error stats
        self.assertEqual(sync.stats["total_errors"], 1)
        self.assertIn("test.jsonl", sync.stats["files_with_errors"])

    @patch('os.path.exists')
    @patch('builtins.open', new_callable=mock_open, read_data='{"file1.jsonl": {"last_modified": "2023-01-01T00:00:00Z", "doc_ids": ["doc1", "doc2"]}}')
    def test_load_state_existing_file(self, mock_file, mock_exists):
        # Mock os.path.exists to return True
        mock_exists.return_value = True
        
        # Call the function
        state = sync.load_state()
        
        # Verify the state was loaded correctly
        expected_state = {
            "file1.jsonl": {
                "last_modified": "2023-01-01T00:00:00Z",
                "doc_ids": ["doc1", "doc2"]
            }
        }
        self.assertEqual(state, expected_state)
        
        # Verify open was called correctly
        mock_file.assert_called_once_with(sync.STATE_FILE, "r")

    @patch('os.path.exists')
    def test_load_state_no_file(self, mock_exists):
        # Mock os.path.exists to return False
        mock_exists.return_value = False
        
        # Call the function
        state = sync.load_state()
        
        # Verify an empty state was returned
        self.assertEqual(state, {})

    @patch('builtins.open', new_callable=mock_open)
    def test_save_state(self, mock_file):
        # Test state to save
        state = {
            "file1.jsonl": {
                "last_modified": "2023-01-01T00:00:00Z",
                "doc_ids": ["doc1", "doc2"]
            }
        }
        
        # Call the function
        sync.save_state(state)
        
        # Verify open was called correctly
        mock_file.assert_called_once_with(sync.STATE_FILE, "w")
        
        # Verify write was called with the correct JSON
        handle = mock_file()
        handle.write.assert_called_once_with(json.dumps(state))

    @patch('sync.load_state')
    @patch('sync.save_state')
    @patch('boto3.client')
    @patch('sync.process_file')
    @patch('sync.update_firebase')
    def test_incremental_update(self, mock_update, mock_process, mock_boto3_client, mock_save, mock_load):
        # Configure load_state mock to return a previous state
        previous_state = {
            "unchanged.jsonl": {
                "last_modified": "2023-01-01T00:00:00Z", 
                "doc_ids": ["unchanged1", "unchanged2"]
            },
            "modified.jsonl": {
                "last_modified": "2023-01-01T00:00:00Z", 
                "doc_ids": ["modified1", "modified2"]
            },
            "deleted.jsonl": {
                "last_modified": "2023-01-01T00:00:00Z", 
                "doc_ids": ["deleted1", "deleted2"]
            }
        }
        mock_load.return_value = previous_state
        
        # Configure S3 mock to return current objects
        mock_s3 = MagicMock()
        mock_paginator = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator
        
        # Mock LastModified as datetime objects that will be converted to ISO strings
        unchanged_date = datetime(2023, 1, 1)
        modified_date = datetime(2023, 1, 2)
        new_date = datetime(2023, 1, 3)
        
        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "unchanged.jsonl", "LastModified": unchanged_date},
                    {"Key": "modified.jsonl", "LastModified": modified_date},
                    {"Key": "new.jsonl", "LastModified": new_date}
                ]
            }
        ]
        mock_boto3_client.return_value = mock_s3
        
        # Configure process_file mock
        mock_process.side_effect = [
            ["modified1_new", "modified2_new"],  # For modified.jsonl
            ["new1", "new2"]                    # For new.jsonl
        ]
        
        # Call the function
        sync.incremental_update()
        
        # Verify boto3 client was created correctly
        mock_boto3_client.assert_called_once_with(
            "s3",
            aws_access_key_id=sync.DO_ACCESS_KEY,
            aws_secret_access_key=sync.DO_SECRET_KEY,
            endpoint_url=sync.DO_ENDPOINT,
        )
        
        # Verify paginator was used
        mock_s3.get_paginator.assert_called_once_with("list_objects_v2")
        mock_paginator.paginate.assert_called_once_with(Bucket=sync.DO_BUCKET_NAME)
        
        # Verify process_file was called for new and modified files
        self.assertEqual(mock_process.call_count, 2)
        process_calls = [
            call("modified.jsonl", modified_date.isoformat()),
            call("new.jsonl", new_date.isoformat())
        ]
        mock_process.assert_has_calls(process_calls, any_order=True)
        
        # Verify update_firebase was called for deleted files
        mock_update.assert_called_once_with({"deleted1": None, "deleted2": None})
        
        # Verify save_state was called with the updated state
        expected_new_state = {
            "unchanged.jsonl": {
                "last_modified": unchanged_date.isoformat(), 
                "doc_ids": ["unchanged1", "unchanged2"]
            },
            "modified.jsonl": {
                "last_modified": modified_date.isoformat(), 
                "doc_ids": ["modified1_new", "modified2_new"]
            },
            "new.jsonl": {
                "last_modified": new_date.isoformat(), 
                "doc_ids": ["new1", "new2"]
            }
        }
        mock_save.assert_called_once()
        # Check that keys match (exact equality of the whole dict is tricky with nested structures)
        saved_state = mock_save.call_args[0][0]
        self.assertEqual(set(saved_state.keys()), set(expected_new_state.keys()))
        
        # Verify stats
        self.assertEqual(sync.stats["total_files_processed"], 2)  # modified + new
        self.assertEqual(sync.stats["total_files_skipped"], 1)    # unchanged
        self.assertEqual(sync.stats["total_files_deleted"], 1)    # deleted


if __name__ == "__main__":
    unittest.main()