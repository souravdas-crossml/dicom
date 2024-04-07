"""
dicom_saver.py

A module to handle saving DICOM files and metadata to RDS and S3.

This module defines a class DicomSaver that provides methods to save DICOM files and metadata
to an RDS instance and an S3 bucket.

Example usage:

    # Initialize DicomSaver with RDS instance and S3 bucket name
    rds_instance = "your-rds-instance"
    s3_bucket = "your-s3-bucket"
    saver = DicomSaver(rds_instance, s3_bucket)

    # Read DICOM file as bytes
    with open("example.dcm", "rb") as f:
        dicom_bytes = f.read()

    # Process and save DICOM file
    saver.process_and_save(dicom_bytes, "example.dcm")
"""

import boto3
import io
import pydicom
import json
import psycopg2

class DicomSaver:
    """
    A class to handle saving DICOM files and metadata to RDS and S3.

    Args:
        rds_instance (str): The name of the RDS instance.
        s3_bucket (str): The name of the S3 bucket.

    Attributes:
        rds_instance (str): The name of the RDS instance.
        s3_bucket (str): The name of the S3 bucket.
        rds_client (boto3.client): The RDS client.
        s3_client (boto3.client): The S3 client.
    """
    def __init__(self, rds_instance: str, s3_bucket: str) -> None:
        self.rds_instance = rds_instance
        self.s3_bucket = s3_bucket
        self.rds_client = boto3.client('rds')
        self.s3_client = boto3.client('s3')

    def save_to_rds(self, dicom_metadata: dict) -> None:
        """
        Connects to the RDS instance and saves DICOM metadata.

        Args:
            dicom_metadata (dict): The DICOM metadata to be saved.
        """
        # Connect to RDS instance
        try:
            # Example: Connect to RDS
            conn = psycopg2.connect(
                database="your_db", 
                user="your_user", 
                password="your_password", 
                host="your_host", 
                port="your_port"
            )
            cursor = conn.cursor()
            
            # Execute SQL query to save metadata
            cursor.execute("INSERT INTO dicom_metadata_table (metadata) VALUES (%s)", (json.dumps(dicom_metadata),))
            
            # Commit the transaction
            conn.commit()
            
            print("DICOM metadata saved to RDS successfully.")
        except Exception as e:
            # Rollback in case of error
            conn.rollback()
            print(f"Error saving DICOM metadata to RDS: {e}")
        finally:
            # Close cursor and connection
            cursor.close()
            conn.close()


    def save_to_s3(self, dicom_bytes: bytes, filename: str) -> None:
        """
        Uploads DICOM bytes to the specified S3 bucket.

        Args:
            dicom_bytes (bytes): The DICOM file content as bytes.
            filename (str): The filename to be used in S3.
        """
        s3_client = boto3.client('s3')
        # Upload DICOM bytes to S3 bucket
        s3_client.put_object(Bucket=self.s3_bucket, Key=filename, Body=dicom_bytes)

    def process_and_save(self, dicom_bytes: bytes, filename: str) -> None:
        """
        Extracts DICOM metadata, saves it to RDS, and saves the DICOM file to S3.

        Args:
            dicom_bytes (bytes): The DICOM file content as bytes.
            filename (str): The filename to be used in S3.
        """
        # Extract DICOM metadata
        dicom_data = pydicom.dcmread(io.BytesIO(dicom_bytes))
        metadata = self.extract_dicom_metadata(dicom_data)
        
        # Save metadata to RDS
        self.save_to_rds(metadata)
        
        # Save DICOM file to S3
        self.save_to_s3(dicom_bytes, filename)

    def extract_dicom_metadata(self, dicom_data: pydicom.dataset.FileDataset) -> dict:
        """
        Extracts DICOM metadata from the DICOM dataset.

        Args:
            dicom_data (pydicom.dataset.FileDataset): The DICOM dataset.

        Returns:
            dict: The extracted DICOM metadata.
        """
        flat_dicom = {}
        for element in dicom_data:
            if element.VR == "SQ":  # Handle sequences separately
                sequence_data = []
                for seq_item in element.value:
                    sequence_data.append(self.flatten_dicom(seq_item))
                flat_dicom[element.keyword] = sequence_data
            else:
                flat_dicom[element.keyword] = str(element.value)
        return flat_dicom

    def flatten_dicom(self, dicom_data: pydicom.dataset.FileDataset) -> dict:
        """
        Flattens the DICOM dataset into a dictionary.

        Args:
            dicom_data (pydicom.dataset.FileDataset): The DICOM dataset.

        Returns:
            dict: The flattened DICOM metadata.
        """
        flat_dicom = {}
        for element in dicom_data:
            if element.VR == "SQ":  # Handle sequences separately
                sequence_data = []
                for seq_item in element.value:
                    sequence_data.append(self.flatten_dicom(seq_item))
                flat_dicom[element.keyword] = sequence_data
            else:
                flat_dicom[element.keyword] = str(element.value)
        return flat_dicom

