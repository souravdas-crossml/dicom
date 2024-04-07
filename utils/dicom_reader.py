"""

"""

# Import dependencies
import boto3
import io
import pydicom
import psycopg2
import matplotlib.pyplot as plt

class DicomReader:
    """
    A class to read DICOM files from an S3 bucket and display them as plots.

    Args:
        s3_bucket (str): The name of the S3 bucket.
        s3_object_key (str): The key of the DICOM file in the S3 bucket.

    Attributes:
        s3_bucket (str): The name of the S3 bucket.
        s3_object_key (str): The key of the DICOM file in the S3 bucket.
    """
    def __init__(self, s3_bucket: str, s3_object_key: str) -> None:
        self.s3_bucket = s3_bucket
        self.s3_object_key = s3_object_key
        self.s3_client = boto3.client('s3')

    def read_dicom_from_s3(self) -> pydicom.dataset.FileDataset:
        """
        Reads the DICOM file from S3.

        Returns:
            pydicom.dataset.FileDataset: The DICOM dataset.
        """
        try:
            # Get DICOM file from S3
            response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=self.s3_object_key)
            dicom_bytes = response['Body'].read()
            dicom_dataset = pydicom.dcmread(io.BytesIO(dicom_bytes))
            return dicom_dataset
        except Exception as e:
            print(f"Error reading DICOM file from S3: {e}")

    def show_dicom_plot(self) -> None:
        """
        Displays the DICOM image as a plot.
        """
        dicom_dataset = self.read_dicom_from_s3()
        if dicom_dataset:
            plt.imshow(dicom_dataset.pixel_array, cmap=plt.cm.bone)
            plt.title("DICOM Image")
            plt.axis('off')
            plt.show()


class RdsDataFetcher:
    """
    A class to fetch data from an RDS instance with dynamic filtering, sorting, and pagination.

    Args:
        db_name (str): The name of the database.
        db_user (str): The username to connect to the database.
        db_password (str): The password to connect to the database.
        db_host (str): The hostname of the database.
        db_port (str): The port of the database.

    """
    def __init__(self, db_name: str, db_user: str, db_password: str, db_host: str, db_port: str) -> None:
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.db_host = db_host
        self.db_port = db_port

    def build_query(self, base_query: str, filters: dict = None, sort_by: str = None, sort_order: str = 'asc', page: int = 1, page_size: int = 10) -> str:
        """
        Builds the SQL query based on the provided parameters.

        Args:
            base_query (str): The base SQL query to build upon.
            filters (dict): A dictionary containing filter conditions.
            sort_by (str): The column to sort by.
            sort_order (str): The sort order ('asc' or 'desc').
            page (int): The page number for pagination (1-based index).
            page_size (int): The number of records per page.

        Returns:
            str: The constructed SQL query.
        """
        query = base_query

        # Build SQL query with filters
        if filters:
            filter_conditions = []
            for key, value in filters.items():
                filter_conditions.append(f"{key} = %s")
            filter_clause = " AND ".join(filter_conditions)
            query += f" WHERE {filter_clause}"

        # Add sorting
        if sort_by:
            query += f" ORDER BY {sort_by} {sort_order}"

        # Add pagination
        if page and page_size:
            offset = (page - 1) * page_size
            query += f" OFFSET {offset} LIMIT {page_size}"

        return query

    def fetch_data(self, query: str, filter_values: list = None) -> list:
        """
        Fetches data from the RDS instance based on the provided query.

        Args:
            query (str): The SQL query to fetch data from the database.
            filter_values (list): A list containing values for filter conditions.

        Returns:
            list: A list of tuples containing the fetched data.
        """
        try:
            # Connect to RDS instance
            conn = psycopg2.connect(database=self.db_name, user=self.db_user, password=self.db_password, host=self.db_host, port=self.db_port)
            cursor = conn.cursor()

            # Execute SQL query
            cursor.execute(query, filter_values)
            data = cursor.fetchall()

            return data
        except Exception as e:
            print(f"Error fetching data from RDS: {e}")
        finally:
            # Close cursor and connection
            cursor.close()
            conn.close()

# Example usage:
# Initialize RdsDataFetcher with RDS credentials
db_name = "your_db"
db_user = "your_user"
db_password = "your_password"
db_host = "your_host"
db_port = "your_port"
fetcher = RdsDataFetcher(db_name, db_user, db_password, db_host, db_port)

# Example base query
base_query = "SELECT * FROM your_table"

# Example filters
filters = {"column1": "value1", "column2": "value2"}

# Example sorting
sort_by = "column3"
sort_order = "desc"

# Example pagination
page = 1
page_size = 10

# Build the query
query = fetcher.build_query(base_query, filters=filters, sort_by=sort_by, sort_order=sort_order, page=page, page_size=page_size)

# Fetch data from RDS
filter_values = list(filters.values()) if filters else None
data = fetcher.fetch_data(query, filter_values)
print(data)
