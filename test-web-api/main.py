import argparse
import logging
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import requests

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)

logging.basicConfig(
    format='%(levelname)s | %(asctime)s | %(message)s', level=logging.INFO,
    handlers=[logging.FileHandler('load_test.log', mode='w'), stream_handler]
)


class Tester:

    def __init__(self):
        self.args = None
        self.time_elapsed = 0
        self.collection_name = uuid.uuid4().hex
        self.headers = {'Content-Type': 'application/json', 'grpc-metadata-sessionid': None}
        self.lock = threading.Lock()
        self.transactions = 0

    def configure_arguments(self):
        parser = argparse.ArgumentParser(
            description="Load test tool for immudb web API",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
        parser.add_argument("-d", "--duration", help="Duration of tests execution", default=10, type=int)
        parser.add_argument("-w", "--workers", help="Number of concurrent workers", default=1, type=int)
        parser.add_argument("-u", "--username", help="immudb username", default="immudb")
        parser.add_argument("-p", "--password", help="immudb password", default="immudb")
        parser.add_argument("-db", "--database", help="immudb database", default="defaultdb")
        parser.add_argument("-b", "--base_url", help="Host url for web API", required=True)
        documents_message = "Number of documents to send in a single request"
        parser.add_argument("-s", "--documents_size", help=documents_message, default=1, type=int)
        self.args = parser.parse_args()

    def open_session(self):
        data = {
            "username": self.args.username,
            "password": self.args.password,
            "database": self.args.database
        }
        response = requests.post(
            f"{self.args.base_url}/api/v2/authorization/session/open",
            json=data
        )
        self.headers['grpc-metadata-sessionid'] = response.json()["sessionID"]
        logging.info(f'Started with {self.args.workers} worker(s) and size {self.args.documents_size}')

    def create_collection(self):
        data = {
            "name": self.collection_name,
            "documentIdFieldName": "emp_no",
            "fields": [
                {
                    "name": "birth_date"
                },
                {
                    "name": "first_name"
                },
                {
                    "name": "last_name"
                },
                {
                    "name": "gender"
                },
                {
                    "name": "hire_date"
                }
            ]
        }
        logging.debug(f"Trying to create a collection with {data}")
        response = requests.post(
            f"{self.args.base_url}/api/v2/collection/{self.collection_name}",
            headers=self.headers, json=data
        )
        logging.debug(f"Status code: {response.status_code} | Text: {response.text}")

    def insert_documents(self):
        start_time = time.time()
        threading.Thread(target=self.count_seconds).start()
        while time.time() - start_time < self.args.duration:
            with ThreadPoolExecutor(max_workers=self.args.workers) as executor:
                for worker in range(self.args.workers):
                    executor.submit(self.send_request)

    def count_seconds(self):
        while self.args.duration > self.time_elapsed:
            time.sleep(1)
            self.time_elapsed += 1
            logging.info(
                f"Time elapsed: {self.time_elapsed}s | Time remaining: {self.args.duration - self.time_elapsed}s"
            )

    def send_request(self):
        documents = [
            {
                "birth_date": "1961-04-24",
                "first_name": "Basil",
                "last_name": "Tramer",
                "gender": "F",
                "hire_date": "1992-05-04"
            }
        ]
        data = {
            'documents': documents * self.args.documents_size
        }
        logging.debug(f"Trying to insert document {data} with worker {threading.current_thread().name}")
        response = requests.post(
            f"{self.args.base_url}/api/v2/collection/{self.collection_name}/documents",
            headers=self.headers, json=data
        )
        if response.status_code != 200:
            logging.warning(f"Response was not 200! | Status code: {response.status_code} | Text: {response.text}")
        else:
            logging.debug(f"Status code: {response.status_code} | Text: {response.text}")
            with self.lock:
                self.transactions += 1

    def log_results(self):
        transactions_per_second = round(self.transactions / self.args.duration, 2)
        logging.info(f'TX: {self.transactions} | TX/s {transactions_per_second}')


if __name__ == "__main__":
    tester = Tester()
    tester.configure_arguments()
    tester.open_session()
    tester.create_collection()
    tester.insert_documents()
    tester.log_results()
