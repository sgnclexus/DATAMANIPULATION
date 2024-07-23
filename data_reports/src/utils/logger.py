import os
from datetime import datetime

class Logger:

    def __init__(self, default_event_path='logs', default_error_path='logs'):
        self.default_event_path = default_event_path
        self.default_error_path = default_error_path
        os.makedirs(self.default_event_path, exist_ok=True)
        os.makedirs(self.default_error_path, exist_ok=True)

    def log_event(self, message, file_path=None):
        if file_path is None:
            file_path = self.default_event_path
        file_name = os.path.join(file_path, 'event.log')
        self._write_log(file_name, message)
        # For testing 
        # print(message)

    def log_error(self, message, file_path=None):
        if file_path is None:
            file_path = self.default_error_path
        file_name = os.path.join(file_path, 'error.log')
        self._write_log(file_name, message)

    def _write_log(self, file_name, message):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with open(file_name, 'a', encoding="utf-8") as file:
            file.write(f"{timestamp} - {message}\n")