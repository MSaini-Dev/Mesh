import os
import sys
import time

LOG_FILE = "meshos.log"

class MeshLogger:
    def __init__(self, filename: str = LOG_FILE):
        self.filename = filename

    def log(self, msg: str):
        t = time.strftime("%H:%M:%S")
        formatted = f"[{t}] [MeshOS] {msg}"
        # Print to console
        print(formatted, flush=True)
        # Write to log file
        try:
            with open(self.filename, "a", encoding="utf-8") as f:
                f.write(formatted + "\n")
        except Exception:
            pass

_logger_instance = MeshLogger()

def setup_logger(filename: str = LOG_FILE):
    global _logger_instance
    _logger_instance = MeshLogger(filename)

def log(msg: str):
    _logger_instance.log(msg)
