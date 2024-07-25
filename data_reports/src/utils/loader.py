from itertools import cycle
from shutil import get_terminal_size
from threading import Thread
from time import sleep

OKCYAN = '\033[96m'
WARNING = '\033[93m'
OKGREEN = '\033[92m'
FAIL = '\033[91m'
BOLD = '\033[1m'
NORMAL = '\033[0m'

class Loader:
    def __init__(self, desc="Loading...", end="Done!", timeout=0.2):
        """
        A loader-like context manager

        Args:
            desc (str, optional): The loader's description. Defaults to "Loading...".
            end (str, optional): Final print. Defaults to "Done!".
            timeout (float, optional): Sleep time between prints. Defaults to 0.2.
        """
        self.desc = desc
        self.end = end
        self.timeout = timeout

        self._thread = Thread(target=self._animate, daemon=True)
        self.steps = [" ⢿ ", " ⣻ ", " ⣽ ", " ⣾ ", " ⣷ ", " ⣯ ", " ⣟ ", " ⡿ "]
        self.done = False

    def start(self):
        self._thread.start()
        return self

    def _animate(self):
        for c in cycle(self.steps):
            if self.done:
                print(f"\r{OKCYAN}{self.desc}{NORMAL}", end="")
                break
            print(f"\r{OKCYAN}{self.desc} {WARNING}{c}{NORMAL}", end="")
            sleep(self.timeout)

    def __enter__(self):
        self.start()

    def stop(self):
        self.done = True
        print(f"\n{BOLD}{OKGREEN}{self.end}")

    def __exit__(self, exc_type, exc_value, tb):
        self.stop()