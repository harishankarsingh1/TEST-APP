import time
import traceback
import threading
from PyQt5.QtCore import QObject, pyqtSignal, QRunnable, QThreadPool, pyqtSlot

# --- WorkerSignals for communication ---
class WorkerSignals(QObject):
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int, int)
    process_finished = pyqtSignal(object)
    process_error = pyqtSignal(str)

# --- Global Worker Manager ---
class WorkerManager:
    def __init__(self):
        self._active_workers = set()
        self._lock = threading.Lock()

    def register(self, worker: QRunnable):
        with self._lock:
            self._active_workers.add(worker)

    def unregister(self, worker: QRunnable):
        with self._lock:
            self._active_workers.discard(worker)

    def cancel_all(self):
        with self._lock:
            for worker in list(self._active_workers):
                if hasattr(worker, 'cancel_worker'):
                    worker.cancel_worker()

    def wait_all(self, timeout=None):
        """
        Waits for all workers to finish.
        This only waits by polling their is_running() method.
        """
        import time as _time
        deadline = None if timeout is None else (_time.time() + timeout)
        while True:
            with self._lock:
                still_running = [w for w in self._active_workers if getattr(w, 'is_running', lambda: False)()]
            if not still_running:
                break
            if deadline and _time.time() > deadline:
                break
            _time.sleep(0.1)

# Global singleton instance
global_worker_manager = WorkerManager()

# --- Worker class ---
class Worker(QRunnable):
    def __init__(self, func, *args, **kwargs):
        super().__init__()
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.signals = WorkerSignals()
        self._cancel_event = threading.Event()
        self._is_running = False
        self.setAutoDelete(True)
        global_worker_manager.register(self)

    @pyqtSlot()
    def run(self):
        self._is_running = True
        try:
            updated_kwargs = self.kwargs.copy()
            updated_kwargs['log_emitter'] = self.signals.log_signal.emit
            updated_kwargs['progress_emitter'] = self.signals.progress_signal.emit
            updated_kwargs['cancel_event'] = self._cancel_event

            self.signals.log_signal.emit("Worker started.")
            result = self.func(*self.args, **updated_kwargs)

            if not self._cancel_event.is_set():
                self.signals.log_signal.emit("Worker finished successfully.")
                self.signals.process_finished.emit(result)
            else:
                self.signals.log_signal.emit("Worker execution cancelled.")
                self.signals.process_finished.emit(None)

        except Exception as e:
            error_msg = f"Worker error: {e}\n{traceback.format_exc()}"
            self.signals.log_signal.emit(error_msg)
            self.signals.process_error.emit(error_msg)

        finally:
            self.signals.log_signal.emit("Worker run method complete.")
            self._is_running = False
            global_worker_manager.unregister(self)

    def start_worker(self):
        QThreadPool.globalInstance().start(self)
        self.signals.log_signal.emit("Worker submitted to thread pool.")

    def cancel_worker(self):
        self.signals.log_signal.emit("Cancellation requested.")
        self._cancel_event.set()

    def is_running(self):
        return self._is_running

    # Convenience methods to connect signals
    def connect_log(self, slot):
        self.signals.log_signal.connect(slot)

    def connect_progress(self, slot):
        self.signals.progress_signal.connect(slot)

    def connect_finished(self, slot):
        self.signals.process_finished.connect(slot)

    def connect_error(self, slot):
        self.signals.process_error.connect(slot)
