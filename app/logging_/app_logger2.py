# app_logger2.py

import sys
import logging
from PyQt5.QtCore import QObject, pyqtSignal, QTimer
from typing import Tuple

logger_instance = logging.getLogger(__name__)

class LogEmitter(QObject):
    log_signal = pyqtSignal(str)

class SharedLogBuffer:
    def __init__(self, text_edit_widget, formatter: logging.Formatter, max_lines):
        logger_instance.debug(f"SharedLogBuffer __init__ for instance {id(self)}")
        self.text_edit = text_edit_widget
        self.formatter = formatter
        self.max_lines = max_lines
        self._all_records: list[logging.LogRecord] = []
        self.current_filter_level_name = "INFO"
        self.log_lines_for_display: list[str] = []
        self.log_emitter = LogEmitter()

        try:
            self.log_emitter.log_signal.disconnect(self._append_to_text_edit_gui)
        except TypeError:
            pass
        self.log_emitter.log_signal.connect(self._append_to_text_edit_gui)

        self._gui_update_buffer: list[str] = []
        self._buffer_timer = QTimer()
        self._buffer_timer.setInterval(500)
        self._buffer_timer.timeout.connect(self._process_gui_update_buffer)
        self._buffer_timer.start()
        logger_instance.debug(f"SharedLogBuffer instance {id(self)}: __init__ complete.")

    def add_record(self, record: logging.LogRecord):
        if not hasattr(self, '_all_records'):
            logger_instance.error(f"SharedLogBuffer instance {id(self)}: _all_records MISSING, re-initializing.")
            self._all_records = []

        self._all_records.append(record)
        if len(self._all_records) > self.max_lines * 2:
            self._all_records = self._all_records[-self.max_lines:]

        current_filter_level_numeric = logging.getLevelName(self.current_filter_level_name)
        if record.levelno >= current_filter_level_numeric:
            try:
                formatted_msg = self.formatter.format(record) if self.formatter else record.getMessage()
                self._gui_update_buffer.append(formatted_msg)
            except Exception as ex_format:
                logger_instance.error(f"Log formatting error: {ex_format}", exc_info=True)
                self._gui_update_buffer.append(f"[FORMAT ERROR] {record.getMessage_fallback()}")

    def _process_gui_update_buffer(self):
        if not self._gui_update_buffer or self.text_edit is None:
            return
        full_message_block = "\n".join(self._gui_update_buffer)
        self._gui_update_buffer.clear()
        try:
            self.log_emitter.log_signal.emit(full_message_block)
        except Exception as e_emit:
            print(f"Error during log emit: {e_emit}", file=sys.__stderr__)

    def _append_to_text_edit_gui(self, message_block_to_append: str):
        if self.text_edit is None:
            return
        new_lines = message_block_to_append.splitlines()
        self.log_lines_for_display.extend(new_lines)
        if len(self.log_lines_for_display) > self.max_lines:
            self.log_lines_for_display = self.log_lines_for_display[-self.max_lines:]
        self.text_edit.setPlainText("\n".join(self.log_lines_for_display))
        self.text_edit.verticalScrollBar().setValue(self.text_edit.verticalScrollBar().maximum())

    def set_filter_level(self, level_name: str):
        new_level_name = level_name.upper()
        if self.current_filter_level_name != new_level_name:
            self.current_filter_level_name = new_level_name
            self._repopulate_display_from_all_records()

    def _repopulate_display_from_all_records(self):
        self._buffer_timer.stop()
        self._gui_update_buffer.clear()
        self.log_lines_for_display.clear()
        if self.text_edit:
            self.text_edit.clear()
        level_num = logging.getLevelName(self.current_filter_level_name)
        temp_lines = []
        for record in self._all_records:
            if record.levelno >= level_num:
                try:
                    msg = self.formatter.format(record) if self.formatter else record.getMessage()
                    temp_lines.append(msg)
                except Exception:
                    temp_lines.append(f"Error formatting: {record.getMessage_fallback()}")
        self.log_lines_for_display = temp_lines[-self.max_lines:]
        if self.text_edit:
            self.text_edit.setPlainText("\n".join(self.log_lines_for_display))
            self.text_edit.verticalScrollBar().setValue(self.text_edit.verticalScrollBar().maximum())
        self._buffer_timer.start()

    def clear(self):
        self._buffer_timer.stop()
        self._all_records.clear()
        self.log_lines_for_display.clear()
        self._gui_update_buffer.clear()
        if self.text_edit:
            self.text_edit.clear()
        self._buffer_timer.start()

    def shutdown(self):
        logger_instance.debug(f"SharedLogBuffer {id(self)} shutdown called.")
        try:
            self._buffer_timer.stop()
            self._buffer_timer.timeout.disconnect(self._process_gui_update_buffer)
            self.log_emitter.log_signal.disconnect(self._append_to_text_edit_gui)
        except Exception as e:
            logger_instance.warning(f"SharedLogBuffer shutdown cleanup error: {e}")
        self.text_edit = None
        self._all_records.clear()
        self._gui_update_buffer.clear()
        self.log_lines_for_display.clear()

# Monkey patch fallback
def getMessage_fallback(self):
    try:
        return self.msg % self.args if self.args else str(self.msg)
    except Exception:
        return f"{self.msg} (ERR_ARGS: {self.args})"

if not hasattr(logging.LogRecord, 'getMessage_fallback'):
    logging.LogRecord.getMessage_fallback = getMessage_fallback

class QTextEditStream:
    def __init__(self, shared_buffer_instance: SharedLogBuffer, is_stderr=False):
        self.shared_buffer = shared_buffer_instance
        self.is_stderr = is_stderr
        self.line_buffer: list[str] = []

    def write(self, message: str):
        self.line_buffer.append(message)
        if '\n' in message:
            self.flush()

    def flush(self):
        if not self.line_buffer:
            return
        full_msg = "".join(self.line_buffer).rstrip('\n')
        self.line_buffer.clear()
        if full_msg:
            record = logging.LogRecord(
                name='stderr' if self.is_stderr else 'stdout',
                level=logging.ERROR if self.is_stderr else logging.INFO,
                pathname='', lineno=0, msg=full_msg, args=(), exc_info=None, func=''
            )
            self.shared_buffer.add_record(record)

    def isatty(self): return False

class QtHandler(logging.Handler):
    def __init__(self, shared_buffer_instance: SharedLogBuffer, formatter: logging.Formatter):
        super().__init__()
        self.shared_buffer = shared_buffer_instance
        self.setFormatter(formatter)

    def emit(self, record: logging.LogRecord):
        try:
            self.shared_buffer.add_record(record)
        except Exception:
            self.handleError(record)

    def shutdown(self):
        if self.shared_buffer:
            self.shared_buffer.shutdown()

def setup_logging_system(qtext_edit_for_logs, max_log_lines) -> Tuple[logging.Logger, QtHandler]:
    log_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    shared_buffer = SharedLogBuffer(qtext_edit_for_logs, log_formatter, max_log_lines)
    qt_log_handler = QtHandler(shared_buffer, log_formatter)
    qt_log_handler.setLevel(logging.DEBUG)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    root_logger.addHandler(qt_log_handler)

    sys.stdout = QTextEditStream(shared_buffer, is_stderr=False)
    sys.stderr = QTextEditStream(shared_buffer, is_stderr=True)

    root_logger.info("Logging system initialized. stdout/stderr redirected.")
    return root_logger, qt_log_handler
