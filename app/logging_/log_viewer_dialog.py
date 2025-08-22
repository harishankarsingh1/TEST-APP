import logging
from PyQt5.QtWidgets import QDialog, QVBoxLayout, QDialogButtonBox, QMessageBox
from PyQt5.QtCore import Qt
from .log_widget import LogWidget 
from .app_logger2 import QtHandler, SharedLogBuffer # To access SharedLogBuffer

logger = logging.getLogger(__name__)

class LogViewerDialog(QDialog):
    def __init__(self, log_widget_instance: LogWidget, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Application Log Viewer")
        self.setMinimumSize(700, 500)
        self.setWindowModality(Qt.NonModal) 

        self.log_widget = log_widget_instance # Use the passed instance
        self.log_widget.clear_button.clicked.connect(self.confirm_clear_logs)
        self.log_widget.level_changed.connect(self.on_log_level_filter_changed)


        main_layout = QVBoxLayout(self)
        main_layout.addWidget(self.log_widget)

        self.button_box = QDialogButtonBox(QDialogButtonBox.Close)
        self.button_box.rejected.connect(self.reject) 
        
        main_layout.addWidget(self.button_box)
        self.setLayout(main_layout)
        logger.debug("LogViewerDialog UI setup complete.")
        
        # Set initial filter level for the buffer
        self.on_log_level_filter_changed(self.log_widget.level_filter_combo.currentText())


    def on_log_level_filter_changed(self, level_name: str):
        logger.debug(f"Log level filter in Dialog changed to: {level_name}")
        # Find the SharedLogBuffer instance to call set_filter_level
        # This assumes QtHandler is attached to the root logger and uses the same SharedLogBuffer
        # that was initialized with the LogWidget's QTextEdit.
        root_logger = logging.getLogger()
        for handler in root_logger.handlers:
            if isinstance(handler, QtHandler) and hasattr(handler, 'shared_buffer'):
                if isinstance(handler.shared_buffer, SharedLogBuffer):
                    handler.shared_buffer.set_filter_level(level_name)
                    logger.debug(f"Filter level set on SharedLogBuffer to {level_name}")
                    return
        logger.warning("Could not find SharedLogBuffer to set filter level.")


    def confirm_clear_logs(self):
        reply = QMessageBox.question(self, 'Confirm Clear', 
                                     "Are you sure you want to clear all logs from the display and buffer?",
                                     QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        if reply == QMessageBox.Yes:
            try:
                root_logger = logging.getLogger()
                cleared_by_handler = False
                for handler in root_logger.handlers:
                    if isinstance(handler, QtHandler) and hasattr(handler, 'clear_logs_in_buffer'): 
                        handler.clear_logs_in_buffer() # This will call SharedLogBuffer.clear()
                        cleared_by_handler = True
                        # Assuming one QtHandler is responsible for the main log display
                        break 
                
                if cleared_by_handler:
                    logger.info("Log display and buffer cleared by user via handler(s).")
                    # Also reset the combo box in LogWidget to default if desired,
                    # or ensure SharedLogBuffer.clear() resets its internal filter level.
                    self.log_widget.level_filter_combo.setCurrentText("INFO")

                else: # Fallback, though unlikely if setup is correct
                    self.log_widget.log_display.clear() 
                    logger.warning("Log display cleared directly (no handler with clear_logs_in_buffer found). Buffer might persist.")

            except Exception as e:
                logger.error(f"Error clearing log display: {e}", exc_info=True)
                QMessageBox.critical(self, "Error", f"Could not clear logs: {e}")

    def closeEvent(self, event):
        logger.debug("LogViewerDialog closing.")
        super().closeEvent(event)