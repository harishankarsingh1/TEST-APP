import logging
import ast # For ast.literal_eval
import pandas as pd # For type hinting and result checking

from PyQt5.QtWidgets import (QVBoxLayout, QWidget, QScrollArea, QLabel, 
                             QProgressBar, QFrame) # Added QMessageBox
from PyQt5.QtCore import Qt, pyqtSlot, pyqtSignal, QTimer, QTime, QObject # Added QObject for DummyWorkerSignals

# Imports for GroupBoxes (now within the same package)
from .artifact_batch_group_box import ArtifactBatchGroupBox
from .database_group_box import DatabaseGroupBox
from .universe_group_box import UniverseGroupBox

# Import for createButton utility
from utils.create_button import createButton


from .scripts.worker_v2 import Worker # Assuming your Worker class is in this file
from .scripts.exc_proc_postgres_ import main_process 
from .scripts.batchfile_test_clarifi_v2_adapted import main as clarifi_main

logger = logging.getLogger(__name__)

class LeftPaneWidget(QWidget): 
    runTestRequested = pyqtSignal(dict) 
    resultsAvailable = pyqtSignal(pd.DataFrame, str) 
    processingCompleted = pyqtSignal(bool, str) 

    def __init__(self, parent_di_testing_widget=None, right_pane_ref=None): 
        super().__init__(parent_di_testing_widget) 
        self.setObjectName("DITestingLeftPane")
        self.right_pane = right_pane_ref 
        self.worker = None
        self._active_worker_signals = None # To store the signals object of the current worker
        self.start_time = None
        self._initUI() 
        logger.info("LeftPaneWidget for DI Testing initialized.")


    def _initUI(self):
        self.setMinimumWidth(350) 
        self.setMaximumWidth(450) 
        
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.update_elapsed_time)

        outer_layout = QVBoxLayout(self)
        outer_layout.setContentsMargins(5, 5, 5, 5) 
        outer_layout.setSpacing(2) # Reduced spacing between items in outer_layout

        scroll_area = QScrollArea(self)
        scroll_area.setWidgetResizable(True)
        scroll_area.setFrameShape(QFrame.NoFrame) 
        
        scroll_content_widget = QWidget() 
        scroll_content_widget.setLayout(QVBoxLayout()) 
        scroll_area.setWidget(scroll_content_widget)
        
        content_layout = scroll_content_widget.layout() 
        content_layout.setAlignment(Qt.AlignTop)
        content_layout.setSpacing(2) # Reduced spacing between group boxes

        try:
            self.artifactBatchGroupBox = ArtifactBatchGroupBox(self)
            self.databaseGroupBox = DatabaseGroupBox(self)
            self.universeGroupBox = UniverseGroupBox(self)

            content_layout.addWidget(self.artifactBatchGroupBox)
            content_layout.addWidget(self.databaseGroupBox)
            content_layout.addWidget(self.universeGroupBox)

            # Expand the ArtifactBatchGroupBox by default and make it non-collapsible for this specific instance
            if hasattr(self, 'artifactBatchGroupBox'):
                if hasattr(self.artifactBatchGroupBox, 'expand'):
                    self.artifactBatchGroupBox.expand() 
                # if hasattr(self.artifactBatchGroupBox, 'toggle_button'):
                #     # self.artifactBatchGroupBox.toggle_button.setArrowType(Qt.NoArrow) # Hide arrow
                #     self.artifactBatchGroupBox.toggle_button.setCheckable(False) # Make it not checkable
                #     self.artifactBatchGroupBox.toggle_button.setArrowType(Qt.NoArrow) # Hide arrow
                logger.debug("ArtifactBatchGroupBox expanded by default and made non-collapsible.")

            # Other group boxes remain collapsible but start expanded
            if hasattr(self, 'databaseGroupBox') and hasattr(self.databaseGroupBox, 'expand'):
                self.databaseGroupBox.expand() 
            if hasattr(self, 'universeGroupBox') and hasattr(self.universeGroupBox, 'expand'):
                self.universeGroupBox.expand() 


            self.submitButton = createButton('Submit', self.on_submit, tooltip="Start the Data Integrity test.")
            # self.submitButton.setStyleSheet("QPushButton { font-weight: bold; padding: 8px; }") # Styling handled by global QSS
            content_layout.addWidget(self.submitButton)

            self.cancelButton = createButton('Cancel Test', self.on_cancel, tooltip="Attempt to cancel the running test.")
            self.cancelButton.setEnabled(False)
            content_layout.addWidget(self.cancelButton)

            self.status_label = QLabel("Status: Idle")
            self.status_label.setStyleSheet("font-weight: bold; color: white; width:200")
            content_layout.addWidget(self.status_label)

            self.progress_bar = QProgressBar(self)
            self.progress_bar.setRange(0, 100)
            self.progress_bar.setValue(0)
            self.progress_bar.setTextVisible(True)
            self.progress_bar.setVisible(False) 
            content_layout.addWidget(self.progress_bar)
            
            self.elapsed_time_label = QLabel("Elapsed Time: 00:00:00")
            self.elapsed_time_label.setStyleSheet("font-weight: bold; color: lightgray; width:200")
            self.elapsed_time_label.setVisible(False) 
            content_layout.addWidget(self.elapsed_time_label)

            content_layout.addStretch(1) 

            outer_layout.addWidget(scroll_area)

            if hasattr(self.artifactBatchGroupBox, 'widgets') and 'Source' in self.artifactBatchGroupBox.widgets:
                self.artifactBatchGroupBox.widgets['Source'].currentTextChanged.connect(self.updateGroupBoxVisibility)
            if hasattr(self.artifactBatchGroupBox, 'widgets') and 'Folder' in self.artifactBatchGroupBox.widgets:
                 self.artifactBatchGroupBox.widgets['Folder'].currentTextChanged.connect(self.updateGroupBoxVisibility)
            
            self.updateGroupBoxVisibility() 

        except Exception as e:
            logger.error(f"Error during LeftPaneWidget _initUI: {e}", exc_info=True)
            error_label = QLabel(f"Error initializing UI: {e}")
            content_layout.addWidget(error_label) 

    def updateGroupBoxVisibility(self):
        try:
            source = ""
            if hasattr(self.artifactBatchGroupBox, 'widgets') and 'Source' in self.artifactBatchGroupBox.widgets:
                source = self.artifactBatchGroupBox.widgets['Source'].currentText()
            folder = ""
            if hasattr(self.artifactBatchGroupBox, 'widgets') and 'Folder' in self.artifactBatchGroupBox.widgets:
                folder = self.artifactBatchGroupBox.widgets['Folder'].currentText()

            is_batch = (source == 'Batch File')
            is_clarifi = (source == 'Clarifi')
            is_expressions_in_batch = (is_batch and folder == "expressions")

            if hasattr(self, 'databaseGroupBox'):
                self.databaseGroupBox.setVisible(is_batch) 
            if hasattr(self, 'universeGroupBox'):
                self.universeGroupBox.setVisible(is_clarifi or is_expressions_in_batch) 
        except Exception as e:
            logger.warning(f"Error in LeftPaneWidget updateGroupBoxVisibility: {e}", exc_info=False)

    def on_submit(self):
        logger.info("Submit button clicked.")
        try:
            if self.right_pane and hasattr(self.right_pane, 'clear_results'):
                self.right_pane.clear_results()
            
            self.update_status_message("Preparing to run...", color="yellow")
            self.set_controls_enabled(False) 
            self.progress_bar.setValue(0)
            self.progress_bar.setVisible(True)
            self.cancelButton.setEnabled(True)

            all_settings = self.get_all_configurations()
            if not all_settings: 
                self.on_process_error("Failed to gather configurations.")
                return
            logger.debug(f"Collected settings for DI Test: {all_settings}")
            source = all_settings["artifact_config"]["source"]
            worker_target = None
            worker_args = []

            if source == "Batch File":
                logger.info("Configuring worker for Batch File processing.")
                worker_target = main_process
                ac = all_settings["artifact_config"]
                dc = all_settings["database_config"]
                uc = all_settings["universe_config"] 
                column_dict_val = ac.get("batch_column_dict")
                if isinstance(column_dict_val, str): 
                    try: 
                        column_dict_val = ast.literal_eval(column_dict_val)
                    except (ValueError, SyntaxError): 
                        column_dict_val = None
                        logger.warning("Invalid column_dict string.")
                worker_args = [
                    dc.get("db_url"), ac.get("start_date"), ac.get("stop_date"),
                    ac.get("path"), column_dict_val,
                    uc.get("universe_name") if ac.get("batch_file_folder") == "expressions" else None, 
                    uc.get("universe_type") if ac.get("batch_file_folder") == "expressions" else None, 
                    10, 5, ac.get("sample_size"),
                    ac.get("clarifi_artifact_name"), 
                    uc.get("api_username") if ac.get("batch_file_folder") == "expressions" else None, 
                    uc.get("api_password") if ac.get("batch_file_folder") == "expressions" else None, 
                    ac.get("max_workers", 1), ac.get("batch_file_folder"), ac.get('save_to_sqlite')
                ]
            elif source == "Clarifi":
                logger.info("Configuring worker for Clarifi processing.")
                worker_target = clarifi_main
                ac = all_settings["artifact_config"]

                uc = all_settings["universe_config"]
                basketName, portfolioName = (uc.get("universe_name"), None) if uc.get("universe_type") == "basket" else (None, uc.get("universe_name"))
                sample_type_map = {"Each Path": 1, "Across Path": 2, "Each ID": 3}
                sample_type_val = sample_type_map.get(ac.get("clarifi_sample_by"), 1)
                worker_args = [
                    ac.get("clarifi_artifact_type"), ac.get('path'), 
                    uc.get("api_username"), uc.get("api_password"),
                    ac.get("start_date"), ac.get("stop_date"),
                    basketName, portfolioName, ac.get("clarifi_artifact_name"), 
                    sample_type_val, ac.get("sample_size"), ac.get("max_workers", 1),
                    ac.get('save_to_sqlite')
                ]
            else:
                self.on_process_error(f"Unknown source selected: {source}")
                return

            if worker_target is None or Worker is None or not callable(worker_target) or not callable(Worker):
                 self.on_process_error("Worker or target process not configured correctly (scripts might be missing).")
                 return

            self.worker = Worker(worker_target, *worker_args)
            if self.worker is None or not hasattr(self.worker, 'signals') or \
               not hasattr(self.worker.signals, 'log_signal') or \
               not hasattr(self.worker.signals, 'progress_signal') or \
               not hasattr(self.worker.signals, 'process_finished') or \
               not hasattr(self.worker.signals, 'process_error'): 
                self.on_process_error("Failed to initialize DI Worker or its signals (script might be missing or has incorrect signal setup).")
                self.worker = None 
                return
            
            self._active_worker_signals = self.worker.signals # Store the current worker's signals object

            self.worker.signals.log_signal.connect(self.handle_worker_log) 
            self.worker.signals.progress_signal.connect(self.handle_worker_progress) 
            self.worker.signals.process_finished.connect(self.handle_worker_finished) 
            self.worker.signals.process_error.connect(self.on_process_error)

            self.start_time = QTime.currentTime()
            self.elapsed_time_label.setText("Elapsed Time: 00:00:00")
            self.elapsed_time_label.setVisible(True)
            self.timer.start(1000) 
            self.worker.start_worker() 
            if self.databaseGroupBox.isVisible(): self.databaseGroupBox.saveSettings()
            if self.universeGroupBox.isVisible(): self.universeGroupBox.saveSettings()
        except Exception as e:
            logger.error(f"Exception in on_submit: {e}", exc_info=True) 
            self.on_process_error(f"Submit Error: {e}")

    def get_all_configurations(self):
        try:
            return {
                "universe_config": self.universeGroupBox.get_settings() if hasattr(self, 'universeGroupBox') else {},
                "database_config": self.databaseGroupBox.get_settings() if hasattr(self, 'databaseGroupBox') else {},
                "artifact_config": self.artifactBatchGroupBox.get_settings() if hasattr(self, 'artifactBatchGroupBox') else {}
            }
        except Exception as e:
            logger.error(f"Error gathering configurations: {e}", exc_info=True)
            return None

    def on_cancel(self):
        logger.info("Cancel button clicked.")
        if self.worker and hasattr(self.worker, 'is_running') and self.worker.is_running():
            self.update_status_message("Cancelling...", color="orange")
            self.worker.cancel_worker()
            # Don't disable cancel button immediately; let the worker's finished/error signal handle UI reset.
            # self.cancelButton.setEnabled(False)
            # self.cancelButton.setEnabled(False) 
        else:
            logger.info("No active worker to cancel.")
            if not (self.worker and hasattr(self.worker, 'is_running') and self.worker.is_running()):
                 self.set_controls_enabled(True)
                 self.progress_bar.setVisible(False)
                 self.elapsed_time_label.setVisible(False)
                 self.timer.stop()
                 self.update_status_message("Idle", color="lime")
                 self.cancelButton.setEnabled(False)


    def update_status_message(self, message, color="lightgreen"):
        self.status_label.setText(f"<b><font color='{color}'>Status: {message}</font></b>")

    @pyqtSlot(str)
    def handle_worker_log(self, message):
        if self._active_worker_signals is None or self.sender() is not self._active_worker_signals:
            logger.debug(f"Ignoring log signal from an old/unexpected worker: {message}")
            return
        logger.info(f"DI Worker: {message}") 

    @pyqtSlot(int, int)
    def handle_worker_progress(self, current, total):
        if self._active_worker_signals is None or self.sender() is not self._active_worker_signals:
            logger.debug("Ignoring progress signal from an old/unexpected worker.")
            return
        if total > 0:
            percent = int((current * 100) / total)
            self.progress_bar.setRange(0, 100) 
            self.progress_bar.setValue(percent)
            self.update_status_message(f"Running... ({current}/{total})", color="lightgreen")
        else: 
            self.progress_bar.setRange(0,0) 
            self.progress_bar.setValue(0) 
            self.update_status_message(f"Running... (Processed {current})", color="lightgreen")

    def update_elapsed_time(self):
        if self.start_time and self.timer.isActive():
            elapsed_secs = self.start_time.secsTo(QTime.currentTime())
            hours, rem = divmod(elapsed_secs, 3600)
            minutes, seconds = divmod(rem, 60)
            self.elapsed_time_label.setText(
                f"Elapsed Time: {int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}")
            
    @pyqtSlot(object) 
    def handle_worker_finished(self, result):
        if self._active_worker_signals is None or self.sender() is not self._active_worker_signals:
            logger.warning("handle_worker_finished: Signal from an old or unexpected worker. Ignoring.")
            return
        
        self.timer.stop()
        logger.info(f"Worker finished. Result type: {type(result)}")
        final_status_msg, final_status_color = ("Completed", "green")

        if result is None: 
            final_status_msg, final_status_color = ("Completed (No results or Cancelled)", "darkorange")
            logger.warning("DI Worker finished with None result, possibly cancelled or no data.")
        elif isinstance(result, str) and "error" in result.lower(): 
             final_status_msg, final_status_color = (f"Completed with Error: {result}", "red")
             logger.error(f"DI Worker finished with error string: {result}")
        else:
            try:
                if self.right_pane and hasattr(self.right_pane, 'display_results'):
                    if isinstance(result, list): 
                        if not result:
                             logger.info("Worker returned an empty list of results.")
                             self.right_pane.display_results(pd.DataFrame({'Info': ['No DataFrames returned.']}), "Info")
                        else:
                            for item in result:
                                if isinstance(item, tuple) and len(item) == 2:
                                    tab_name, df_data = item
                                    if isinstance(df_data, pd.DataFrame):
                                        self.right_pane.display_results(df_data, tab_name) 
                                    else: logger.warning(f"Item '{tab_name}' in result list is not a DataFrame.")
                                elif isinstance(item, pd.DataFrame): 
                                     self.right_pane.display_results(item, "Result DataFrame")
                                else: logger.warning(f"Unexpected item type in result list: {type(item)}")
                    elif isinstance(result, pd.DataFrame):
                        self.right_pane.display_results(result, "DI Test Result")
                    else:
                        logger.warning(f"Worker finished with unexpected result type: {type(result)}. Displaying as info.")
                        self.right_pane.display_results(pd.DataFrame({'Info': [f'Process completed. Result: {str(result)}']}), "Process Info")
                else: logger.warning("Right pane not available or no display_results method to show results.")
            except Exception as e:
                logger.error(f"Error processing results in handle_worker_finished: {e}", exc_info=True)
                final_status_msg, final_status_color = ("Error displaying results", "red")
        
        self.update_status_message(final_status_msg, color=final_status_color)
        self.set_controls_enabled(True)
        self.progress_bar.setVisible(False)
        self.progress_bar.setRange(0,100) 
        self.cancelButton.setEnabled(False)
        self.worker = None 
        self._active_worker_signals = None # Clear active signals reference
        self.processingCompleted.emit(final_status_color == "green", final_status_msg)

    @pyqtSlot(str) 
    def on_process_error(self, error_message):
        if self._active_worker_signals is not None and self.sender() is not self._active_worker_signals :
             # If we have an active worker signal object, and this signal is not from it, ignore.
             # This can happen if an error signal from a previous worker arrives late.
            if self.sender() is not None: # Only log if it's a signal from some WorkerSignals
                logger.warning(f"on_process_error: Signal from an old or unexpected worker. Message: {error_message}. Ignoring.")
                return
            # If self.sender() is None, it might be a direct call, proceed with current logic.

        self.timer.stop()
        logger.error(f"Process error reported in LeftPane: {error_message}") 
        self.update_status_message(f"Error: {error_message}", color="red")
        self.set_controls_enabled(True)
        self.progress_bar.setVisible(False)
        self.progress_bar.setRange(0,100)
        self.cancelButton.setEnabled(False)
        
        if self.worker: 
            # Attempt to disconnect signals to prevent further processing from this worker
            # This is a bit tricky as the worker might be auto-deleted.
            try:
                if hasattr(self.worker, 'signals'):
                    self.worker.signals.log_signal.disconnect(self.handle_worker_log)
                    self.worker.signals.progress_signal.disconnect(self.handle_worker_progress)
                    self.worker.signals.process_finished.disconnect(self.handle_worker_finished)
                    self.worker.signals.process_error.disconnect(self.on_process_error)
            except TypeError: # Catches "disconnect() failed between" if already disconnected or object deleted
                pass 
            except Exception as e_disconnect:
                logger.error(f"Error disconnecting worker signals: {e_disconnect}", exc_info=True)

            if hasattr(self.worker, 'is_running') and self.worker.is_running():
                self.worker.cancel_worker() 
            self.worker = None
        self._active_worker_signals = None # Clear active signals reference
        self.processingCompleted.emit(False, error_message)

    def set_controls_enabled(self, enabled: bool):
        if hasattr(self, 'artifactBatchGroupBox'): self.artifactBatchGroupBox.setEnabled(enabled)
        if hasattr(self, 'databaseGroupBox'): self.databaseGroupBox.setEnabled(enabled)
        if hasattr(self, 'universeGroupBox'): self.universeGroupBox.setEnabled(enabled)
        if hasattr(self, 'submitButton'): self.submitButton.setEnabled(enabled)