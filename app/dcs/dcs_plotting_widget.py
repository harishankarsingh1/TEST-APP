import logging
import sqlite3
from PyQt5.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel,
                             QLineEdit, QFileDialog, QComboBox, QMessageBox, QApplication,
                             QGroupBox, QToolTip, QListWidget, QAbstractItemView,
                             QSizePolicy, QListWidgetItem, QDialog, QDialogButtonBox) # Added QDialog, QDialogButtonBox
from PyQt5.QtCore import QObject, pyqtSignal, QThread, Qt
from PyQt5.QtGui import QCursor

import pandas as pd
import numpy as np

import matplotlib
matplotlib.use('Agg')
from matplotlib.figure import Figure
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.lines import Line2D 
from matplotlib.legend_handler import HandlerLine2D

logger = logging.getLogger(__name__)

class DataWorker(QObject):
    data_ready = pyqtSignal(pd.DataFrame, list, list) 
    error_occurred = pyqtSignal(str)
    finished = pyqtSignal()

    def __init__(self, db1_path, db2_path):
        super().__init__()
        self.db1_path = db1_path
        self.db2_path = db2_path
        self._worker_logger = logging.getLogger(__name__ + ".DataWorker")

    def run(self):
        self._worker_logger.info(f"Background data loading started for DBs: {self.db1_path}, {self.db2_path}")
        try:
            conn1 = sqlite3.connect(self.db1_path)
            query = '''WITH ranked_stats AS (
                            SELECT 
                                *,
                                ROW_NUMBER() OVER (PARTITION BY exp_name ORDER BY model_id) AS rn
                            FROM stats
                        )
                        SELECT * 
                        FROM ranked_stats
                        WHERE rn = 1
                        ORDER BY exp_name;'''
            df1 = pd.read_sql_query(query, conn1)
            conn1.close()
            source1_name = self.db1_path.split('/')[-1] if self.db1_path else "Database_1"
            df1['source'] = source1_name
            self._worker_logger.debug(f"Data from {self.db1_path} loaded. Source: '{source1_name}'.")

            conn2 = sqlite3.connect(self.db2_path)
            df2 = pd.read_sql_query(query, conn2)
            conn2.close()
            source2_name = self.db2_path.split('/')[-1] if self.db2_path else "Database_2"
            if source1_name == source2_name and source1_name != "Database_1":
                updated_source1_name = f"{source1_name} (1)"
                df1['source'] = updated_source1_name
                source2_name = f"{source2_name} (2)"
            df2['source'] = source2_name
            self._worker_logger.debug(f"Data from {self.db2_path} loaded. Source: '{source2_name}'.")
            
            combined_df = pd.concat([df1, df2], ignore_index=True)
            self._worker_logger.debug("DataFrames combined.")

            if 'exp_name' not in combined_df.columns:
                raise ValueError("Required column 'exp_name' not found in one or both 'stats' tables.")

            unique_exp_names = sorted(combined_df['exp_name'].unique().astype(str))
            
            numeric_columns = list(combined_df.select_dtypes(include=np.number).columns)
            numeric_columns = [col for col in numeric_columns if col not in ('model_id', 'rn')]
            
            self._worker_logger.info("Data processing in worker complete.")
            self.data_ready.emit(combined_df, unique_exp_names, numeric_columns)

        except sqlite3.Error as e_sql:
            self._worker_logger.exception("SQLite error occurred in worker.")
            self.error_occurred.emit(f"SQLite error: {str(e_sql)}")
        except ValueError as e_val:
            self._worker_logger.error(f"Data validation error in worker: {str(e_val)}")
            self.error_occurred.emit(f"Data validation error: {str(e_val)}")
        except Exception as e:
            self._worker_logger.exception("An unexpected error occurred in worker.")
            self.error_occurred.emit(f"Unexpected error during data loading: {str(e)}")
        finally:
            self.finished.emit()

class ExpNameFilterDialog(QDialog):
    def __init__(self, all_exp_names, initially_selected_exp_names, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Filter expr")
        self.all_exp_names = all_exp_names
        # Make a copy to avoid modifying the list passed by the parent directly during dialog interaction
        self.current_dialog_selection = set(initially_selected_exp_names)


        layout = QVBoxLayout(self)

        # Search field
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Search exp_name...")
        self.search_input.textChanged.connect(self._filter_list_items)
        layout.addWidget(self.search_input)

        # List widget for exp_names
        self.exp_name_list_widget = QListWidget()
        self.exp_name_list_widget.setSelectionMode(QAbstractItemView.NoSelection) # Handled by checkboxes
        self.exp_name_list_widget.itemChanged.connect(self._on_item_changed) # Connect to itemChanged
        self._populate_list_widget()
        layout.addWidget(self.exp_name_list_widget)

        # Select All / Deselect All buttons
        button_layout = QHBoxLayout()
        select_all_button = QPushButton("Select All")
        select_all_button.clicked.connect(self._select_all_visible_items)
        deselect_all_button = QPushButton("Deselect All")
        deselect_all_button.clicked.connect(self._deselect_all_visible_items)
        button_layout.addWidget(select_all_button)
        button_layout.addWidget(deselect_all_button)
        button_layout.addStretch(1)
        layout.addLayout(button_layout)

        # OK and Cancel buttons
        self.button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.button_box.accepted.connect(self.accept)
        self.button_box.rejected.connect(self.reject)
        layout.addWidget(self.button_box)
        
        self.setMinimumWidth(350)
        self.setMinimumHeight(400)

    def _on_item_changed(self, item):
        # Update the internal selection state when a checkbox is changed
        if item.checkState() == Qt.Checked:
            self.current_dialog_selection.add(item.text())
        else:
            self.current_dialog_selection.discard(item.text())

    def _populate_list_widget(self, filter_text=""):
        self.exp_name_list_widget.blockSignals(True)
        self.exp_name_list_widget.clear()
        filter_text_lower = filter_text.lower()
        for name in self.all_exp_names:
            if filter_text_lower in name.lower():
                item = QListWidgetItem(name)
                item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
                item.setCheckState(Qt.Checked if name in self.current_dialog_selection else Qt.Unchecked)
                self.exp_name_list_widget.addItem(item)
        self.exp_name_list_widget.blockSignals(False)

    def _filter_list_items(self, text):
        self._populate_list_widget(text)

    def _select_all_visible_items(self):
        self.exp_name_list_widget.blockSignals(True)
        for i in range(self.exp_name_list_widget.count()):
            item = self.exp_name_list_widget.item(i)
            if not item.isHidden(): # Only affect visible items
                item.setCheckState(Qt.Checked)
                self.current_dialog_selection.add(item.text()) # Update internal state
        self.exp_name_list_widget.blockSignals(False)

    def _deselect_all_visible_items(self):
        self.exp_name_list_widget.blockSignals(True)
        for i in range(self.exp_name_list_widget.count()):
            item = self.exp_name_list_widget.item(i)
            if not item.isHidden(): # Only affect visible items
                item.setCheckState(Qt.Unchecked)
                self.current_dialog_selection.discard(item.text()) # Update internal state
        self.exp_name_list_widget.blockSignals(False)

    def get_selected_exp_names(self):
        # This method is called when "OK" is clicked.
        # The self.current_dialog_selection set already reflects the true state of checkboxes.
        return sorted(list(self.current_dialog_selection))


class SQLiteDBsComparisonPlottingWidget(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.db1_path = None
        self.db2_path = None
        self.combined_df = None
        self.all_unique_exp_names = [] 
        self.selected_exp_names = [] 

        self.data_thread = None
        self.data_worker = None
        
        self.chart_dark_bg_color = "#2b2b2b" 
        self.chart_axes_bg_color = "#3c3f41" 
        self.chart_text_color = "#dcdcdc"
        self.chart_border_color = "#555555"
        self.chart_grid_color = "#4a4a4a"
        self.line_colors = ['#0078d4', '#34a853', '#fbbc05', '#ea4335', '#9c27b0', '#00bcd4']
        self.line_markers = ['o', 's', '^', 'D', 'v', 'P', '*', 'X']
        
        self.plotted_lines_map = {} 
        self.legend_items_map = {} 
        self.lines_visibility = {} 

        self.current_tooltip_info = None

        self._init_ui()
        self._display_message_on_canvas("Select database files and hit populate to generate a plot.")
        self.setWindowTitle("DCS data analysis")
        logger.info("SQLiteDBsComparisonPlottingWidget initialized with Chart Area GroupBox.")

    def _init_ui(self):
        main_layout = QVBoxLayout()
        main_layout.setSpacing(10)

        # --- Input Options GroupBox ---
        input_options_group = QGroupBox("Input Options")
        input_options_layout = QVBoxLayout()
        input_options_layout.setSpacing(3) 
        input_options_layout.setContentsMargins(5, 5, 5, 5)

        db1_layout = QHBoxLayout()
        db1_layout.setSpacing(3) 
        db1_label = QLabel("DB1 Path:")
        self.db1_path_display = QLineEdit()
        self.db1_path_display.setReadOnly(True)
        db1_browse_button = QPushButton("Browse...")
        db1_browse_button.setFixedWidth(80)
        db1_browse_button.clicked.connect(lambda: self.select_db_file_dialog(1))
        db1_layout.addWidget(db1_label)
        db1_layout.addWidget(self.db1_path_display, 1) 
        db1_layout.addWidget(db1_browse_button)
        db1_layout.addStretch(1)
        input_options_layout.addLayout(db1_layout)

        db2_layout = QHBoxLayout()
        db2_layout.setSpacing(3)
        db2_label = QLabel("DB2 Path:")
        self.db2_path_display = QLineEdit()
        self.db2_path_display.setReadOnly(True)
        db2_browse_button = QPushButton("Browse...")
        db2_browse_button.setFixedWidth(80)
        db2_browse_button.clicked.connect(lambda: self.select_db_file_dialog(2))
        db2_layout.addWidget(db2_label)
        db2_layout.addWidget(self.db2_path_display, 1)
        db2_layout.addWidget(db2_browse_button)
        db2_layout.addStretch(1)
        input_options_layout.addLayout(db2_layout)
        
        input_options_group.setLayout(input_options_layout)
        input_options_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        
        groupbox_container_layout = QHBoxLayout()
        groupbox_container_layout.addWidget(input_options_group, 0, Qt.AlignLeft)
        main_layout.addLayout(groupbox_container_layout)
        
        # --- Load Button ---
        self.load_button = QPushButton("Populate")
        self.load_button.clicked.connect(self.load_and_prepare_data_action)
        load_button_container_layout = QHBoxLayout()
        # load_button_container_layout.addStretch(1)
        load_button_container_layout.addWidget(self.load_button)
        load_button_container_layout.addStretch(2)
        main_layout.addLayout(load_button_container_layout)

        # --- Chart Area GroupBox ---
        chart_area_group = QGroupBox("Chart Area")
        chart_area_layout = QVBoxLayout()
        chart_area_layout.setSpacing(8)
        chart_area_layout.setContentsMargins(8, 8, 8, 8)


        # Plot Controls (Y-axis and X-axis filter)
        plot_controls_row_layout = QHBoxLayout()
        plot_controls_row_layout.setSpacing(15)

        y_axis_label = QLabel("Y-axis Metric:")
        self.y_metric_combo = QComboBox()
        self.y_metric_combo.setMinimumWidth(200) 
        self.y_metric_combo.currentTextChanged.connect(self.plot_update_handler)
        plot_controls_row_layout.addWidget(y_axis_label)
        plot_controls_row_layout.addWidget(self.y_metric_combo)
        plot_controls_row_layout.addSpacing(20)

        x_axis_label = QLabel("X-axis:")
        self.exp_name_filter_dialog_button = QPushButton("Configure X-axis Filter...")
        self.exp_name_filter_dialog_button.clicked.connect(self._open_exp_name_filter_dialog)
        plot_controls_row_layout.addWidget(x_axis_label)
        plot_controls_row_layout.addWidget(self.exp_name_filter_dialog_button)
        plot_controls_row_layout.addStretch(1) 

        chart_area_layout.addLayout(plot_controls_row_layout) # Add controls row to chart area

        # Plot Display Canvas
        self.figure = Figure(figsize=(12, 8), dpi=100) # Adjust as needed
        self.figure.patch.set_facecolor(self.chart_dark_bg_color)
        self.canvas = FigureCanvas(self.figure)
        self.canvas.setObjectName("plotDisplayCanvas") 
        chart_area_layout.addWidget(self.canvas, 1) # Canvas takes remaining space in chart_area_group

        chart_area_group.setLayout(chart_area_layout)
        main_layout.addWidget(chart_area_group, 1) # Chart area group takes remaining vertical space in main_layout

        self.canvas.mpl_connect('motion_notify_event', self._on_motion)
        self.canvas.mpl_connect('axes_leave_event', self._on_leave_axes)
        self.canvas.mpl_connect('pick_event', self._on_pick)

        self.y_metric_combo.setEnabled(False)
        self.exp_name_filter_dialog_button.setEnabled(False)
        self.setLayout(main_layout)

    def _open_exp_name_filter_dialog(self):
        if not self.all_unique_exp_names:
            QMessageBox.information(self, "No Data", "Load data first to populate X-axis filter options.")
            return

        dialog = ExpNameFilterDialog(self.all_unique_exp_names, self.selected_exp_names, self)
        if dialog.exec_() == QDialog.Accepted:
            new_selected_exp_names = dialog.get_selected_exp_names()
            if set(new_selected_exp_names) != set(self.selected_exp_names):
                self.selected_exp_names = new_selected_exp_names
                logger.info(f"X-axis filter updated. Selected {len(self.selected_exp_names)} items.")
                self.plot_update_handler()
            else:
                logger.debug("X-axis filter dialog closed without changes to selection.")
        else:
            logger.debug("X-axis filter dialog cancelled.")


    def _display_message_on_canvas(self, message):
        logger.debug(f"Displaying message on canvas: {message}")
        self.figure.clear()
        ax = self.figure.add_subplot(111)
        ax.text(0.5, 0.5, message,
                horizontalalignment='center', verticalalignment='center',
                transform=ax.transAxes, fontsize=12, color=self.chart_text_color,
                wrap=True, bbox=dict(boxstyle="round,pad=0.5", fc=self.chart_axes_bg_color, ec=self.chart_border_color, alpha=0.9))
        ax.set_facecolor(self.chart_dark_bg_color)
        ax.set_axis_off()
        self.canvas.draw()


    def select_db_file_dialog(self, db_number):
        path, _ = QFileDialog.getOpenFileName(self, f"Select SQLite Database File {db_number}", "", "SQLite DB Files (*.db *.sqlite *.sqlite3)")
        if path:
            if db_number == 1:
                self.db1_path = path
                self.db1_path_display.setText(path)
            elif db_number == 2:
                self.db2_path = path
                self.db2_path_display.setText(path)

    def load_and_prepare_data_action(self):
        if not self.db1_path or not self.db2_path:
            QMessageBox.warning(self, "Input Required", "Please select both SQLite database files.")
            return
        self.load_button.setEnabled(False)
        self._display_message_on_canvas("Loading data from databases...")
        self.data_thread = QThread()
        self.data_worker = DataWorker(self.db1_path, self.db2_path)
        self.data_worker.moveToThread(self.data_thread)
        self.data_thread.started.connect(self.data_worker.run)
        self.data_worker.data_ready.connect(self.handle_data_loaded)
        self.data_worker.error_occurred.connect(self.handle_loading_error)
        self.data_worker.finished.connect(self.data_thread.quit)
        self.data_worker.finished.connect(self.data_worker.deleteLater)
        self.data_thread.finished.connect(self.data_thread.deleteLater)
        self.data_thread.finished.connect(self.handle_loading_thread_finished)
        self.data_thread.start()


    def handle_data_loaded(self, combined_df, unique_exp_names, numeric_columns):
        logger.info("Background data loading successful.")
        self.combined_df = combined_df
        self.all_unique_exp_names = unique_exp_names 
        self.selected_exp_names = list(self.all_unique_exp_names) 
        self.lines_visibility.clear() 

        self.y_metric_combo.blockSignals(True)
        self.y_metric_combo.clear()
        if numeric_columns:
            self.y_metric_combo.addItems(numeric_columns)
            self.y_metric_combo.setEnabled(True)
        else:
            self.y_metric_combo.setEnabled(False)
            self._display_message_on_canvas("No numeric columns found for Y-axis.")
            QMessageBox.warning(self, "No Numeric Data", "No numeric columns found to plot as Y-axis.")
            self.exp_name_filter_dialog_button.setEnabled(False)
            return
        self.y_metric_combo.blockSignals(False)

        if self.all_unique_exp_names:
            self.exp_name_filter_dialog_button.setEnabled(True)
        else:
            self.exp_name_filter_dialog_button.setEnabled(False)
            self._display_message_on_canvas("No 'exp_name' values found for X-axis.")
            QMessageBox.warning(self, "No X-axis Data", "No 'exp_name' values found for X-axis.")
            return

        QMessageBox.information(self, "Success", "Data loaded. Select options to plot.")
        
        if self.y_metric_combo.count() > 0 and self.all_unique_exp_names:
             self.plot_update_handler()
        else:
             self._display_message_on_canvas("Data loaded, but not enough information to plot yet.")


    def handle_loading_error(self, error_message):
        logger.error(f"Error from background data loading: {error_message}")
        QMessageBox.critical(self, "Error Loading Data", error_message)
        self._reset_ui_on_load_failure(error_message=error_message)


    def handle_loading_thread_finished(self):
        logger.info("Background data loading thread has finished.")
        self.load_button.setEnabled(True)


    def _reset_ui_on_load_failure(self, error_message=None):
        self.combined_df = None
        self.all_unique_exp_names = []
        self.selected_exp_names = []
        self.lines_visibility.clear()
        self.y_metric_combo.clear()
        self.y_metric_combo.setEnabled(False)
        self.exp_name_filter_dialog_button.setEnabled(False)
        msg = "Data loading failed or was reset."
        if error_message:
            msg = f"Data loading failed: {error_message}\nPlease check inputs."
        self._display_message_on_canvas(msg)
        if not self.load_button.isEnabled():
            self.load_button.setEnabled(True)


    def plot_update_handler(self):
        logger.debug("Plot update handler triggered.")
        
        if not self.y_metric_combo.isEnabled() or not self.y_metric_combo.currentText():
            self._display_message_on_canvas("Please select a Y-axis Metric.")
            return
        if not self.exp_name_filter_dialog_button.isEnabled() or not self.selected_exp_names:
            self._display_message_on_canvas("Please select at least one 'exp_name' from the X-axis filter.")
            return

        if self.combined_df is not None:
            sources = sorted(self.combined_df['source'].unique())
            for source_name in sources:
                self.lines_visibility[source_name] = True
        
        self.update_plot_view()


    def update_plot_view(self):
        logger.info("Attempting to update the Matplotlib line chart on canvas.")
        if self.combined_df is None or not self.selected_exp_names:
            self._display_message_on_canvas("No data loaded or no 'exp_name' categories selected for X-axis.")
            return

        selected_y_metric = self.y_metric_combo.currentText()
        if not selected_y_metric:
            self._display_message_on_canvas("Please select a Y-axis Metric.")
            return
            
        self._display_message_on_canvas(f"Generating line chart for {selected_y_metric}...")
        QApplication.processEvents()

        try:
            if 'source' not in self.combined_df.columns:
                self._display_message_on_canvas("Critical 'source' column missing.")
                return
            
            filtered_df_for_plot = self.combined_df[self.combined_df['exp_name'].isin(self.selected_exp_names)]
            
            current_plot_df = filtered_df_for_plot[['exp_name', 'source', selected_y_metric]].copy()
            if not pd.api.types.is_numeric_dtype(current_plot_df[selected_y_metric]):
                try:
                    current_plot_df[selected_y_metric] = pd.to_numeric(current_plot_df[selected_y_metric], errors='coerce')
                except ValueError:
                    self._display_message_on_canvas(f"Metric '{selected_y_metric}' could not be converted to numeric.")
                    return

            self.figure.clear() 
            self.plotted_lines_map.clear() 
            self.legend_items_map.clear()
            ax = self.figure.add_subplot(111)
            
            self.figure.patch.set_facecolor(self.chart_dark_bg_color)
            ax.set_facecolor(self.chart_axes_bg_color)
            for spine in ax.spines.values():
                spine.set_color(self.chart_border_color)
            ax.tick_params(axis='x', colors=self.chart_text_color, labelrotation=45)
            ax.tick_params(axis='y', colors=self.chart_text_color)
            ax.grid(True, color=self.chart_grid_color, linestyle='--', linewidth=0.5, alpha=0.7)

            sources = sorted(current_plot_df['source'].unique())
            exp_names_for_x = self.selected_exp_names 
            x_indices = np.arange(len(exp_names_for_x))

            legend_handles = []
            legend_labels = []

            for i, source_name in enumerate(sources):
                if source_name not in self.lines_visibility:
                    self.lines_visibility[source_name] = True

                source_df = current_plot_df[current_plot_df['source'] == source_name]
                source_metric_series = source_df.set_index('exp_name')[selected_y_metric]
                aligned_metric_values = source_metric_series.reindex(exp_names_for_x).values 
                
                line_color = self.line_colors[i % len(self.line_colors)]
                line_marker = self.line_markers[i % len(self.line_markers)]

                line, = ax.plot(x_indices, aligned_metric_values, marker=line_marker, linestyle='-', 
                                color=line_color, label=source_name, markersize=7, linewidth=2,
                                picker=5, 
                                visible=self.lines_visibility.get(source_name, True)) 
                
                self.plotted_lines_map[source_name] = line
                legend_handles.append(line)
                legend_labels.append(source_name)
                
            ax.set_ylabel(selected_y_metric, fontsize=12, color=self.chart_text_color)
            ax.set_xlabel('Experiment Name', fontsize=12, color=self.chart_text_color)
            ax.set_title(f"DCS data analysis: {selected_y_metric}", 
                         pad=20, fontsize=14, color=self.chart_text_color)
            
            if len(exp_names_for_x) > 0 : 
                ax.set_xticks(x_indices)
                ax.set_xticklabels(exp_names_for_x, ha="right", fontsize=9)
            else: 
                ax.set_xticks([])
                ax.set_xticklabels([])
            
            if not ax.lines and len(exp_names_for_x) > 0 :
                 ax.set_ylim(0, 10)
            elif not exp_names_for_x:
                 ax.set_ylim(0,1)

            legend = ax.legend(handles=legend_handles, labels=legend_labels, title="Database Source", frameon=True)
            if legend:
                legend.get_frame().set_facecolor(self.chart_axes_bg_color)
                legend.get_frame().set_edgecolor(self.chart_border_color)
                legend.get_title().set_color(self.chart_text_color)

                for legline, legtext, orig_line_label in zip(legend.get_lines(), legend.get_texts(), legend_labels):
                    legline.set_picker(5)
                    legtext.set_picker(5)
                    legtext.set_color(self.chart_text_color)
                    self.legend_items_map[orig_line_label] = {'line': legline, 'text': legtext}
                    if not self.lines_visibility.get(orig_line_label, True):
                        legline.set_alpha(0.3)
                        legtext.set_alpha(0.3)

            self.figure.subplots_adjust(bottom=0.25 if exp_names_for_x else 0.1, 
                                        top=0.90, left=0.1, right=0.95)
            ax.relim() 
            ax.autoscale_view(scalex=False, scaley=True)

            self.canvas.draw()
            logger.info(f"Matplotlib line chart drawn for metric '{selected_y_metric}' with {len(exp_names_for_x)} X-categories.")

        except Exception as e:
            QMessageBox.critical(self, "Plotting Error", f"An error occurred: {str(e)}")
            logger.exception("Exception during Matplotlib line chart generation on canvas:")
            self._display_message_on_canvas(f"An error occurred during plotting: {str(e)}")

    def _on_pick(self, event):
        artist = event.artist
        ax = self.figure.gca() 
        picked_source_name = None
        for source_name, leg_item in self.legend_items_map.items():
            if artist == leg_item['line'] or artist == leg_item['text']:
                picked_source_name = source_name
                break
        
        if picked_source_name and picked_source_name in self.plotted_lines_map:
            self.lines_visibility[picked_source_name] = not self.lines_visibility[picked_source_name]
            is_visible = self.lines_visibility[picked_source_name]
            plot_line = self.plotted_lines_map[picked_source_name]
            plot_line.set_visible(is_visible)
            leg_item = self.legend_items_map[picked_source_name]
            leg_item['line'].set_alpha(1.0 if is_visible else 0.3)
            leg_item['text'].set_alpha(1.0 if is_visible else 0.3)
            ax.relim()
            ax.autoscale_view(scalex=False, scaley=True)
            self.canvas.draw()
            logger.debug(f"Toggled visibility for '{picked_source_name}' to {is_visible}")


    def _on_motion(self, event):
        if event.inaxes and event.xdata is not None:
            ax = event.inaxes
            
            if not self.selected_exp_names:
                if self.current_tooltip_info is not None: QToolTip.hideText(); self.current_tooltip_info = None
                return

            x_indices_plotted = np.arange(len(self.selected_exp_names))
            # Ensure x_indices_plotted is not empty before using np.argmin
            if len(x_indices_plotted) == 0:
                if self.current_tooltip_info is not None: QToolTip.hideText(); self.current_tooltip_info = None
                return

            closest_x_index = np.argmin(np.abs(x_indices_plotted - event.xdata))
            
            if abs(x_indices_plotted[closest_x_index] - event.xdata) > 0.5:
                if self.current_tooltip_info is not None: QToolTip.hideText(); self.current_tooltip_info = None
                return

            exp_name_under_mouse = self.selected_exp_names[closest_x_index]

            if self.current_tooltip_info and self.current_tooltip_info.get('exp_name') == exp_name_under_mouse:
                return 

            tooltip_parts = [f"Exp: {exp_name_under_mouse}"]
            found_any_data_for_tooltip = False
            selected_y_metric = self.y_metric_combo.currentText()

            if self.combined_df is not None and selected_y_metric:
                for source_name, line_obj in self.plotted_lines_map.items():
                    if line_obj.get_visible():
                        source_data = self.combined_df[
                            (self.combined_df['source'] == source_name) &
                            (self.combined_df['exp_name'] == exp_name_under_mouse)
                        ]
                        y_value_str = "N/A"
                        if not source_data.empty:
                            y_value = source_data[selected_y_metric].iloc[0]
                            if pd.notna(y_value):
                                y_value_str = f"{y_value:.2f}" if isinstance(y_value, float) else str(y_value)
                                found_any_data_for_tooltip = True
                        tooltip_parts.append(f"{source_name}: {y_value_str}")
            
            if found_any_data_for_tooltip:
                tooltip_text = "\n".join(tooltip_parts)
                QToolTip.showText(QCursor.pos(), tooltip_text, self.canvas, self.canvas.rect())
                self.current_tooltip_info = {'exp_name': exp_name_under_mouse}
            else:
                if self.current_tooltip_info is not None: QToolTip.hideText(); self.current_tooltip_info = None
        else:
             if self.current_tooltip_info is not None:
                QToolTip.hideText()
                self.current_tooltip_info = None

    def _on_leave_axes(self, event):
        if self.current_tooltip_info is not None:
            QToolTip.hideText()
            self.current_tooltip_info = None

    def closeEvent(self, event):
        logger.debug("Close event called.")
        if self.data_thread and self.data_thread.isRunning():
            logger.info("Data loading thread running. Attempting to quit.")
            self.data_thread.quit()
            if not self.data_thread.wait(1000):
                logger.warning("Data loading thread did not quit in time.")
        super().closeEvent(event)


if __name__ == '__main__':
    import sys
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    
    app = QApplication(sys.argv)
    
    stylesheet = """
        QWidget {
            font-size: 13px; 
            color: #dcdcdc;
            background-color: #2b2b2b; 
        }
        QGroupBox {
            border: 1px solid #555555;
            border-radius: 4px; 
            margin-top: 0.8em; 
            padding: 5px; 
            background-color: #3c3f41; 
        }
        QGroupBox::title { 
            subcontrol-origin: margin;
            subcontrol-position: top left;
            padding: 0 3px; 
            font-weight: bold;
            color: #dcdcdc;
            background-color: #3c3f41; 
            border-radius: 3px;
            margin-left: 5px; 
        }
        QWidget#plotDisplayCanvas {
            border: 1px solid #4a4a4a; 
        }
        QPushButton { 
            background-color: #0078d4; color: white; border-radius: 4px; 
            padding: 4px 8px; 
            margin: 1px; 
            font-weight: bold;
            min-height: 24px; 
        }
        QPushButton[checkable="true"] {
             padding: 4px 8px;
        }
        QPushButton:hover { background-color: #005a9e; }
        QPushButton:disabled { background-color: #4a4a4a; color: #888888; }
        QLineEdit, QComboBox, QListWidget { 
            border: 1px solid #555555; border-radius: 4px; padding: 2px 4px;
            min-height: 24px; background-color: #313335; color: #dcdcdc;
        }
        QLineEdit[readOnly="true"] {
             background-color: #383838;
        }
        QListWidget::item { 
            padding: 2px; 
        }
        QListWidget::item:selected { /* Might not be visible with NoSelection mode */
            background-color: #0078d4; 
            color: white;
        }
        QComboBox::drop-down { border: none; }
        QComboBox::down-arrow { image: url(noop.png); width:10px; height:10px; } 
        QLabel { 
            padding: 2px; 
            background-color: transparent;
            min-height: 24px; 
            font-weight: normal;
            margin-right: 1px; 
        }
        QToolTip {
            color: #dcdcdc;
            background-color: #1e1e1e;
            border: 1px solid #555555;
            padding: 4px;
            border-radius: 3px;
            opacity: 230;
        }
        /* Styles for the ExpNameFilterDialog */
        QDialog QLineEdit { 
            margin-bottom: 5px;
        }
        QDialog QListWidget { 
            margin-bottom: 5px;
        }
        QDialog QPushButton { 
            min-width: 100px; /* Wider buttons in dialog */
            padding: 5px 10px;
        }
    """
    app.setStyleSheet(stylesheet)

    widget = SQLiteDBsComparisonPlottingWidget()
    widget.setGeometry(50, 50, 1100, 800) 
    widget.show()
    sys.exit(app.exec_())
