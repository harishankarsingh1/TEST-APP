import logging
import pandas as pd 
from PyQt5.QtWidgets import QWidget, QVBoxLayout, QLabel, QTabWidget, QSizePolicy # Added QTabWidget
from PyQt5.QtCore import Qt

from shared_widgets.filterable_table_view import FilterableTableView 

logger = logging.getLogger(__name__)

class RightPaneWidget(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("DITestingRightPane")
        
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(5,5,5,5)

        # self.results_label = QLabel("DI Test Results:")
        # self.results_label.setStyleSheet("font-weight: bold; margin-bottom: 5px;")
        # main_layout.addWidget(self.results_label)

        # Use a QTabWidget to display multiple result DataFrames if needed
        self.results_tab_widget = QTabWidget()
        main_layout.addWidget(self.results_tab_widget, 1) # Tab widget takes expanding space

        # Add an initial placeholder tab or leave empty until results arrive
        self.add_placeholder_tab("Waiting for test results...")
        
        logger.info("RightPaneWidget for DI Testing initialized.")

    def add_placeholder_tab(self, message: str):
        self.clear_results_tabs() # Clear existing tabs first
        placeholder_widget = QWidget()
        layout = QVBoxLayout(placeholder_widget)
        label = QLabel(message)

        layout.addWidget(label)
        # Set QSS for QLabel
        label.setStyleSheet("""
            QLabel {
                padding: 5px 300px;
                border-radius: 5px;
                text-align: top;  /* Aligns text horizontally (use left, right, or center) */
                min-width: 200px;    /* Set minimum width */
                max-width: 400px;    /* Set maximum width */
                width: 300px;       /* Set an exact width (optional) */
            }
        """)
        self.results_tab_widget.addTab(placeholder_widget, "Status")

    def display_results(self, results_df: pd.DataFrame, tab_name: str = "Results"):
        """
        Adds a new tab with a FilterableTableView for the given DataFrame.
        If a tab with the same name exists, it updates it. Otherwise, a new tab is created.
        """
        if not isinstance(results_df, pd.DataFrame):
            logger.warning(f"Invalid data type for results: {type(results_df)}. Expected pandas DataFrame.")
            # Display an error message in a tab
            error_df = pd.DataFrame({'Error': [f'Invalid results data for {tab_name}.']})
            self._add_or_update_results_tab(error_df, f"{tab_name} (Error)")
            return

        logger.info(f"Displaying results in tab '{tab_name}'. Shape: {results_df.shape}")
        self._add_or_update_results_tab(results_df, tab_name)


    def _add_or_update_results_tab(self, df: pd.DataFrame, tab_name: str):
        # Remove placeholder if it exists and this is the first real result
        if self.results_tab_widget.count() == 1:
            widget = self.results_tab_widget.widget(0)
            if isinstance(widget, QWidget) and not isinstance(widget, FilterableTableView):
                 # Assuming placeholder is not FilterableTableView
                self.results_tab_widget.removeTab(0)

        # Check if a tab with this name already exists
        for i in range(self.results_tab_widget.count()):
            if self.results_tab_widget.tabText(i) == tab_name:
                # Tab exists, update its FilterableTableView
                existing_table_view_widget = self.results_tab_widget.widget(i)
                if isinstance(existing_table_view_widget, FilterableTableView):
                    existing_table_view_widget.set_dataframe(df)
                    self.results_tab_widget.setCurrentIndex(i) # Switch to updated tab
                    return
                else: # Unexpected widget type, remove and recreate
                    self.results_tab_widget.removeTab(i)
                    break # Exit loop to recreate tab

        # If tab doesn't exist or was removed, create a new one
        table_view_widget = FilterableTableView(dataframe=df, tab_name=tab_name)
        self.results_tab_widget.addTab(table_view_widget, tab_name)
        self.results_tab_widget.setCurrentWidget(table_view_widget) # Switch to new tab


    def clear_results_tabs(self):
        """Clears all result tabs and adds a placeholder."""
        while self.results_tab_widget.count() > 0:
            self.results_tab_widget.removeTab(0)
        # self.add_placeholder_tab("Results cleared. Waiting for new test...") # Optional: add placeholder after clear
        logger.info("All DI test result tabs cleared.")
        
    def clear_results(self): # Alias for consistency if called from LeftPane
        self.clear_results_tabs()
        self.add_placeholder_tab("Results cleared. Waiting for new test...")