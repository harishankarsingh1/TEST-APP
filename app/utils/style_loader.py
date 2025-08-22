def load_stylesheet():
    """Loads the application's QSS stylesheet with a dark theme."""
    
    # Dark Theme Color Palette
    dark_bg_color = "#2b2b2b"
    dark_widget_bg_color = "#3c3f41" 
    dark_text_color = "#dcdcdc"
    dark_border_color = "#555555"
    dark_accent_color = "#0078d4" 
    dark_button_text_color = "#ffffff"
    dark_disabled_text_color = "#888888"
    dark_disabled_bg_color = "#4a4a4a"
    dark_hover_bg_color = "#4f5254"
    dark_pressed_bg_color = "#5a5e60"
    dark_tab_bg_color = "#313335"
    dark_tab_selected_bg_color = "#45494c" 
    dark_header_bg_color = "#383838"

    # Consistent height and padding for input elements
    standard_control_height = "19px" 
    input_field_padding = "3px 5px" 
    button_padding = "0px 10px" 
    fixed_input_width = "155px" # User requested fixed width

    return f"""
        QMainWindow {{
            background-color: {dark_bg_color};
            border: 2px solid {dark_accent_color}; 
        }}

        QWidget {{
            font-size: 14px; 
            /*font-family: "Segoe UI", "Arial", sans-serif; */
            color: {dark_text_color};
            background-color: {dark_bg_color}; 
        }}

        QDialog {{ 
            background-color: {dark_bg_color};
        }}

        QLabel {{
            font-size: 13px; 
            color: {dark_text_color};
            padding: 5px 10px;
            text-align: left; 
            background-color: transparent; 
            min-height: {standard_control_height}; /* Ensure labels also have a consistent min-height */
            height: {standard_control_height}; /* Optional: if labels also need fixed height */
            min-width: 90px;
            width:100px;
            max-width:150px;
            
        }}

        QLineEdit 
        {{
            border: 1px solid {dark_border_color};
            border-radius: 4px;
            font-size: 13px; 
            padding: {input_field_padding};
            margin: 2px 0; 
            height: {standard_control_height};
            min-height: {standard_control_height};
            background-color: {dark_widget_bg_color};
            color: {dark_text_color};
            width: {fixed_input_width}; /* User requested fixed width */
            min-width: {fixed_input_width}; /* User requested fixed width */
            max-width: {fixed_input_width}; /* User requested fixed width */
        }}
        
        QComboBox,
        QDateEdit {{
            border: 1px solid {dark_border_color};
            border-radius: 4px;
            font-size: 13px; 
            padding: {input_field_padding};
            margin: 2px 0; 
            height: {standard_control_height};
            min-height: {standard_control_height};
            background-color: {dark_widget_bg_color};
            color: {dark_text_color};
            width: {fixed_input_width}; /* User requested fixed width */
            min-width: {fixed_input_width}; /* User requested fixed width */
            max-width:{fixed_input_width};
        }}

        QLineEdit:focus, QComboBox:focus, QDateEdit:focus {{
            border: 1px solid {dark_accent_color};
        }}

        QLineEdit[readOnly="true"] {{
            background-color: #4a4a4a; 
            color: #aaaaaa;
        }}

        /* Remove custom QComboBox arrow styling to use default system arrow */
        /*
        QComboBox::drop-down {{
            subcontrol-origin: padding;
            subcontrol-position: top right;
            width: 18px; 
            background-color: {dark_hover_bg_color};
            border-left: 1px solid {dark_border_color};
            border-top-right-radius: 3px;
            border-bottom-right-radius: 3px;
        }}

        QComboBox::down-arrow {{
            image: none; 
            border: none; 
            width: 0px; 
            height: 0px; 
            margin-right: 5px; 
            margin-left: 3px;
            border-left: 4px solid transparent; 
            border-right: 4px solid transparent;
            border-top: 5px solid {dark_text_color}; 
        }}
        */
        QComboBox {{
            padding-right: 2px; /* Reset padding if custom arrow is removed */
        }}
        QComboBox QAbstractItemView {{ 
            background-color: {dark_widget_bg_color};
            color: {dark_text_color};
            border: 1px solid {dark_border_color};
            selection-background-color: {dark_accent_color};
            selection-color: {dark_button_text_color};
        }}
        QTextEdit {{
            border: 1px solid {dark_border_color};
            border-radius: 4px;
            padding: 5px;
            background-color: {dark_widget_bg_color};
            color: {dark_text_color};
        }}
        QTextEdit#LogDisplay {{ 
             background-color: #212121; 
             color: #c0c0c0;
        }}


        QPushButton {{
            background-color: {dark_accent_color}; 
            color: {dark_button_text_color};
            border-radius: 4px;
            padding: {button_padding};
            margin: 4px 2px;
            font-size: 13px; 
            font-weight: bold;
            border: 1px solid #005a9e; 
            min-width: 100px; /* Make buttons also have this fixed width */
            width: 100px;     /* Make buttons also have this fixed width */
            max-width: 100px;
            height: 25px; 
            min-height: 25px; 
        }}

        QPushButton:hover {{
            background-color: #005a9e; 
        }}

        QPushButton:pressed {{
            background-color: #004578; 
        }}

        QPushButton:disabled {{
            background-color: {dark_disabled_bg_color};
            color: {dark_disabled_text_color};
            border: 1px solid {dark_border_color};
        }}

        QGroupBox {{
            border: 1px solid {dark_border_color};
            border-radius: 4px; 
            margin-top: 1em; 
            padding: 3px;    
            background-color: {dark_widget_bg_color}; 
        }}

        QGroupBox::title {{ 
            subcontrol-origin: margin;
            subcontrol-position: top left;
            padding: 0 5px; 
            font-weight: bold;
            color: {dark_text_color};
            background-color: transparent; 
        }}

        CollapsibleGroupBox > QToolButton {{
            text-align: left;
            padding: 5px;     
            border: none;
            font-weight: bold;
            font-size:14px;
            background-color: {dark_header_bg_color}; 
            border-bottom: 1px solid {dark_border_color}; 
            min-height: {standard_control_height}; 
            height: {standard_control_height}; 
            color: {dark_text_color};
        }}

        CollapsibleGroupBox > QToolButton[checked="true"] {{
            background-color: {dark_pressed_bg_color};
        }}
        CollapsibleGroupBox > QToolButton::down-arrow {{ 
        }}
         CollapsibleGroupBox > QToolButton::right-arrow {{
        }}


        QTabWidget::pane {{
            border: 1px solid {dark_border_color}; 
            border-top: 2px solid {dark_accent_color}; 
        }}

        QTabBar::tab {{
            background: {dark_tab_bg_color};
            border: 1px solid {dark_border_color};
            border-bottom: none; 
            border-top-left-radius: 4px;
            border-top-right-radius: 4px;
            min-width: 8ex;
            padding: 5px 10px; 
            margin-right: 2px;
            color: {dark_text_color};
        }}

        QTabBar::tab:selected {{
            background: {dark_widget_bg_color}; 
            border-color: {dark_accent_color};
            border-bottom-color: {dark_widget_bg_color}; 
            color: #ffffff; 
        }}
        QTabBar::tab:hover {{
            background: {dark_hover_bg_color};
        }}
        QTabBar::tab:!selected {{
            margin-top: 2px;
        }}

        QHeaderView::section {{
            background-color: {dark_header_bg_color};
            color: {dark_text_color};
            padding: 4px;
            border: 1px solid {dark_border_color};
            font-weight: bold;
        }}
        QTableView {{
            gridline-color: {dark_border_color}; 
            background-color: {dark_widget_bg_color};
            color: {dark_text_color};
            alternate-background-color: #313335; 
        }}
         QTableView QLineEdit, QTableView QComboBox {{ 
            background-color: #4a4a4a;
            color: {dark_text_color};
            border: 1px solid {dark_border_color};
            margin-left:15px;
         }}


        QStatusBar {{
            background-color: {dark_header_bg_color};
            border-top: 1px solid {dark_border_color};
            color: {dark_text_color};
        }}

        QStatusBar::item {{
            border: none;
        }}

        QProgressBar {{
            border: 1px solid {dark_border_color};
            border-radius: 3px;
            text-align: center;
            color: {dark_text_color}; 
            background-color: {dark_widget_bg_color};
        }}
        QProgressBar::chunk {{
            background-color: {dark_accent_color};
            width: 10px; 
            margin: 0.5px;
        }}

        QScrollArea {{
            border: none; 
        }}
        QScrollBar:vertical {{
            border: 1px solid {dark_border_color};
            background: {dark_widget_bg_color};
            width: 12px;
            margin: 0px 0px 0px 0px;
        }}
        QScrollBar::handle:vertical {{
            background: #6e6e6e;
            min-height: 20px;
            border-radius: 3px;
        }}
        QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{
            border: none;
            background: none;
            height: 0px;
            subcontrol-position: top;
            subcontrol-origin: margin;
        }}
        QScrollBar:horizontal {{
            border: 1px solid {dark_border_color};
            background: {dark_widget_bg_color};
            height: 12px;
            margin: 0px 0px 0px 0px;
        }}
        QScrollBar::handle:horizontal {{
            background: #6e6e6e;
            min-width: 20px;
            border-radius: 3px;
        }}
        QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {{
            border: none;
            background: none;
            width: 0px;
        }}

        QMenu {{
            background-color: {dark_widget_bg_color};
            color: {dark_text_color};
            border: 1px solid {dark_border_color};
        }}
        QMenu::item {{
            padding: 5px 20px 5px 20px;
        }}
        QMenu::item:selected {{
            background-color: {dark_accent_color};
            color: {dark_button_text_color};
        }}
        QMenu::separator {{
            height: 1px;
            background: {dark_border_color};
            margin-left: 5px;
            margin-right: 5px;
        }}
        QMenuBar {{
            background-color: {dark_header_bg_color};
            color: {dark_text_color};
        }}
        QMenuBar::item {{
            background: transparent;
            padding: 4px 8px;
        }}
        QMenuBar::item:selected {{ 
            background: {dark_accent_color};
            color: {dark_button_text_color};
        }}
        QMenuBar::item:pressed {{
            background: {dark_pressed_bg_color};
        }}
        
        QTextEdit#LogDisplay {{ 
            background-color: #212121; 
            color: #c0c0c0;
       }}
    
        QTreeView {{
            background-color: {dark_widget_bg_color};
            color: {dark_text_color};
            alternate-background-color: #313335; /* Your desired alternate color */
            border: 1px solid {dark_border_color}; /* Optional: if you want a border around the tree */
        }}
        QTreeView::item {{
            padding: 3px; /* Add some padding to items for better readability */
        }}
        QTreeView::item:hover {{
            background-color: {dark_hover_bg_color}; /* Optional: hover effect */
        }}
        QTreeView::item:selected {{
            background-color: {dark_accent_color};
            color: {dark_button_text_color};
        }}
        QTreeWidget {{
            background-color: {dark_widget_bg_color};
            color: {dark_text_color};
            alternate-background-color: #313335; /* Your desired alternate color */
            border: 1px solid {dark_border_color}; /* Optional: border */
        }}
        QTreeWidget::item {{
            padding: 3px;
        }}
        QTreeWidget::item:hover {{
            background-color: {dark_hover_bg_color};
        }}
        QTreeWidget::item:selected {{
            background-color: {dark_accent_color};
            color: {dark_button_text_color};
        }}
        QMessageBox {{
            min-width: 225px;            
        }}
        QMessageBox QLabel {{
            min-width: 225px;
            qproperty-alignment: 'AlignLeft';
            padding: 1px 1px;
            qproperty-wordWrap: true; 
        }}

    """