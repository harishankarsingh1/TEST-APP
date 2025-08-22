import logging
# 1. IMPORT QCheckBox
from PyQt5.QtWidgets import QGridLayout , QLabel , QComboBox , QLineEdit , QDateEdit , QFileDialog , QCheckBox
from PyQt5.QtCore import Qt , QTimer , QSettings
from PyQt5.QtGui import QIntValidator
from utils.create_button import createButton
from shared_widgets.collapsible_group_box import CollapsibleGroupBox
from datetime import datetime , timedelta
import os , json

logger = logging.getLogger(__name__)


class ArtifactBatchGroupBox(CollapsibleGroupBox) :
    def __init__(self , parent=None , ) :
        super().__init__("Artifacts and Batch File Configuration" , parent)
        self.widgets = {}
        self.labels = {}
        self.initUI()
        self.artifactType = None
        self.settings = QSettings("S&P Global" , "ClariFI DI Validator")
        self.last_selected_path = self.settings.value(
            "last_selected_path" , os.path.expanduser('~'))
        logger.debug("ArtifactBatchGroupBox initialized.")

    def get_batch_columns_dict(self , artifact_folder_value) :
        batch_columns_dict = {1 : "path" , 0 : "id" , 5 : "proc" , 2 : "name"}
        if artifact_folder_value == 'baskets' :
            batch_columns_dict = {1 : "path" , 2 : "id" , 6 : "proc"}
        elif artifact_folder_value == 'portfolios' :
            batch_columns_dict = {1 : "path" , 2 : "id" , 9 : "proc"}
        return batch_columns_dict

    def initUI(self) :
        content_layout = QGridLayout()
        content_layout.setAlignment(Qt.AlignTop | Qt.AlignLeft)
        STOPDATE = datetime.now().date()
        STARTDATE = STOPDATE - timedelta(days=90)
        CPU_COUNT = max(1 , os.cpu_count() - 2 if os.cpu_count() and os.cpu_count() > 2 else 1)

        # --- MODIFICATION: The QCheckBox definition is now at the end of this list ---
        field_definitions = [
            ('Max Workers:' , QLineEdit , {'validator' : QIntValidator(1 , CPU_COUNT) , 'default' : str(CPU_COUNT)}) ,
            ('Source:' , QComboBox , {'items' : ['Batch File' , 'Clarifi'] , 'default_index' : 0}) ,
            ('Folder:' , QComboBox , {'items' : ['baskets' , 'portfolios' , 'expressions'] , 'default_index' : 0}) ,
            ('Artifact Type:' , QLineEdit , {'hidden' : True , 'default' : 'SQL'}) ,
            ('Start Date:' , QDateEdit , {'display_format' : "yyyy-MM-dd" , 'default' : STARTDATE}) ,
            ('Stop Date:' , QDateEdit , {'display_format' : "yyyy-MM-dd" , 'default' : STOPDATE}) ,
            ('Column Dict:' , QLineEdit , {'placeholder' : 'e.g., {"col1": "val1"}'}) ,
            ('Sample by:' , QComboBox , {'items' : ['Each Path' , 'Each ID' , 'Across Path' , ] ,
                                         'hidden' : True , 'default_index' : 0}) ,
            ('Sample Size:' , QLineEdit , {'validator' : QIntValidator(0 , 100) , 'default' : '10'}) ,
            ('Name:' , QLineEdit , {}) ,
            ('Path:' , QLineEdit , {}) ,
            # MOVED HERE: The checkbox is now the last item to be created in the loop.
            ('Save to SQLite:' , QCheckBox , {'default' : True}) ,
        ]

        property_methods = {
            'items' : lambda widget , value : widget.addItems(value) if isinstance(widget , QComboBox) else None ,
            'display_format' : lambda widget , value : widget.setDisplayFormat(value) if isinstance(widget ,
                                                                                                    QDateEdit) else None ,
            'validator' : lambda widget , value : widget.setValidator(value) if isinstance(widget ,
                                                                                           QLineEdit) else None ,
            'hidden' : lambda widget , value : widget.setVisible(not value) ,
            'default' : lambda widget , value : (
                widget.setText(str(value)) if isinstance(widget , QLineEdit) else
                widget.setDate(value) if isinstance(widget , QDateEdit) else
                widget.setChecked(value) if isinstance(widget , QCheckBox) else None
            ) ,
            'default_index' : lambda widget , value : widget.setCurrentIndex(value) if isinstance(
                widget , QComboBox) else None ,
            'placeholder' : lambda widget , value : widget.setPlaceholderText(value) if isinstance(
                widget ,QLineEdit) else None ,
        }

        for i , (label_text , widget_type , properties) in enumerate(field_definitions) :
            label = QLabel(label_text)
            widget = widget_type()

            if isinstance(widget , QCheckBox) :
                widget.setText("")
                content_layout.addWidget(label , i , 0 , alignment=Qt.AlignLeft | Qt.AlignVCenter)
                content_layout.addWidget(widget , i , 1 , alignment=Qt.AlignLeft | Qt.AlignVCenter)
            else :
                content_layout.addWidget(label , i , 0 , alignment=Qt.AlignLeft | Qt.AlignVCenter)
                content_layout.addWidget(widget , i , 1 , alignment=Qt.AlignLeft | Qt.AlignVCenter)

            for prop , value in properties.items() :
                if prop in property_methods :
                    property_methods[prop](widget , value)
                    if prop == 'hidden' :
                        label.setVisible(not value)

            self.widgets[label_text.strip(':')] = widget
            self.labels[label_text.strip(':')] = label

        self.widgets['Source'].currentIndexChanged.connect(self.updateArtifactTypeVisibility)
        self.widgets['Folder'].currentTextChanged.connect(self.updateArtifactTypeVisibility)

        self.browseButton = createButton('Browse' , self.browseFile , tooltip="Select a batch file (.txt, .csv)")
        content_layout.addWidget(self.browseButton , len(field_definitions) , 1 , alignment=Qt.AlignLeft)

        self.setContentLayout(content_layout)
        QTimer.singleShot(0 , self.updateArtifactTypeVisibility)

    def updateArtifactTypeVisibility(self) :
        source = self.widgets['Source'].currentText()
        is_batch_file = source == 'Batch File'

        self.toggleWidgetVisibility('Folder' , is_batch_file)
        self.toggleWidgetVisibility('Artifact Type' , not is_batch_file)
        self.toggleWidgetVisibility('Sample by' , not is_batch_file)
        self.browseButton.setEnabled(is_batch_file)
        self.toggleWidgetVisibility('Column Dict' , is_batch_file)

        current_folder_value = ""
        if is_batch_file :
            self.artifactType = self.widgets['Folder'].currentText()
            current_folder_value = self.artifactType
            default_col_dict = self.get_batch_columns_dict(current_folder_value)
            try :
                self.widgets['Column Dict'].setText(str(default_col_dict))
            except Exception as e :
                logger.error(f"Error setting default column dict: {e}")
                self.widgets['Column Dict'].setText("{}")
        else :
            self.artifactType = self.widgets['Artifact Type'].text()
            self.widgets['Artifact Type'].setText("SQL")

    def toggleWidgetVisibility(self , label_key , visible) :
        if label_key in self.widgets and label_key in self.labels :
            self.widgets[label_key].setVisible(visible)
            self.labels[label_key].setVisible(visible)

    def browseFile(self) :
        options = QFileDialog.Options()
        initial_path = self.last_selected_path if os.path.exists(self.last_selected_path) else os.path.expanduser('~')
        filePath , _ = QFileDialog.getOpenFileName(
            self , "Select Batch File" , initial_path , "Text Files (*.txt);;CSV Files (*.csv);;All Files (*)" ,
            options=options)
        if filePath :
            self.settings.setValue("last_selected_path" , filePath)
            self.widgets['Path'].setText(filePath)
            logger.info(f"Batch file selected: {filePath}")

    def getColumnDict(self) :
        if 'Column Dict' in self.widgets :
            text = self.widgets['Column Dict'].text()
            if not text : return None
            try :
                return text
            except json.JSONDecodeError as e :
                logger.error(f"Invalid JSON in 'Column Dict': {text}. Error: {e}")
                return None
        return None

    def get_settings(self) :
        settings_data = {
            "save_to_sqlite" : self.widgets['Save to SQLite'].isChecked() ,
            "max_workers" : int(self.widgets['Max Workers'].text()) if self.widgets[
                'Max Workers'].text().isdigit() else 1 ,
            "source" : self.widgets['Source'].currentText() ,
            "start_date" : self.widgets['Start Date'].date().toString("yyyy-MM-dd") ,
            "stop_date" : self.widgets['Stop Date'].date().toString("yyyy-MM-dd") ,
            "sample_size" : int(self.widgets['Sample Size'].text()) if self.widgets[
                'Sample Size'].text().isdigit() else None ,
            "path" : self.widgets['Path'].text() or None ,
            "clarifi_artifact_name":self.widgets['Name'].text() or None
        }
        if settings_data["source"] == "Batch File" :
            settings_data["batch_file_folder"] = self.widgets['Folder'].currentText()
            settings_data["batch_column_dict"] = self.getColumnDict() or None
        else :
            settings_data["clarifi_artifact_type"] = self.widgets['Artifact Type'].text() or None
            #settings_data["clarifi_artifact_name"] = self.widgets['Name'].text() or None
            settings_data["clarifi_sample_by"] = self.widgets['Sample by'].currentText()
        settings_data["artifact_type_resolved"] = self.artifactType
        return settings_data