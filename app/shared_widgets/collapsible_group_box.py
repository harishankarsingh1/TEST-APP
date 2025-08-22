from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QGroupBox, QSizePolicy, QToolButton
)
from PyQt5.QtCore import Qt, pyqtSignal

class CollapsibleGroupBox(QGroupBox):
    toggled = pyqtSignal(bool)

    def __init__(self, title="", parent=None, ):
        super().__init__("", parent,)
        self.setObjectName("CollapsibleGroupBox")

        # Toggle Button
        self.toggle_button = QToolButton(checkable=True, checked=True)
        self.toggle_button.setObjectName("CollapsibleGroupBoxToggleButton")
        self.toggle_button.setToolButtonStyle(Qt.ToolButtonTextBesideIcon)
        self.toggle_button.setText(title)
        self.toggle_button.setArrowType(Qt.RightArrow)
        self.toggle_button.clicked.connect(self._on_toggle)
        self.toggle_button.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)

        # Content Area
        self.content_area = QWidget()
        self.content_area.setObjectName("CollapsibleContentArea")
        self.content_area.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.content_area.setVisible(False)

        # Layout
        internal_layout = QVBoxLayout()
        internal_layout.setSpacing(0)
        internal_layout.setContentsMargins(1,1,1,1)
        internal_layout.addWidget(self.toggle_button)
        internal_layout.addWidget(self.content_area)
        self.setLayout(internal_layout)

        # Ensure initial visual state matches `checked`
        self._on_toggle()

    def _on_toggle(self):
        checked = self.toggle_button.isChecked()
        self.toggle_button.setArrowType(Qt.DownArrow if checked else Qt.RightArrow)
        self.content_area.setVisible(checked)
        self.toggled.emit(checked)

        if checked:
            self.content_area.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
            self.content_area.adjustSize()
            if self.parentWidget() and self.parentWidget().layout() is not None:
                self.parentWidget().layout().activate()
        else:
            self.content_area.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)

        self.adjustSize()

    def setContentLayout(self, layout_to_set):
        old_layout = self.content_area.layout()
        if old_layout is not None:
            while old_layout.count():
                item = old_layout.takeAt(0)
                widget = item.widget()
                if widget:
                    widget.setParent(None)
            old_layout.deleteLater()
        self.content_area.setLayout(layout_to_set)
        if layout_to_set is not None:
            layout_to_set.setContentsMargins(0,0,0,0)

    def setText(self, title):
        self.toggle_button.setText(title)

    def is_expanded(self):
        return self.toggle_button.isChecked()

    def expand(self):
        if not self.is_expanded():
            self.toggle_button.setChecked(True)
            self._on_toggle()

    def collapse(self):
        if self.is_expanded():
            self.toggle_button.setChecked(False)
            self._on_toggle()
