from PyQt6.QtWidgets import QApplication, QMainWindow, QStackedWidget, QPushButton, QWidget
import sys
from config import load_config
from database_manager import DatabaseManager
from widget_a import WidgetA
from widget_b import WidgetB

class MainWindow(QMainWindow):
    def __init__(self, db_manager, other_config):
        super().__init__()
        self.db_manager = db_manager
        self.other_config = other_config
        self.setWindowTitle(self.other_config.app_name)
        
        self.init_ui()

    def init_ui(self):
        self.stacked_widget = QStackedWidget()
        self.setCentralWidget(self.stacked_widget)
        
        # Instantiate other main widgets with db_manager
        self.widget_a = WidgetA(self.db_manager)
        self.widget_b = WidgetB(self.db_manager)
        
        # Add widgets to the stacked widget
        self.stacked_widget.addWidget(self.widget_a)
        self.stacked_widget.addWidget(self.widget_b)
        
        # Sample buttons to switch between widgets
        button1 = QPushButton('Show WidgetA')
        button1.clicked.connect(self.show_widget_a)
        self.stacked_widget.addWidget(button1)

        button2 = QPushButton('Show WidgetB')
        button2.clicked.connect(self.show_widget_b)
        self.stacked_widget.addWidget(button2)

    def show_widget_a(self):
        self.stacked_widget.setCurrentWidget(self.widget_a)

    def show_widget_b(self):
        self.stacked_widget.setCurrentWidget(self.widget_b)

    def closeEvent(self, event):
        # Perform cleanup before closing
        self.db_manager.cleanup()
        event.accept()

   # Main application entry point, simplified
if __name__ == '__main__':
    config_file = 'path/to/config.json'

    config = load_config(config_file)
    db_manager = DatabaseManager(config)

    app = QApplication(sys.argv)
    main_window = MainWindow(db_manager, config.other_config)
    main_window.show()
    sys.exit(app.exec())