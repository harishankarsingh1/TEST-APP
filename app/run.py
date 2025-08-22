
import sys
import traceback
from PyQt5.QtWidgets import QApplication
#from PyQt5.QtCore import Qt # <<<< ADD THIS IMPORT
from main_window import MainWindow
from logging_.app_logger2 import setup_logging_system
from utils.style_loader import load_stylesheet
from utils.clean_process import kill_child_processes
from di_testing.scripts.worker_v2 import  global_worker_manager

# --- START OF GLOBAL EXCEPTION HOOK ---
def global_except_hook(exctype, value, tb):
    """Catches ANY unhandled Python exception."""
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!", file=sys.stderr)
    print("----- GLOBAL UNHANDLED EXCEPTION CAUGHT -----", file=sys.stderr)
    print(f"Exception Type: {exctype.__name__}", file=sys.stderr)
    print(f"Exception Value: {value}", file=sys.stderr)
    print("----- TRACEBACK -----", file=sys.stderr)
    traceback.print_tb(tb, file=sys.stderr)
    print("----- END OF GLOBAL UNHANDLED EXCEPTION -----", file=sys.stderr)
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!", file=sys.stderr)

    # If you have a logger configured:
    # logging.critical("Global unhandled exception caught!", exc_info=(exctype, value, tb))

    # For GUI apps, you might want to show a message box (import QMessageBox first)
    # from PyQt5.QtWidgets import QMessageBox
    # QMessageBox.critical(None, "Critical Application Error",
    #                      f"An unhandled error occurred: {exctype.__name__}: {value}\n\n"
    #                      "Please report this error. Details have been printed to the console/log.")

    # Then call the original hook, which usually prints to stderr and exits.
    sys.__excepthook__(exctype, value, tb)
    # To forcefully exit if the default hook doesn't (e.g., if overwritten by Qt):
    # sys.exit(1)

sys.excepthook = global_except_hook

if __name__ == '__main__' :
    app = QApplication(sys.argv)
    app.setStyleSheet(load_stylesheet())
    main_win = MainWindow()
    log_text_edit_instance = main_win.get_log_text_edit_for_setup()
    app_logger , qt_log_handler = setup_logging_system(log_text_edit_instance , 500)
    app_logger.info("Application started and logging is configured.")

    main_win.show()
    try :
        sys.exit(app.exec_())
    finally :
        print("Cancelling all workers...")
        global_worker_manager.cancel_all()
        print("Waiting for all workers to finish...")
        if not global_worker_manager.wait_all(timeout=10) :
            print("Warning: some workers still running after timeout!")
            # Optional: forcibly terminate them if you use QThread subclass
        kill_child_processes()
        print("Cleanup done.")
