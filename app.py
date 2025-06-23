import os
import sys
import subprocess
import datetime
import time
import traceback
import platform
import csv
import shutil
import warnings
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
                             QLabel, QLineEdit, QPushButton, QComboBox, QListWidget,
                             QMessageBox, QFileDialog, QTabWidget, QGroupBox, 
                             QTableWidget, QTableWidgetItem, QHeaderView, QCheckBox)
from PyQt5.QtCore import Qt, QTimer
import psycopg2
import pymysql
from configparser import ConfigParser
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

# Suppress warnings
warnings.filterwarnings("ignore", category=UserWarning, message="pkg_resources is deprecated")
warnings.filterwarnings("ignore", category=DeprecationWarning, message="sipPyTypeDict")

# Import service management modules (Windows only)
if platform.system() == 'Windows':
    import win32serviceutil
    import win32service
    import win32con
    import win32api
    import win32process
    import win32event
    try:
        import wmi
    except ImportError:
        wmi = None
import psutil

class DatabaseBackupApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Database Backup Manager")
        self.setGeometry(100, 100, 900, 700)
        
        self.connection = None
        self.current_db_type = None
        self.current_postgres_service = None
        self.current_mysql_service = None
        self.pg_dump_path = None
        self.pg_restore_path = None
        self.mysqldump_path = None
        self.mysql_path = None
        self.max_backups = 3
        self.background_processes = []  # Track background processes
        
        # Initialize scheduler
        self.scheduler = BackgroundScheduler()
        self.scheduler.start()
        
        # Check for admin rights on Windows
        if platform.system() == 'Windows':
            self.check_admin_privileges()
        
        self.init_ui()
        self.load_config()
        self.find_database_tools()
    
    def check_admin_privileges(self):
        """Check if running with admin privileges on Windows"""
        try:
            import ctypes
            return ctypes.windll.shell32.IsUserAnAdmin()
        except:
            return False
    
    def init_ui(self):
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        main_layout = QVBoxLayout(main_widget)
        
        tabs = QTabWidget()
        main_layout.addWidget(tabs)
        
        # Connection Tab
        connection_tab = QWidget()
        tabs.addTab(connection_tab, "Connection")
        self.setup_connection_tab(connection_tab)
        
        # Backup Tab
        backup_tab = QWidget()
        tabs.addTab(backup_tab, "Backup/Restore")
        self.setup_backup_tab(backup_tab)
        
        # User Management Tab
        user_tab = QWidget()
        tabs.addTab(user_tab, "User Management")
        self.setup_user_tab(user_tab)
        
        self.statusBar().showMessage("Ready")
    
    def setup_connection_tab(self, tab):
        layout = QVBoxLayout(tab)
        
        # Database type selection
        db_type_group = QGroupBox("Database Type")
        db_type_layout = QHBoxLayout()
        self.db_type_combo = QComboBox()
        self.db_type_combo.addItems(["PostgreSQL", "MySQL"])
        db_type_layout.addWidget(QLabel("Database:"))
        db_type_layout.addWidget(self.db_type_combo)
        db_type_group.setLayout(db_type_layout)
        layout.addWidget(db_type_group)
        
        # Connection details
        connection_group = QGroupBox("Connection Details")
        connection_layout = QVBoxLayout()
        
        # Host
        host_layout = QHBoxLayout()
        host_layout.addWidget(QLabel("Host:"))
        self.host_input = QLineEdit("localhost")
        host_layout.addWidget(self.host_input)
        connection_layout.addLayout(host_layout)
        
        # Port
        port_layout = QHBoxLayout()
        port_layout.addWidget(QLabel("Port:"))
        self.port_input = QLineEdit()
        self.port_input.setPlaceholderText("5432 for PostgreSQL, 3306 for MySQL")
        port_layout.addWidget(self.port_input)
        connection_layout.addLayout(port_layout)
        
        # Database
        db_layout = QHBoxLayout()
        db_layout.addWidget(QLabel("Database:"))
        self.db_name_input = QLineEdit()
        db_layout.addWidget(self.db_name_input)
        connection_layout.addLayout(db_layout)
        
        # Username
        user_layout = QHBoxLayout()
        user_layout.addWidget(QLabel("Username:"))
        self.user_input = QLineEdit()
        user_layout.addWidget(self.user_input)
        connection_layout.addLayout(user_layout)
        
        # Password
        pass_layout = QHBoxLayout()
        pass_layout.addWidget(QLabel("Password:"))
        self.pass_input = QLineEdit()
        self.pass_input.setEchoMode(QLineEdit.Password)
        pass_layout.addWidget(self.pass_input)
        connection_layout.addLayout(pass_layout)
        
        connection_group.setLayout(connection_layout)
        layout.addWidget(connection_group)
        
        # Manual Tool Paths
        tool_path_group = QGroupBox("Manual Tool Paths")
        tool_path_layout = QVBoxLayout()
        
        # PostgreSQL paths
        pg_layout = QHBoxLayout()
        pg_layout.addWidget(QLabel("pg_dump:"))
        self.pg_dump_path_input = QLineEdit()
        self.pg_dump_path_input.setPlaceholderText("Leave blank for auto-detection")
        pg_layout.addWidget(self.pg_dump_path_input)
        pg_browse = QPushButton("Browse...")
        pg_browse.clicked.connect(lambda: self.browse_for_tool("pg_dump"))
        pg_layout.addWidget(pg_browse)
        tool_path_layout.addLayout(pg_layout)
        
        # MySQL paths
        mysql_layout = QHBoxLayout()
        mysql_layout.addWidget(QLabel("mysqldump:"))
        self.mysqldump_path_input = QLineEdit()
        self.mysqldump_path_input.setPlaceholderText("Leave blank for auto-detection")
        mysql_layout.addWidget(self.mysqldump_path_input)
        mysql_browse = QPushButton("Browse...")
        mysql_browse.clicked.connect(lambda: self.browse_for_tool("mysqldump"))
        mysql_layout.addWidget(mysql_browse)
        tool_path_layout.addLayout(mysql_layout)
        
        # Apply button
        apply_paths_button = QPushButton("Apply Paths")
        apply_paths_button.clicked.connect(self.apply_manual_paths)
        tool_path_layout.addWidget(apply_paths_button)
        
        tool_path_group.setLayout(tool_path_layout)
        layout.addWidget(tool_path_group)
        
        # Buttons
        button_layout = QHBoxLayout()
        self.connect_button = QPushButton("Connect")
        self.connect_button.clicked.connect(self.connect_to_db)
        button_layout.addWidget(self.connect_button)
        
        self.logout_button = QPushButton("Logout")
        self.logout_button.clicked.connect(self.logout_from_db)
        self.logout_button.setEnabled(False)
        button_layout.addWidget(self.logout_button)
        
        self.save_config_button = QPushButton("Save Configuration")
        self.save_config_button.clicked.connect(self.save_config)
        button_layout.addWidget(self.save_config_button)
        
        layout.addLayout(button_layout)
        
        # Connection status
        self.connection_status = QLabel("Not connected")
        self.connection_status.setAlignment(Qt.AlignCenter)
        layout.addWidget(self.connection_status)
        
        # Tool paths status
        self.tools_status = QLabel("")
        self.tools_status.setAlignment(Qt.AlignCenter)
        layout.addWidget(self.tools_status)
        self.update_tools_status()
        
        # Service Controls (Windows only)
        if platform.system() == 'Windows':
            service_tab = QTabWidget()
            
            # PostgreSQL Service Control
            pg_service_group = QGroupBox("PostgreSQL Service Control")
            pg_service_layout = QVBoxLayout()
            
            pg_service_name_layout = QHBoxLayout()
            pg_service_name_layout.addWidget(QLabel("Service Name:"))
            self.pg_service_name_input = QLineEdit()
            self.pg_service_name_input.setPlaceholderText("Leave blank for auto-detection")
            pg_service_name_layout.addWidget(self.pg_service_name_input)
            pg_service_layout.addLayout(pg_service_name_layout)
            
            self.postgres_status_label = QLabel("Service status: Checking...")
            pg_service_layout.addWidget(self.postgres_status_label)
            
            # PostgreSQL service buttons
            pg_button_layout = QHBoxLayout()
            
            self.start_postgres_button = QPushButton("Start PostgreSQL")
            self.start_postgres_button.clicked.connect(self.start_postgresql_service)
            self.start_postgres_button.setEnabled(False)
            pg_button_layout.addWidget(self.start_postgres_button)
            
            self.stop_postgres_button = QPushButton("Stop PostgreSQL")
            self.stop_postgres_button.clicked.connect(self.stop_postgresql_service)
            self.stop_postgres_button.setEnabled(False)
            pg_button_layout.addWidget(self.stop_postgres_button)
            
            self.restart_postgres_button = QPushButton("Restart PostgreSQL")
            self.restart_postgres_button.clicked.connect(self.restart_postgresql_service)
            self.restart_postgres_button.setEnabled(False)
            pg_button_layout.addWidget(self.restart_postgres_button)
            
            pg_service_layout.addLayout(pg_button_layout)
            pg_service_group.setLayout(pg_service_layout)
            service_tab.addTab(pg_service_group, "PostgreSQL")
            
            # MySQL Service Control
            mysql_service_group = QGroupBox("MySQL Service Control")
            mysql_service_layout = QVBoxLayout()
            
            mysql_service_name_layout = QHBoxLayout()
            mysql_service_name_layout.addWidget(QLabel("Service Name:"))
            self.mysql_service_name_input = QLineEdit()
            self.mysql_service_name_input.setPlaceholderText("Leave blank for auto-detection")
            mysql_service_name_layout.addWidget(self.mysql_service_name_input)
            mysql_service_layout.addLayout(mysql_service_name_layout)
            
            self.mysql_status_label = QLabel("Service status: Checking...")
            mysql_service_layout.addWidget(self.mysql_status_label)
            
            # MySQL service buttons
            mysql_button_layout = QHBoxLayout()
            
            self.start_mysql_button = QPushButton("Start MySQL")
            self.start_mysql_button.clicked.connect(self.start_mysql_service)
            self.start_mysql_button.setEnabled(False)
            mysql_button_layout.addWidget(self.start_mysql_button)
            
            self.stop_mysql_button = QPushButton("Stop MySQL")
            self.stop_mysql_button.clicked.connect(self.stop_mysql_service)
            self.stop_mysql_button.setEnabled(False)
            mysql_button_layout.addWidget(self.stop_mysql_button)
            
            self.restart_mysql_button = QPushButton("Restart MySQL")
            self.restart_mysql_button.clicked.connect(self.restart_mysql_service)
            self.restart_mysql_button.setEnabled(False)
            mysql_button_layout.addWidget(self.restart_mysql_button)
            
            mysql_service_layout.addLayout(mysql_button_layout)
            mysql_service_group.setLayout(mysql_service_layout)
            service_tab.addTab(mysql_service_group, "MySQL")
            
            layout.addWidget(service_tab)
            
            # Update service statuses
            QTimer.singleShot(100, self.update_postgres_service_status)
            QTimer.singleShot(100, self.update_mysql_service_status)
        else:
            service_label = QLabel("Service control: Windows only")
            service_label.setAlignment(Qt.AlignCenter)
            layout.addWidget(service_label)
        
        layout.addStretch()
    
    def update_mysql_service_status(self):
        """Check MySQL service status with better error handling"""
        if platform.system() != 'Windows':
            self.mysql_status_label.setText("Service control: Windows only")
            self.start_mysql_button.setEnabled(False)
            self.stop_mysql_button.setEnabled(False)
            self.restart_mysql_button.setEnabled(False)
            return
            
        try:
            service_name = self.mysql_service_name_input.text().strip()
            
            if not service_name:
                # Try to auto-detect service
                try:
                    scm = win32service.OpenSCManager(
                        None, None, 
                        win32service.SC_MANAGER_CONNECT | win32service.SC_MANAGER_ENUMERATE_SERVICE
                    )
                    services = win32service.EnumServicesStatus(
                        scm, 
                        win32service.SERVICE_WIN32, 
                        win32service.SERVICE_STATE_ALL
                    )
                    
                    mysql_services = []
                    for service in services:
                        name, display_name, _ = service
                        if "mysql" in name.lower() or "mysql" in display_name.lower():
                            mysql_services.append((name, display_name))
                    
                    if mysql_services:
                        # Prefer exact "MySQL" service name
                        service_name = next((name for name, _ in mysql_services if name.lower() == "mysql"), mysql_services[0][0])
                        self.current_mysql_service = service_name
                        
                except win32api.error as e:
                    if e.winerror == 5:  # Access Denied
                        # Try WMI as fallback
                        if wmi:
                            try:
                                c = wmi.WMI()
                                services = c.Win32_Service()
                                
                                mysql_services = []
                                for service in services:
                                    if "mysql" in service.Name.lower() or "mysql" in service.DisplayName.lower():
                                        mysql_services.append(service)
                                        
                                if mysql_services:
                                    # Find the most likely MySQL service
                                    best_match = None
                                    for service in mysql_services:
                                        if "server" in service.DisplayName.lower():
                                            best_match = service
                                            break
                                            
                                    if not best_match and mysql_services:
                                        best_match = mysql_services[0]
                                        
                                    if best_match:
                                        service_name = best_match.Name
                                        self.current_mysql_service = service_name
                            except:
                                pass  # Couldn't use WMI either
                    else:
                        raise  # Re-raise other errors
            
            if service_name:
                try:
                    # Try to open service with minimum required access
                    scm = win32service.OpenSCManager(
                        None, None, 
                        win32service.SC_MANAGER_CONNECT
                    )
                    service_handle = win32service.OpenService(
                        scm,
                        service_name,
                        win32service.SERVICE_QUERY_STATUS
                    )
                    
                    status = win32service.QueryServiceStatus(service_handle)
                    win32service.CloseServiceHandle(service_handle)
                    
                    if status[1] == win32service.SERVICE_RUNNING:
                        status_text = "Running"
                        self.start_mysql_button.setEnabled(False)
                        self.stop_mysql_button.setEnabled(True)
                        self.restart_mysql_button.setEnabled(True)
                    else:
                        status_text = "Stopped"
                        self.start_mysql_button.setEnabled(True)
                        self.stop_mysql_button.setEnabled(False)
                        self.restart_mysql_button.setEnabled(False)
                    
                    self.mysql_status_label.setText(f"Service: {status_text} ({service_name})")
                    
                except win32api.error as e:
                    if e.winerror == 5:  # Access Denied
                        self.mysql_status_label.setText(f"Access denied to service: {service_name}")
                        self.start_mysql_button.setEnabled(False)
                        self.stop_mysql_button.setEnabled(False)
                        self.restart_mysql_button.setEnabled(False)
                    else:
                        raise
                        
            else:
                self.mysql_status_label.setText("MySQL service not found")
                self.start_mysql_button.setEnabled(False)
                self.stop_mysql_button.setEnabled(False)
                self.restart_mysql_button.setEnabled(False)
                self.current_mysql_service = None
                
        except Exception as e:
            error_msg = str(e)
            if hasattr(e, 'winerror'):
                if e.winerror == 5:
                    error_msg = "Access denied (run as administrator)"
                elif e.winerror == 1060:
                    error_msg = "Service does not exist"
                    
            self.mysql_status_label.setText(f"Service error: {error_msg}")
            self.start_mysql_button.setEnabled(False)
            self.stop_mysql_button.setEnabled(False)
            self.restart_mysql_button.setEnabled(False)

    def start_mysql_service(self):
        """Start the MySQL service"""
        if platform.system() != 'Windows':
            QMessageBox.information(self, "Not Supported", "Service control is only available on Windows")
            return
            
        service_name = self.mysql_service_name_input.text().strip() or self.current_mysql_service
        if not service_name:
            QMessageBox.warning(self, "Error", "MySQL service not detected")
            return
            
        try:
            # Disconnect if currently connected to MySQL
            if self.connection and self.current_db_type == "MySQL":
                self.logout_from_db()
            
            # Try to start with minimal privileges first
            try:
                info = subprocess.STARTUPINFO()
                info.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                info.wShowWindow = subprocess.SW_HIDE
                
                proc = subprocess.Popen(
                    ['net', 'start', service_name],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    startupinfo=info
                )
                self.background_processes.append(proc)
                proc.wait(timeout=30)
                
            except subprocess.TimeoutExpired:
                QMessageBox.warning(self, "Timeout", "Service start timed out")
                return
            except Exception as net_error:
                # Fall back to win32serviceutil if net commands fail
                try:
                    scm = win32service.OpenSCManager(
                        None, None, 
                        win32service.SC_MANAGER_CONNECT
                    )
                    service_handle = win32service.OpenService(
                        scm,
                        service_name,
                        win32service.SERVICE_START | win32service.SERVICE_QUERY_STATUS
                    )
                    
                    win32service.StartService(service_handle, None)
                    win32service.CloseServiceHandle(service_handle)
                    
                except win32api.error as e:
                    if e.winerror == 5:  # Access Denied
                        QMessageBox.critical(
                            self, "Access Denied",
                            "Failed to start service: Access denied\n\n"
                            "Please run this application as Administrator to manage services."
                        )
                        return
                    raise
                
            # Update status after start
            time.sleep(2)  # Give it time to start
            self.update_mysql_service_status()
            
            QMessageBox.information(
                self, "Success",
                f"MySQL service started successfully\n({service_name})"
            )
            
        except Exception as e:
            error_msg = str(e)
            if hasattr(e, 'winerror'):
                if e.winerror == 1056:
                    error_msg = "Service already running"
                elif e.winerror == 1058:
                    error_msg = "Service is disabled"
                    
            QMessageBox.critical(
                self, "Error",
                f"Failed to start MySQL:\n{error_msg}\n"
                "You may need to start the service manually."
            )
            self.update_mysql_service_status()

    def stop_mysql_service(self):
        """Stop the MySQL service"""
        if platform.system() != 'Windows':
            QMessageBox.information(self, "Not Supported", "Service control is only available on Windows")
            return
            
        service_name = self.mysql_service_name_input.text().strip() or self.current_mysql_service
        if not service_name:
            QMessageBox.warning(self, "Error", "MySQL service not detected")
            return
            
        reply = QMessageBox.question(
            self, "Confirm Stop",
            f"Stop MySQL service?\n\nService: {service_name}\nThis will disconnect all active connections.",
            QMessageBox.Yes | QMessageBox.No
        )
        
        if reply != QMessageBox.Yes:
            return
            
        try:
            # Disconnect if currently connected to MySQL
            if self.connection and self.current_db_type == "MySQL":
                self.logout_from_db()
            
            # Try to stop with minimal privileges first
            try:
                info = subprocess.STARTUPINFO()
                info.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                info.wShowWindow = subprocess.SW_HIDE
                
                proc = subprocess.Popen(
                    ['net', 'stop', service_name],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    startupinfo=info
                )
                self.background_processes.append(proc)
                proc.wait(timeout=30)
                
            except subprocess.TimeoutExpired:
                QMessageBox.warning(self, "Timeout", "Service stop timed out")
                return
            except Exception as net_error:
                # Fall back to win32serviceutil if net commands fail
                try:
                    scm = win32service.OpenSCManager(
                        None, None, 
                        win32service.SC_MANAGER_CONNECT
                    )
                    service_handle = win32service.OpenService(
                        scm,
                        service_name,
                        win32service.SERVICE_STOP | win32service.SERVICE_QUERY_STATUS
                    )
                    
                    win32service.ControlService(service_handle, win32service.SERVICE_CONTROL_STOP)
                    
                    # Wait for service to stop
                    for _ in range(10):
                        status = win32service.QueryServiceStatus(service_handle)
                        if status[1] == win32service.SERVICE_STOPPED:
                            break
                        time.sleep(1)
                    
                    win32service.CloseServiceHandle(service_handle)
                    
                except win32api.error as e:
                    if e.winerror == 5:  # Access Denied
                        QMessageBox.critical(
                            self, "Access Denied",
                            "Failed to stop service: Access denied\n\n"
                            "Please run this application as Administrator to manage services."
                        )
                        return
                    raise
                
            # Update status after stop
            time.sleep(1)  # Give it a moment to stop
            self.update_mysql_service_status()
            
            QMessageBox.information(
                self, "Success",
                f"MySQL service stopped successfully\n({service_name})"
            )
            
        except Exception as e:
            error_msg = str(e)
            if hasattr(e, 'winerror'):
                if e.winerror == 1062:
                    error_msg = "Service not running"
                    
            QMessageBox.critical(
                self, "Error",
                f"Failed to stop MySQL:\n{error_msg}\n"
                "You may need to stop the service manually."
            )
            self.update_mysql_service_status()

    def restart_mysql_service(self):
        """Enhanced MySQL service restart with better process tracking"""
        if platform.system() != 'Windows':
            QMessageBox.information(self, "Not Supported", "Service restart is only available on Windows")
            return
            
        service_name = self.mysql_service_name_input.text().strip() or self.current_mysql_service
        if not service_name:
            QMessageBox.warning(self, "Error", "MySQL service not detected")
            return
            
        reply = QMessageBox.question(
            self, "Confirm Restart",
            f"Restart MySQL service?\n\nService: {service_name}\nThis will disconnect all active connections.",
            QMessageBox.Yes | QMessageBox.No
        )
        
        if reply == QMessageBox.Yes:
            try:
                # Disconnect if currently connected to MySQL
                if self.connection and self.current_db_type == "MySQL":
                    self.logout_from_db()
                
                # Try to restart with minimal privileges first
                try:
                    # Start the restart in a separate process
                    info = subprocess.STARTUPINFO()
                    info.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                    info.wShowWindow = subprocess.SW_HIDE
                    
                    proc = subprocess.Popen(
                        ['net', 'stop', service_name],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        startupinfo=info
                    )
                    self.background_processes.append(proc)
                    proc.wait(timeout=30)
                    
                    time.sleep(2)  # Give it a moment to stop
                    
                    proc = subprocess.Popen(
                        ['net', 'start', service_name],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        startupinfo=info
                    )
                    self.background_processes.append(proc)
                    proc.wait(timeout=30)
                    
                except subprocess.TimeoutExpired:
                    QMessageBox.warning(self, "Timeout", "Service restart timed out")
                    return
                except Exception as net_error:
                    # Fall back to win32serviceutil if net commands fail
                    try:
                        # Use win32serviceutil with minimal privileges
                        scm = win32service.OpenSCManager(
                            None, None, 
                            win32service.SC_MANAGER_CONNECT
                        )
                        service_handle = win32service.OpenService(
                            scm,
                            service_name,
                            win32service.SERVICE_STOP | win32service.SERVICE_START | win32service.SERVICE_QUERY_STATUS
                        )
                        
                        # Stop the service
                        win32service.ControlService(service_handle, win32service.SERVICE_CONTROL_STOP)
                        
                        # Wait for service to stop
                        for _ in range(10):
                            status = win32service.QueryServiceStatus(service_handle)
                            if status[1] == win32service.SERVICE_STOPPED:
                                break
                            time.sleep(1)
                        
                        # Start the service
                        win32service.StartService(service_handle, None)
                        win32service.CloseServiceHandle(service_handle)
                        
                    except win32api.error as e:
                        if e.winerror == 5:  # Access Denied
                            QMessageBox.critical(
                                self, "Access Denied",
                                "Failed to restart service: Access denied\n\n"
                                "Please run this application as Administrator to manage services."
                            )
                            return
                        raise
                
                # Update status after restart
                time.sleep(2)  # Give it time to start
                self.update_mysql_service_status()
                
                QMessageBox.information(
                    self, "Success",
                    f"MySQL service restarted successfully\n({service_name})"
                )
                
            except Exception as e:
                error_msg = str(e)
                if hasattr(e, 'winerror'):
                    if e.winerror == 5:
                        error_msg = "Access denied (run as administrator)"
                    elif e.winerror == 1056:
                        error_msg = "Service already running"
                    elif e.winerror == 1062:
                        error_msg = "Service not started"
                        
                QMessageBox.critical(
                    self, "Error",
                    f"Failed to restart MySQL:\n{error_msg}\n"
                    "You may need to restart the service manually."
                )
                self.update_mysql_service_status()

    def update_postgres_service_status(self):
        """Improved PostgreSQL service detection with better error handling"""
        if platform.system() != 'Windows':
            self.postgres_status_label.setText("Service control: Windows only")
            self.start_postgres_button.setEnabled(False)
            self.stop_postgres_button.setEnabled(False)
            self.restart_postgres_button.setEnabled(False)
            return
            
        try:
            service_name = self.pg_service_name_input.text().strip()
            
            if not service_name:
                # Try to auto-detect service
                try:
                    scm = win32service.OpenSCManager(
                        None, None, 
                        win32service.SC_MANAGER_CONNECT | win32service.SC_MANAGER_ENUMERATE_SERVICE
                    )
                    services = win32service.EnumServicesStatus(
                        scm, 
                        win32service.SERVICE_WIN32, 
                        win32service.SERVICE_STATE_ALL
                    )
                    
                    postgres_services = []
                    search_terms = ['postgres', 'pgsql', 'postgre', 'pg_']
                    
                    for service in services:
                        name, display_name, status = service
                        if any(term in name.lower() or term in display_name.lower() for term in search_terms):
                            postgres_services.append((name, display_name, status))
                    
                    if postgres_services:
                        # Find the best matching service
                        best_match = None
                        version_numbers = []
                        
                        for service in postgres_services:
                            name, display_name, status = service
                            # Extract version numbers from name
                            numbers = [int(s) for s in name.split() if s.isdigit()]
                            if numbers:
                                version_numbers.append((max(numbers), service))
                        
                        if version_numbers:
                            version_numbers.sort()
                            best_match = version_numbers[-1][1]
                        else:
                            for service in postgres_services:
                                if 'server' in service[1].lower():
                                    best_match = service
                                    break
                            if not best_match:
                                best_match = postgres_services[0]
                        
                        service_name = best_match[0]
                        self.current_postgres_service = service_name
                        
                except win32api.error as e:
                    if e.winerror == 5:  # Access Denied
                        # Try WMI as fallback
                        if wmi:
                            try:
                                c = wmi.WMI()
                                services = c.Win32_Service()
                                
                                postgres_services = []
                                for service in services:
                                    if "postgres" in service.Name.lower() or "postgres" in service.DisplayName.lower():
                                        postgres_services.append(service)
                                        
                                if postgres_services:
                                    # Find the most likely PostgreSQL service
                                    best_match = None
                                    for service in postgres_services:
                                        if "server" in service.DisplayName.lower():
                                            best_match = service
                                            break
                                            
                                    if not best_match and postgres_services:
                                        best_match = postgres_services[0]
                                        
                                    if best_match:
                                        service_name = best_match.Name
                                        self.current_postgres_service = service_name
                            except:
                                pass  # Couldn't use WMI either
                    else:
                        raise  # Re-raise other errors
            
            if service_name:
                try:
                    # Try to open service with minimum required access
                    scm = win32service.OpenSCManager(
                        None, None, 
                        win32service.SC_MANAGER_CONNECT
                    )
                    service_handle = win32service.OpenService(
                        scm,
                        service_name,
                        win32service.SERVICE_QUERY_STATUS
                    )
                    
                    status = win32service.QueryServiceStatus(service_handle)
                    win32service.CloseServiceHandle(service_handle)
                    
                    if status[1] == win32service.SERVICE_RUNNING:
                        status_text = "Running"
                        self.start_postgres_button.setEnabled(False)
                        self.stop_postgres_button.setEnabled(True)
                        self.restart_postgres_button.setEnabled(True)
                    else:
                        status_text = "Stopped"
                        self.start_postgres_button.setEnabled(True)
                        self.stop_postgres_button.setEnabled(False)
                        self.restart_postgres_button.setEnabled(False)
                    
                    self.postgres_status_label.setText(f"Service: {status_text} ({service_name})")
                    
                except win32api.error as e:
                    if e.winerror == 5:  # Access Denied
                        self.postgres_status_label.setText(f"Access denied to service: {service_name}")
                        self.start_postgres_button.setEnabled(False)
                        self.stop_postgres_button.setEnabled(False)
                        self.restart_postgres_button.setEnabled(False)
                    else:
                        raise
                        
            else:
                self.postgres_status_label.setText("PostgreSQL service not found")
                self.start_postgres_button.setEnabled(False)
                self.stop_postgres_button.setEnabled(False)
                self.restart_postgres_button.setEnabled(False)
                self.current_postgres_service = None
                
        except Exception as e:
            error_msg = str(e)
            if hasattr(e, 'winerror'):
                if e.winerror == 5:
                    error_msg = "Access denied (run as administrator)"
                elif e.winerror == 1060:
                    error_msg = "Service does not exist"
                    
            self.postgres_status_label.setText(f"Service error: {error_msg}")
            self.start_postgres_button.setEnabled(False)
            self.stop_postgres_button.setEnabled(False)
            self.restart_postgres_button.setEnabled(False)
            self.current_postgres_service = None

    def start_postgresql_service(self):
        """Start the PostgreSQL service"""
        if platform.system() != 'Windows':
            QMessageBox.information(self, "Not Supported", "Service control is only available on Windows")
            return
            
        service_name = self.pg_service_name_input.text().strip() or self.current_postgres_service
        if not service_name:
            QMessageBox.warning(self, "Error", "PostgreSQL service not detected")
            return
            
        try:
            # Disconnect if currently connected to PostgreSQL
            if self.connection and self.current_db_type == "PostgreSQL":
                self.logout_from_db()
            
            # Try to start with minimal privileges first
            try:
                info = subprocess.STARTUPINFO()
                info.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                info.wShowWindow = subprocess.SW_HIDE
                
                proc = subprocess.Popen(
                    ['net', 'start', service_name],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    startupinfo=info
                )
                self.background_processes.append(proc)
                proc.wait(timeout=30)
                
            except subprocess.TimeoutExpired:
                QMessageBox.warning(self, "Timeout", "Service start timed out")
                return
            except Exception as net_error:
                # Fall back to win32serviceutil if net commands fail
                try:
                    scm = win32service.OpenSCManager(
                        None, None, 
                        win32service.SC_MANAGER_CONNECT
                    )
                    service_handle = win32service.OpenService(
                        scm,
                        service_name,
                        win32service.SERVICE_START | win32service.SERVICE_QUERY_STATUS
                    )
                    
                    win32service.StartService(service_handle, None)
                    win32service.CloseServiceHandle(service_handle)
                    
                except win32api.error as e:
                    if e.winerror == 5:  # Access Denied
                        QMessageBox.critical(
                            self, "Access Denied",
                            "Failed to start service: Access denied\n\n"
                            "Please run this application as Administrator to manage services."
                        )
                        return
                    raise
                
            # Update status after start
            time.sleep(2)  # Give it time to start
            self.update_postgres_service_status()
            
            QMessageBox.information(
                self, "Success",
                f"PostgreSQL service started successfully\n({service_name})"
            )
            
        except Exception as e:
            error_msg = str(e)
            if hasattr(e, 'winerror'):
                if e.winerror == 1056:
                    error_msg = "Service already running"
                elif e.winerror == 1058:
                    error_msg = "Service is disabled"
                    
            QMessageBox.critical(
                self, "Error",
                f"Failed to start PostgreSQL:\n{error_msg}\n"
                "You may need to start the service manually."
            )
            self.update_postgres_service_status()

    def stop_postgresql_service(self):
        """Stop the PostgreSQL service"""
        if platform.system() != 'Windows':
            QMessageBox.information(self, "Not Supported", "Service control is only available on Windows")
            return
            
        service_name = self.pg_service_name_input.text().strip() or self.current_postgres_service
        if not service_name:
            QMessageBox.warning(self, "Error", "PostgreSQL service not detected")
            return
            
        reply = QMessageBox.question(
            self, "Confirm Stop",
            f"Stop PostgreSQL service?\n\nService: {service_name}\nThis will disconnect all active connections.",
            QMessageBox.Yes | QMessageBox.No
        )
        
        if reply != QMessageBox.Yes:
            return
            
        try:
            # Disconnect if currently connected to PostgreSQL
            if self.connection and self.current_db_type == "PostgreSQL":
                self.logout_from_db()
            
            # Try to stop with minimal privileges first
            try:
                info = subprocess.STARTUPINFO()
                info.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                info.wShowWindow = subprocess.SW_HIDE
                
                proc = subprocess.Popen(
                    ['net', 'stop', service_name],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    startupinfo=info
                )
                self.background_processes.append(proc)
                proc.wait(timeout=30)
                
            except subprocess.TimeoutExpired:
                QMessageBox.warning(self, "Timeout", "Service stop timed out")
                return
            except Exception as net_error:
                # Fall back to win32serviceutil if net commands fail
                try:
                    scm = win32service.OpenSCManager(
                        None, None, 
                        win32service.SC_MANAGER_CONNECT
                    )
                    service_handle = win32service.OpenService(
                        scm,
                        service_name,
                        win32service.SERVICE_STOP | win32service.SERVICE_QUERY_STATUS
                    )
                    
                    win32service.ControlService(service_handle, win32service.SERVICE_CONTROL_STOP)
                    
                    # Wait for service to stop
                    for _ in range(10):
                        status = win32service.QueryServiceStatus(service_handle)
                        if status[1] == win32service.SERVICE_STOPPED:
                            break
                        time.sleep(1)
                    
                    win32service.CloseServiceHandle(service_handle)
                    
                except win32api.error as e:
                    if e.winerror == 5:  # Access Denied
                        QMessageBox.critical(
                            self, "Access Denied",
                            "Failed to stop service: Access denied\n\n"
                            "Please run this application as Administrator to manage services."
                        )
                        return
                    raise
                
            # Update status after stop
            time.sleep(1)  # Give it a moment to stop
            self.update_postgres_service_status()
            
            QMessageBox.information(
                self, "Success",
                f"PostgreSQL service stopped successfully\n({service_name})"
            )
            
        except Exception as e:
            error_msg = str(e)
            if hasattr(e, 'winerror'):
                if e.winerror == 1062:
                    error_msg = "Service not running"
                    
            QMessageBox.critical(
                self, "Error",
                f"Failed to stop PostgreSQL:\n{error_msg}\n"
                "You may need to stop the service manually."
            )
            self.update_postgres_service_status()

    def restart_postgresql_service(self):
        """Enhanced service restart with better process tracking"""
        if platform.system() != 'Windows':
            QMessageBox.information(self, "Not Supported", "Service restart is only available on Windows")
            return
            
        service_name = self.pg_service_name_input.text().strip() or self.current_postgres_service
        if not service_name:
            QMessageBox.warning(self, "Error", "PostgreSQL service not detected")
            return
            
        reply = QMessageBox.question(
            self, "Confirm Restart",
            f"Restart PostgreSQL service?\n\n"
            f"Service: {service_name}\n"
            "This will disconnect all active connections.",
            QMessageBox.Yes | QMessageBox.No
        )
        
        if reply == QMessageBox.Yes:
            try:
                # Disconnect if currently connected to PostgreSQL
                if self.connection and self.current_db_type == "PostgreSQL":
                    self.logout_from_db()
                
                # Try to restart with minimal privileges first
                try:
                    # Start the restart in a separate process
                    info = subprocess.STARTUPINFO()
                    info.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                    info.wShowWindow = subprocess.SW_HIDE
                    
                    proc = subprocess.Popen(
                        ['net', 'stop', service_name],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        startupinfo=info
                    )
                    self.background_processes.append(proc)
                    proc.wait(timeout=30)
                    
                    time.sleep(2)  # Give it a moment to stop
                    
                    proc = subprocess.Popen(
                        ['net', 'start', service_name],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        startupinfo=info
                    )
                    self.background_processes.append(proc)
                    proc.wait(timeout=30)
                    
                except subprocess.TimeoutExpired:
                    QMessageBox.warning(self, "Timeout", "Service restart timed out")
                    return
                except Exception as net_error:
                    # Fall back to win32serviceutil if net commands fail
                    try:
                        # Use win32serviceutil with minimal privileges
                        scm = win32service.OpenSCManager(
                            None, None, 
                            win32service.SC_MANAGER_CONNECT
                        )
                        service_handle = win32service.OpenService(
                            scm,
                            service_name,
                            win32service.SERVICE_STOP | win32service.SERVICE_START | win32service.SERVICE_QUERY_STATUS
                        )
                        
                        # Stop the service
                        win32service.ControlService(service_handle, win32service.SERVICE_CONTROL_STOP)
                        
                        # Wait for service to stop
                        for _ in range(10):
                            status = win32service.QueryServiceStatus(service_handle)
                            if status[1] == win32service.SERVICE_STOPPED:
                                break
                            time.sleep(1)
                        
                        # Start the service
                        win32service.StartService(service_handle, None)
                        win32service.CloseServiceHandle(service_handle)
                        
                    except win32api.error as e:
                        if e.winerror == 5:  # Access Denied
                            QMessageBox.critical(
                                self, "Access Denied",
                                "Failed to restart service: Access denied\n\n"
                                "Please run this application as Administrator to manage services."
                            )
                            return
                        raise
                
                # Update status after restart
                time.sleep(2)  # Give it time to start
                self.update_postgres_service_status()
                
                QMessageBox.information(
                    self, "Success",
                    f"PostgreSQL service restarted successfully\n"
                    f"({service_name})"
                )
                
            except Exception as e:
                error_msg = str(e)
                if hasattr(e, 'winerror'):
                    if e.winerror == 5:
                        error_msg = "Access denied (run as administrator)"
                    elif e.winerror == 1056:
                        error_msg = "Service already running"
                    elif e.winerror == 1062:
                        error_msg = "Service not started"
                        
                QMessageBox.critical(
                    self, "Error",
                    f"Failed to restart PostgreSQL:\n{error_msg}\n"
                    "You may need to restart the service manually."
                )
                self.update_postgres_service_status()

    def setup_backup_tab(self, tab):
        layout = QVBoxLayout(tab)
        
        # Backup section
        backup_group = QGroupBox("Create Backup")
        backup_layout = QVBoxLayout()
        
        # Backup format selection
        format_layout = QHBoxLayout()
        format_layout.addWidget(QLabel("Backup Format:"))
        self.backup_format_combo = QComboBox()
        self.backup_format_combo.addItems(["SQL", "CSV"])
        format_layout.addWidget(self.backup_format_combo)
        backup_layout.addLayout(format_layout)
        
        # Backup location
        location_layout = QHBoxLayout()
        location_layout.addWidget(QLabel("Backup Location:"))
        self.backup_location_input = QLineEdit()
        self.backup_location_input.setPlaceholderText("Select a directory")
        location_layout.addWidget(self.backup_location_input)
        browse_button = QPushButton("Browse...")
        browse_button.clicked.connect(self.select_backup_directory)
        location_layout.addWidget(browse_button)
        backup_layout.addLayout(location_layout)
        
        # Backup button
        self.backup_button = QPushButton("Create Backup")
        self.backup_button.clicked.connect(self.create_backup)
        self.backup_button.setEnabled(False)
        backup_layout.addWidget(self.backup_button)
        
        # Scheduled backup section
        schedule_group = QGroupBox("Scheduled Backups")
        schedule_layout = QVBoxLayout()
        
        # Schedule controls
        controls_layout = QHBoxLayout()
        controls_layout.addWidget(QLabel("Schedule:"))
        
        self.schedule_combo = QComboBox()
        self.schedule_combo.addItems([
            "Disabled",
            "Every 1 hour",
            "Every 6 hours",
            "Every 12 hours",
            "Daily at midnight",
            "Weekly on Sunday"
        ])
        controls_layout.addWidget(self.schedule_combo)
        
        self.enable_schedule_button = QPushButton("Enable Schedule")
        self.enable_schedule_button.clicked.connect(self.toggle_scheduled_backups)
        controls_layout.addWidget(self.enable_schedule_button)
        
        schedule_layout.addLayout(controls_layout)
        
        # Next backup time
        self.next_backup_label = QLabel("Next backup: Not scheduled")
        schedule_layout.addWidget(self.next_backup_label)
        
        schedule_group.setLayout(schedule_layout)
        backup_layout.addWidget(schedule_group)
        
        backup_group.setLayout(backup_layout)
        layout.addWidget(backup_group)
        
        # Restore section
        restore_group = QGroupBox("Restore Backup")
        restore_layout = QVBoxLayout()
        
        # Backup list
        self.backup_list = QListWidget()
        restore_layout.addWidget(self.backup_list)
        
        # Refresh button
        refresh_button = QPushButton("Refresh Backups")
        refresh_button.clicked.connect(self.refresh_backup_list)
        restore_layout.addWidget(refresh_button)
        
        # Restore button
        self.restore_button = QPushButton("Restore Selected Backup")
        self.restore_button.clicked.connect(self.restore_backup)
        self.restore_button.setEnabled(False)
        restore_layout.addWidget(self.restore_button)
        
        restore_group.setLayout(restore_layout)
        layout.addWidget(restore_group)
        
        # Connect signals
        self.backup_list.itemSelectionChanged.connect(self.toggle_restore_button)
    
    def setup_user_tab(self, tab):
        layout = QVBoxLayout(tab)
        
        # User Operations Group
        operations_group = QGroupBox("User Operations")
        operations_layout = QVBoxLayout()
        
        # Operation Selection
        op_layout = QHBoxLayout()
        op_layout.addWidget(QLabel("Operation:"))
        self.user_op_combo = QComboBox()
        self.user_op_combo.addItems(["Create User", "Modify Users", "Delete Users"])
        op_layout.addWidget(self.user_op_combo)
        operations_layout.addLayout(op_layout)
        
        # Username input (visible only for Create User)
        self.username_input_layout = QHBoxLayout()
        self.username_input_layout.addWidget(QLabel("Username:"))
        self.username_input = QLineEdit()
        self.username_input.setPlaceholderText("Enter new username")
        self.username_input_layout.addWidget(self.username_input)
        operations_layout.addLayout(self.username_input_layout)
        
        # Privilege Selection (for create/modify)
        self.privilege_group = QGroupBox("Privileges")
        privilege_layout = QVBoxLayout()
        
        # PostgreSQL specific privileges
        self.pg_privs = {
            'Login': 'LOGIN',
            'Superuser': 'SUPERUSER',
            'Create DB': 'CREATEDB',
            'Create Role': 'CREATEROLE',
            'Replication': 'REPLICATION'
        }
        
        # MySQL specific privileges
        self.mysql_privs = {
            'All Privileges': 'ALL PRIVILEGES',
            'Create': 'CREATE',
            'Alter': 'ALTER',
            'Drop': 'DROP',
            'Insert': 'INSERT',
            'Select': 'SELECT',
            'Update': 'UPDATE',
            'Delete': 'DELETE'
        }
        
        self.privilege_checkboxes = {}
        privilege_grid = QHBoxLayout()
        
        # PostgreSQL privileges column
        pg_col = QVBoxLayout()
        pg_col.addWidget(QLabel("PostgreSQL:"))
        for name, priv in self.pg_privs.items():
            cb = QCheckBox(name)
            self.privilege_checkboxes[priv] = cb
            pg_col.addWidget(cb)
        privilege_grid.addLayout(pg_col)
        
        # MySQL privileges column
        mysql_col = QVBoxLayout()
        mysql_col.addWidget(QLabel("MySQL:"))
        for name, priv in self.mysql_privs.items():
            cb = QCheckBox(name)
            self.privilege_checkboxes[priv] = cb
            mysql_col.addWidget(cb)
        privilege_grid.addLayout(mysql_col)
        
        privilege_layout.addLayout(privilege_grid)
        self.privilege_group.setLayout(privilege_layout)
        operations_layout.addWidget(self.privilege_group)
        
        # Password for new users
        self.user_password_input = QLineEdit()
        self.user_password_input.setPlaceholderText("Password for new user")
        self.user_password_input.setEchoMode(QLineEdit.Password)
        operations_layout.addWidget(self.user_password_input)
        
        # Execute button
        self.execute_user_op_button = QPushButton("Execute Operation")
        self.execute_user_op_button.clicked.connect(self.execute_user_operation)
        operations_layout.addWidget(self.execute_user_op_button)
        
        operations_group.setLayout(operations_layout)
        layout.addWidget(operations_group)
        
        # User Table
        self.user_table = QTableWidget()
        self.user_table.setColumnCount(4)
        self.user_table.setHorizontalHeaderLabels(["Username", "Can Login", "Superuser", "Other Privileges"])
        self.user_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        
        # Load Users button
        self.load_users_button = QPushButton("Load Users")
        self.load_users_button.clicked.connect(self.load_users)
        operations_layout.addWidget(self.load_users_button)
        
        layout.addWidget(self.user_table)
        
        # Connect signals
        self.user_op_combo.currentTextChanged.connect(self.update_ui_for_operation)
        self.update_ui_for_operation()
    
    def update_ui_for_operation(self):
        op = self.user_op_combo.currentText()
        
        if op == "Create User":
            self.username_input_layout.setEnabled(True)
            self.username_input.setEnabled(True)
            self.privilege_group.setEnabled(True)
            self.user_password_input.setEnabled(True)
            self.user_password_input.setPlaceholderText("Password for new user")
            self.user_table.setEnabled(False)
        elif op == "Modify Users":
            self.username_input_layout.setEnabled(False)
            self.username_input.setEnabled(False)
            self.privilege_group.setEnabled(True)
            self.user_password_input.setEnabled(False)
            self.user_password_input.setPlaceholderText("(Password unchanged)")
            self.user_table.setEnabled(True)
        else:  # Delete Users
            self.username_input_layout.setEnabled(False)
            self.username_input.setEnabled(False)
            self.privilege_group.setEnabled(False)
            self.user_password_input.setEnabled(False)
            self.user_password_input.setPlaceholderText("(Not applicable)")
            self.user_table.setEnabled(True)
    
    def execute_user_operation(self):
        if not self.connection:
            QMessageBox.warning(self, "Not Connected", "Please connect to a database first.")
            return
            
        operation = self.user_op_combo.currentText()
        
        if operation == "Create User":
            username = self.username_input.text().strip()
            if not username:
                QMessageBox.warning(self, "Missing Information", "Please enter a username to create.")
                return
                
            password = self.user_password_input.text()
            if not password:
                QMessageBox.warning(self, "Missing Information", "Please enter a password for the new user.")
                return
                
            try:
                self.create_user(username, password)
                QMessageBox.information(self, "Success", f"User {username} created successfully.")
                self.load_users()
                self.username_input.clear()
                self.user_password_input.clear()
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Failed to create user:\n{self.format_exception(e)}")
        else:
            selected_rows = set(index.row() for index in self.user_table.selectedIndexes())
            
            if not selected_rows:
                QMessageBox.warning(self, "No Selection", "Please select users to operate on.")
                return
                
            try:
                if operation == "Modify Users":
                    self.modify_users(selected_rows)
                else:
                    self.delete_users(selected_rows)
                    
                QMessageBox.information(self, "Success", f"User {operation.lower()} completed successfully.")
                self.load_users()
                
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Failed to {operation.lower()} users:\n{self.format_exception(e)}")
    
    def create_user(self, username, password):
        privileges = [priv for priv, cb in self.privilege_checkboxes.items() if cb.isChecked()]
        
        if self.current_db_type == "PostgreSQL":
            with self.connection.cursor() as cursor:
                query = f"CREATE USER {username} WITH PASSWORD '{password}'"
                if privileges:
                    query += " " + " ".join(privileges)
                cursor.execute(query)
                
        else:
            with self.connection.cursor() as cursor:
                cursor.execute(f"CREATE USER '{username}'@'%' IDENTIFIED BY '{password}'")
                
                if privileges:
                    priv_list = ", ".join(priv for priv in privileges if priv in self.mysql_privs.values())
                    if priv_list:
                        cursor.execute(f"GRANT {priv_list} ON *.* TO '{username}'@'%'")
                        
        self.connection.commit()
    
    def modify_users(self, selected_rows):
        privileges = [priv for priv, cb in self.privilege_checkboxes.items() if cb.isChecked()]
        
        for row in selected_rows:
            username_item = self.user_table.item(row, 0)
            if not username_item:
                continue
                
            username = username_item.text()
            
            if self.current_db_type == "PostgreSQL":
                with self.connection.cursor() as cursor:
                    query = f"ALTER USER {username}"
                    
                    if privileges:
                        query += " WITH " + " ".join(privileges)
                        
                    cursor.execute(query)
                    
            else:
                with self.connection.cursor() as cursor:
                    cursor.execute(f"REVOKE ALL PRIVILEGES, GRANT OPTION FROM '{username}'@'%'")
                    
                    if privileges:
                        priv_list = ", ".join(priv for priv in privileges if priv in self.mysql_privs.values())
                        if priv_list:
                            cursor.execute(f"GRANT {priv_list} ON *.* TO '{username}'@'%'")
                            
            self.connection.commit()
            
    def delete_users(self, selected_rows):
        for row in selected_rows:
            username_item = self.user_table.item(row, 0)
            if not username_item:
                continue
                
            username = username_item.text()
            
            if self.current_db_type == "PostgreSQL":
                with self.connection.cursor() as cursor:
                    cursor.execute(f"DROP USER IF EXISTS {username}")
            else:
                with self.connection.cursor() as cursor:
                    cursor.execute(f"DROP USER IF EXISTS '{username}'@'%'")
                    
            self.connection.commit()
    
    def load_users(self):
        if not self.connection:
            QMessageBox.warning(self, "Not Connected", "Please connect to a database first.")
            return
            
        try:
            self.user_table.setRowCount(0)
            
            if self.current_db_type == "PostgreSQL":
                with self.connection.cursor() as cursor:
                    cursor.execute("""
                        SELECT usename, usecreatedb, usesuper, useconfig 
                        FROM pg_user
                        ORDER BY usename
                    """)
                    users = cursor.fetchall()
                    
                    for row, user in enumerate(users):
                        self.user_table.insertRow(row)
                        self.user_table.setItem(row, 0, QTableWidgetItem(user[0]))
                        self.user_table.setItem(row, 1, QTableWidgetItem("Yes" if user[2] else "No"))
                        self.user_table.setItem(row, 2, QTableWidgetItem("Yes" if user[2] else "No"))
                        self.user_table.setItem(row, 3, QTableWidgetItem(", ".join(user[3]) if user[3] else ""))
                        
            else:
                with self.connection.cursor() as cursor:
                    cursor.execute("SELECT user, host FROM mysql.user")
                    users = cursor.fetchall()
                    
                    for row, user in enumerate(users):
                        self.user_table.insertRow(row)
                        username = f"{user[0]}@{user[1]}"
                        self.user_table.setItem(row, 0, QTableWidgetItem(username))
                        
                        cursor.execute(f"SHOW GRANTS FOR '{user[0]}'@'{user[1]}'")
                        grants = cursor.fetchall()
                        privileges = []
                        for grant in grants:
                            privileges.append(grant[0].split(" ON ")[0].replace("GRANT ", ""))
                        
                        self.user_table.setItem(row, 1, QTableWidgetItem("Yes"))
                        self.user_table.setItem(row, 2, QTableWidgetItem("Yes" if "ALL PRIVILEGES" in privileges else "No"))
                        self.user_table.setItem(row, 3, QTableWidgetItem(", ".join(privileges)))
                        
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to load users:\n{self.format_exception(e)}")
    
    def browse_for_tool(self, tool_name):
        if platform.system() == "Windows":
            exe_filter = "Executable (*.exe)"
        else:
            exe_filter = ""
        
        path, _ = QFileDialog.getOpenFileName(
            self,
            f"Select {tool_name} executable",
            "",
            f"{exe_filter};;All Files (*)"
        )
        
        if path:
            if tool_name == "pg_dump":
                self.pg_dump_path_input.setText(path)
            elif tool_name == "mysqldump":
                self.mysqldump_path_input.setText(path)
    
    def apply_manual_paths(self):
        if self.pg_dump_path_input.text():
            self.pg_dump_path = self.pg_dump_path_input.text()
            self.pg_restore_path = os.path.join(
                os.path.dirname(self.pg_dump_path),
                "pg_restore.exe" if platform.system() == "Windows" else "pg_restore"
            )
        
        if self.mysqldump_path_input.text():
            self.mysqldump_path = self.mysqldump_path_input.text()
            self.mysql_path = os.path.join(
                os.path.dirname(self.mysqldump_path),
                "mysql.exe" if platform.system() == "Windows" else "mysql"
            )
        
        self.update_tools_status()
        QMessageBox.information(self, "Paths Updated", "Tool paths have been updated.")
    
    def update_tools_status(self):
        status = []
        
        if self.pg_dump_path and os.path.exists(self.pg_dump_path):
            status.append("pg_dump: Found")
        else:
            status.append("pg_dump: Not found")
            
        if self.pg_restore_path and os.path.exists(self.pg_restore_path):
            status.append("pg_restore: Found")
        else:
            status.append("pg_restore: Not found")
            
        if self.mysqldump_path and os.path.exists(self.mysqldump_path):
            status.append("mysqldump: Found")
        else:
            status.append("mysqldump: Not found")
            
        if self.mysql_path and os.path.exists(self.mysql_path):
            status.append("mysql: Found")
        else:
            status.append("mysql: Not found")
            
        self.tools_status.setText(" | ".join(status))
    
    def safe_decode(self, byte_data):
        if isinstance(byte_data, str):
            return byte_data
            
        encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
        
        for encoding in encodings:
            try:
                return byte_data.decode(encoding)
            except UnicodeDecodeError:
                continue
            except AttributeError:
                return str(byte_data)
                
        try:
            return byte_data.decode('utf-8', errors='replace')
        except:
            return "Unable to decode error message"
    
    def format_exception(self, e):
        if hasattr(e, 'args') and e.args:
            decoded_args = []
            for arg in e.args:
                if isinstance(arg, bytes):
                    decoded_args.append(self.safe_decode(arg))
                else:
                    decoded_args.append(str(arg))
            return "\n".join(decoded_args)
        return str(e)
    
    def logout_from_db(self):
        if self.connection:
            try:
                self.connection.close()
                self.connection = None
                self.current_db_type = None
                self.connection_status.setText("Disconnected")
                self.connection_status.setStyleSheet("color: black;")
                self.backup_button.setEnabled(False)
                self.logout_button.setEnabled(False)
                self.connect_button.setEnabled(True)
                self.statusBar().showMessage("Logged out successfully", 3000)
            except Exception as e:
                QMessageBox.warning(self, "Logout Error", f"Error during logout:\n{str(e)}")
    
    def find_database_tools(self):
        if platform.system() == 'Windows':
            pg_versions = ["16", "15", "14", "13", "12", "11", "10", "9.6"]
            pg_paths = [
                rf"C:\Program Files\PostgreSQL\{ver}\bin\pg_dump.exe" for ver in pg_versions
            ] + [
                r"C:\Program Files\PostgreSQL\bin\pg_dump.exe",
                os.path.expandvars(r"%PROGRAMFILES%\PostgreSQL\bin\pg_dump.exe"),
                os.path.expandvars(r"%PROGRAMFILES(x86)%\PostgreSQL\bin\pg_dump.exe")
            ]
            
            mysql_versions = ["8.1", "8.0", "5.7", "5.6"]
            mysql_paths = [
                rf"C:\Program Files\MySQL\MySQL Server {ver}\bin\mysqldump.exe" for ver in mysql_versions
            ] + [
                r"C:\Program Files\MySQL\bin\mysqldump.exe",
                os.path.expandvars(r"%PROGRAMFILES%\MySQL\bin\mysqldump.exe")
            ]
            
            for drive in ["C:", "D:", "E:"]:
                pg_paths.append(rf"{drive}\PostgreSQL\bin\pg_dump.exe")
                mysql_paths.append(rf"{drive}\MySQL\bin\mysqldump.exe")
                
            for path in pg_paths:
                if os.path.exists(path):
                    self.pg_dump_path = path
                    self.pg_restore_path = path.replace("pg_dump.exe", "pg_restore.exe")
                    break
                    
            for path in mysql_paths:
                if os.path.exists(path):
                    self.mysqldump_path = path
                    self.mysql_path = path.replace("mysqldump.exe", "mysql.exe")
                    break
        else:
            for tool in ['pg_dump', 'pg_restore', 'mysqldump', 'mysql']:
                try:
                    path = subprocess.check_output(['which', tool]).decode().strip()
                    if tool == 'pg_dump':
                        self.pg_dump_path = path
                    elif tool == 'pg_restore':
                        self.pg_restore_path = path
                    elif tool == 'mysqldump':
                        self.mysqldump_path = path
                    elif tool == 'mysql':
                        self.mysql_path = path
                except:
                    pass
        
        self.check_environment_paths()
        self.update_tools_status()
    
    def check_environment_paths(self):
        paths = os.environ['PATH'].split(os.pathsep)
        
        for path in paths:
            if not path.strip():
                continue
                
            path = path.strip('"')
            
            pg_dump = os.path.join(path, "pg_dump.exe" if platform.system() == "Windows" else "pg_dump")
            if not self.pg_dump_path and os.path.exists(pg_dump):
                self.pg_dump_path = pg_dump
                self.pg_restore_path = os.path.join(path, "pg_restore.exe" if platform.system() == "Windows" else "pg_restore")
                
            mysqldump = os.path.join(path, "mysqldump.exe" if platform.system() == "Windows" else "mysqldump")
            if not self.mysqldump_path and os.path.exists(mysqldump):
                self.mysqldump_path = mysqldump
                self.mysql_path = os.path.join(path, "mysql.exe" if platform.system() == "Windows" else "mysql")
    
    def select_backup_directory(self):
        directory = QFileDialog.getExistingDirectory(self, "Select Backup Directory")
        if directory:
            self.backup_location_input.setText(directory)
            self.refresh_backup_list()
            
    def refresh_backup_list(self):
        self.backup_list.clear()
        backup_dir = self.backup_location_input.text()
        
        if not backup_dir or not os.path.isdir(backup_dir):
            return
            
        for filename in os.listdir(backup_dir):
            if filename.startswith("Backup_") and (filename.endswith('.sql') or filename.endswith('.zip')):
                self.backup_list.addItem(filename)
                
    def toggle_restore_button(self):
        self.restore_button.setEnabled(len(self.backup_list.selectedItems()) > 0)
        
    def connect_to_db(self):
        db_type = self.db_type_combo.currentText()
        host = self.host_input.text()
        port = self.port_input.text()
        db_name = self.db_name_input.text()
        user = self.user_input.text()
        password = self.pass_input.text()
        
        if not all([host, db_name, user]):
            QMessageBox.warning(self, "Missing Information", "Please fill in all required fields.")
            return
            
        try:
            if db_type == "PostgreSQL":
                port = port or "5432"
                self.connection = psycopg2.connect(
                    host=host,
                    port=port,
                    database=db_name,
                    user=user,
                    password=password
                )
            else:
                port = port or "3306"
                self.connection = pymysql.connect(
                    host=host,
                    port=int(port),
                    database=db_name,
                    user=user,
                    password=password
                )
                
            self.current_db_type = db_type
            self.connection_status.setText(f"Connected to {db_type} database: {db_name}")
            self.connection_status.setStyleSheet("color: green;")
            self.backup_button.setEnabled(True)
            self.logout_button.setEnabled(True)
            self.connect_button.setEnabled(False)
            self.statusBar().showMessage("Connection successful", 3000)
            
        except Exception as e:
            self.connection = None
            self.connection_status.setText("Connection failed")
            self.connection_status.setStyleSheet("color: red;")
            self.backup_button.setEnabled(False)
            self.logout_button.setEnabled(False)
            
            error_msg = self.format_exception(e)
            QMessageBox.critical(self, "Connection Error", f"Failed to connect to database:\n{error_msg}")
            
    def create_backup(self):
        if not self.connection:
            QMessageBox.warning(self, "Not Connected", "Please connect to a database first.")
            return
            
        if self.current_db_type == "PostgreSQL" and not self.pg_dump_path:
            self.suggest_pg_install()
            return
            
        if self.current_db_type == "MySQL" and not self.mysqldump_path:
            self.suggest_mysql_install()
            return
            
        backup_dir = self.backup_location_input.text()
        if not backup_dir:
            QMessageBox.warning(self, "No Backup Location", "Please select a backup directory.")
            return
            
        if not os.path.exists(backup_dir):
            try:
                os.makedirs(backup_dir)
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Cannot create backup directory:\n{str(e)}")
                return
            
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"Backup_{self.db_name_input.text()}_{timestamp}"
        backup_format = self.backup_format_combo.currentText().lower()
        
        if self.current_db_type == "PostgreSQL":
            if backup_format == "csv":
                self.create_postgres_csv_backup(backup_dir, backup_name)
            else:
                self.create_postgres_sql_backup(backup_dir, backup_name)
        else:
            if backup_format == "csv":
                self.create_mysql_csv_backup(backup_dir, backup_name)
            else:
                self.create_mysql_sql_backup(backup_dir, backup_name)
                
        self.cleanup_old_backups(backup_dir)
        self.refresh_backup_list()

    def create_postgres_sql_backup(self, backup_dir, backup_name):
        backup_file = os.path.join(backup_dir, f"{backup_name}.sql")
        try:
            command = [
                self.pg_dump_path,
                "-h", self.host_input.text(),
                "-p", self.port_input.text() or "5432",
                "-U", self.user_input.text(),
                "-f", backup_file,
                self.db_name_input.text()
            ]
            
            env = os.environ.copy()
            env["PGPASSWORD"] = self.pass_input.text()
            
            process = subprocess.Popen(command, env=env, stderr=subprocess.PIPE)
            self.background_processes.append(process)
            _, stderr = process.communicate()
            
            if process.returncode != 0:
                error_msg = self.safe_decode(stderr) if stderr else "Unknown error"
                raise Exception(error_msg)
                
            QMessageBox.information(self, "Backup Successful", f"Database backup created:\n{backup_file}")
            
        except Exception as e:
            QMessageBox.critical(self, "Backup Failed", f"Failed to create backup:\n{self.format_exception(e)}")

    def create_postgres_csv_backup(self, backup_dir, backup_name):
        try:
            csv_dir = os.path.join(backup_dir, backup_name)
            if not os.path.exists(csv_dir):
                os.makedirs(csv_dir)
                
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_type = 'BASE TABLE'
                """)
                tables = cursor.fetchall()
                
                for table in tables:
                    table_name = table[0]
                    csv_file = os.path.join(csv_dir, f"{table_name}.csv")
                    
                    with open(csv_file, 'w') as f:
                        cursor.copy_expert(
                            f"COPY {table_name} TO STDOUT WITH CSV HEADER",
                            f
                        )
            
            shutil.make_archive(
                os.path.join(backup_dir, backup_name),
                'zip',
                csv_dir
            )
            
            shutil.rmtree(csv_dir)
            
            QMessageBox.information(
                self, "Backup Successful",
                f"CSV backup created:\n{backup_name}.zip"
            )
            
        except Exception as e:
            QMessageBox.critical(self, "Backup Failed", f"Failed to create CSV backup:\n{self.format_exception(e)}")

    def create_mysql_sql_backup(self, backup_dir, backup_name):
        backup_file = os.path.join(backup_dir, f"{backup_name}.sql")
        try:
            command = [
                self.mysqldump_path,
                "-h", self.host_input.text(),
                "-P", self.port_input.text() or "3306",
                "-u", self.user_input.text(),
                f"--password={self.pass_input.text()}",
                self.db_name_input.text()
            ]
            
            with open(backup_file, 'w') as output_file:
                process = subprocess.Popen(command, stdout=output_file, stderr=subprocess.PIPE)
                self.background_processes.append(process)
                _, stderr = process.communicate()
                
                if process.returncode != 0:
                    error_msg = self.safe_decode(stderr) if stderr else "Unknown error"
                    raise Exception(error_msg)
                    
            QMessageBox.information(self, "Backup Successful", f"Database backup created:\n{backup_file}")
            
        except Exception as e:
            QMessageBox.critical(self, "Backup Failed", f"Failed to create backup:\n{self.format_exception(e)}")

    def create_mysql_csv_backup(self, backup_dir, backup_name):
        try:
            csv_dir = os.path.join(backup_dir, backup_name)
            if not os.path.exists(csv_dir):
                os.makedirs(csv_dir)
                
            with self.connection.cursor() as cursor:
                cursor.execute("SHOW TABLES")
                tables = cursor.fetchall()
                
                for table in tables:
                    table_name = table[0]
                    csv_file = os.path.join(csv_dir, f"{table_name}.csv")
                    
                    cursor.execute(f"SELECT * FROM {table_name}")
                    rows = cursor.fetchall()
                    
                    cursor.execute(f"SHOW COLUMNS FROM {table_name}")
                    columns = [column[0] for column in cursor.fetchall()]
                    
                    with open(csv_file, 'w', newline='') as f:
                        writer = csv.writer(f)
                        writer.writerow(columns)
                        writer.writerows(rows)
            
            shutil.make_archive(
                os.path.join(backup_dir, backup_name),
                'zip',
                csv_dir
            )
            
            shutil.rmtree(csv_dir)
            
            QMessageBox.information(
                self, "Backup Successful",
                f"CSV backup created:\n{backup_name}.zip"
            )
            
        except Exception as e:
            QMessageBox.critical(self, "Backup Failed", f"Failed to create CSV backup:\n{self.format_exception(e)}")

    def cleanup_old_backups(self, backup_dir):
        try:
            backups = []
            for filename in os.listdir(backup_dir):
                if filename.startswith("Backup_") and (filename.endswith(".sql") or filename.endswith(".zip")):
                    filepath = os.path.join(backup_dir, filename)
                    mtime = os.path.getmtime(filepath)
                    backups.append((mtime, filepath))
            
            backups.sort()
            
            while len(backups) > self.max_backups:
                _, oldest_backup = backups.pop(0)
                try:
                    os.remove(oldest_backup)
                except Exception as e:
                    print(f"Error deleting old backup {oldest_backup}: {e}")
                    
        except Exception as e:
            print(f"Error cleaning up old backups: {e}")

    def toggle_scheduled_backups(self):
        schedule = self.schedule_combo.currentText()
        
        self.scheduler.remove_all_jobs()
        
        if schedule == "Disabled":
            self.next_backup_label.setText("Next backup: Not scheduled")
            self.enable_schedule_button.setText("Enable Schedule")
            return
            
        if schedule == "Every 1 hour":
            trigger = CronTrigger(hour="*", minute=0)
        elif schedule == "Every 6 hours":
            trigger = CronTrigger(hour="*/6", minute=0)
        elif schedule == "Every 12 hours":
            trigger = CronTrigger(hour="*/12", minute=0)
        elif schedule == "Daily at midnight":
            trigger = CronTrigger(hour=0, minute=0)
        elif schedule == "Weekly on Sunday":
            trigger = CronTrigger(day_of_week="sun", hour=0, minute=0)
            
        self.scheduler.add_job(
            self.create_backup,
            trigger=trigger,
            next_run_time=datetime.datetime.now() + datetime.timedelta(seconds=10)
        )
        
        self.enable_schedule_button.setText("Disable Schedule")
        self.update_next_backup_time()
        
    def update_next_backup_time(self):
        jobs = self.scheduler.get_jobs()
        if jobs:
            next_run = jobs[0].next_run_time
            self.next_backup_label.setText(f"Next backup: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            self.next_backup_label.setText("Next backup: Not scheduled")

    def suggest_pg_install(self):
        msg = QMessageBox()
        msg.setIcon(QMessageBox.Critical)
        msg.setWindowTitle("PostgreSQL Tools Not Found")
        msg.setText("Could not find pg_dump utility")
        msg.setInformativeText(
            "This application requires PostgreSQL client tools to perform backups.\n\n"
            "Please install PostgreSQL or specify the path to pg_dump manually."
        )
        
        if platform.system() == "Windows":
            msg.setDetailedText(
                "You can download PostgreSQL from:\n"
                "https://www.postgresql.org/download/windows/\n\n"
                "Typical installation paths:\n"
                "C:\\Program Files\\PostgreSQL\\15\\bin\\pg_dump.exe\n"
                "C:\\Program Files\\PostgreSQL\\14\\bin\\pg_dump.exe"
            )
        else:
            msg.setDetailedText(
                "On Linux, install with:\n"
                "Ubuntu/Debian: sudo apt-get install postgresql-client\n"
                "RHEL/CentOS: sudo yum install postgresql\n\n"
                "On macOS: brew install postgresql"
            )
        
        msg.exec_()
    
    def suggest_mysql_install(self):
        msg = QMessageBox()
        msg.setIcon(QMessageBox.Critical)
        msg.setWindowTitle("MySQL Tools Not Found")
        msg.setText("Could not find mysqldump utility")
        msg.setInformativeText(
            "This application requires MySQL client tools to perform backups.\n\n"
            "Please install MySQL or specify the path to mysqldump manually."
        )
        
        if platform.system() == "Windows":
            msg.setDetailedText(
                "You can download MySQL from:\n"
                "https://dev.mysql.com/downloads/installer/\n\n"
                "Typical installation paths:\n"
                "C:\\Program Files\\MySQL\\MySQL Server 8.0\\bin\\mysqldump.exe\n"
                "C:\\Program Files\\MySQL\\MySQL Server 5.7\\bin\\mysqldump.exe"
            )
        else:
            msg.setDetailedText(
                "On Linux, install with:\n"
                "Ubuntu/Debian: sudo apt-get install mysql-client\n"
                "RHEL/CentOS: sudo yum install mysql\n\n"
                "On macOS: brew install mysql"
            )
        
        msg.exec_()
                
    def restore_backup(self):
        if not self.connection:
            QMessageBox.warning(self, "Not Connected", "Please connect to a database first.")
            return
            
        selected_items = self.backup_list.selectedItems()
        if not selected_items:
            return
            
        backup_file = os.path.join(self.backup_location_input.text(), selected_items[0].text())
        
        reply = QMessageBox.question(
            self, "Confirm Restore",
            f"Are you sure you want to restore from:\n{backup_file}\n\nThis will overwrite your current database!",
            QMessageBox.Yes | QMessageBox.No
        )
        
        if reply != QMessageBox.Yes:
            return
            
        try:
            if self.current_db_type == "PostgreSQL":
                if not self.pg_restore_path or not os.path.exists(self.pg_restore_path):
                    QMessageBox.critical(self, "Error", "pg_restore utility not found. Please install PostgreSQL or specify the path.")
                    return
                    
                if self.connection:
                    self.connection.close()
                    
                command = [
                    self.pg_restore_path,
                    "-h", self.host_input.text(),
                    "-p", self.port_input.text() or "5432",
                    "-U", self.user_input.text(),
                    "-d", self.db_name_input.text(),
                    "-c",
                    backup_file
                ]
                
                env = os.environ.copy()
                env["PGPASSWORD"] = self.pass_input.text()
                
                process = subprocess.Popen(command, env=env, stderr=subprocess.PIPE)
                self.background_processes.append(process)
                _, stderr = process.communicate()
                
                if process.returncode != 0:
                    error_msg = self.safe_decode(stderr) if stderr else "Unknown error"
                    raise Exception(error_msg)
                    
            else:
                if not self.mysql_path or not os.path.exists(self.mysql_path):
                    QMessageBox.critical(self, "Error", "mysql utility not found. Please install MySQL or specify the path.")
                    return
                    
                if self.connection:
                    self.connection.close()
                    
                command = [
                    self.mysql_path,
                    "-h", self.host_input.text(),
                    "-P", self.port_input.text() or "3306",
                    "-u", self.user_input.text(),
                    f"--password={self.pass_input.text()}",
                    self.db_name_input.text()
                ]
                
                with open(backup_file, 'r') as input_file:
                    process = subprocess.Popen(command, stdin=input_file, stderr=subprocess.PIPE)
                    self.background_processes.append(process)
                    _, stderr = process.communicate()
                    
                    if process.returncode != 0:
                        error_msg = self.safe_decode(stderr) if stderr else "Unknown error"
                        raise Exception(error_msg)
                        
            self.connect_to_db()
            QMessageBox.information(self, "Restore Successful", "Database restored successfully.")
            
        except Exception as e:
            QMessageBox.critical(self, "Restore Failed", f"Failed to restore database:\n{self.format_exception(e)}")
            try:
                self.connect_to_db()
            except:
                pass
                
    def load_config(self):
        config = ConfigParser()
        if os.path.exists('db_backup_config.ini'):
            config.read('db_backup_config.ini')
            
            if 'Database' in config:
                db_config = config['Database']
                self.db_type_combo.setCurrentText(db_config.get('type', 'PostgreSQL'))
                self.host_input.setText(db_config.get('host', 'localhost'))
                self.port_input.setText(db_config.get('port', ''))
                self.db_name_input.setText(db_config.get('name', ''))
                self.user_input.setText(db_config.get('user', ''))
                
            if 'Backup' in config:
                backup_config = config['Backup']
                self.backup_location_input.setText(backup_config.get('location', ''))
                self.backup_format_combo.setCurrentText(backup_config.get('format', 'SQL'))
                self.schedule_combo.setCurrentText(backup_config.get('schedule', 'Disabled'))
                
            if 'Paths' in config:
                path_config = config['Paths']
                self.pg_dump_path = path_config.get('pg_dump', '')
                self.pg_restore_path = path_config.get('pg_restore', '')
                self.mysqldump_path = path_config.get('mysqldump', '')
                self.mysql_path = path_config.get('mysql', '')
                
                if self.pg_dump_path:
                    self.pg_dump_path_input.setText(self.pg_dump_path)
                if self.mysqldump_path:
                    self.mysqldump_path_input.setText(self.mysqldump_path)
                
                self.update_tools_status()
                
    def save_config(self):
        config = ConfigParser()
        
        config['Database'] = {
            'type': self.db_type_combo.currentText(),
            'host': self.host_input.text(),
            'port': self.port_input.text(),
            'name': self.db_name_input.text(),
            'user': self.user_input.text()
        }
        
        config['Backup'] = {
            'location': self.backup_location_input.text(),
            'format': self.backup_format_combo.currentText(),
            'schedule': self.schedule_combo.currentText()
        }
        
        config['Paths'] = {
            'pg_dump': self.pg_dump_path or '',
            'pg_restore': self.pg_restore_path or '',
            'mysqldump': self.mysqldump_path or '',
            'mysql': self.mysql_path or ''
        }
        
        with open('db_backup_config.ini', 'w') as configfile:
            config.write(configfile)
            
        QMessageBox.information(self, "Configuration Saved", "Settings have been saved to db_backup_config.ini")
        
    def closeEvent(self, event):
        """Handle application close event"""
        try:
            # Shutdown scheduler
            if hasattr(self, 'scheduler') and self.scheduler:
                self.scheduler.shutdown()
                
            # Close database connection
            if hasattr(self, 'connection') and self.connection:
                self.connection.close()
                
            # Terminate any background processes
            self.terminate_background_processes()
            
        except Exception as e:
            print(f"Error during shutdown: {e}")
            
        event.accept()
        
    def terminate_background_processes(self):
        """Terminate all background processes"""
        for proc in self.background_processes:
            try:
                if isinstance(proc, subprocess.Popen):
                    # Terminate the process tree
                    parent = psutil.Process(proc.pid)
                    for child in parent.children(recursive=True):
                        child.terminate()
                    parent.terminate()
                    
                    # Wait a bit then kill if still running
                    try:
                        proc.wait(timeout=2)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        
                elif platform.system() == 'Windows' and isinstance(proc, tuple):
                    # Handle Windows service control processes
                    handle, process_id = proc
                    try:
                        win32process.TerminateProcess(handle, 0)
                        win32api.CloseHandle(handle)
                    except:
                        pass
                        
            except Exception as e:
                print(f"Error terminating process {proc}: {e}")
                
        self.background_processes = []

if __name__ == "__main__":
    # On Windows, hide the console window
    if platform.system() == "Windows":
        import ctypes
        ctypes.windll.user32.ShowWindow(ctypes.windll.kernel32.GetConsoleWindow(), 0)
        
        # Try to restart as admin if needed
        try:
            if not ctypes.windll.shell32.IsUserAnAdmin():
                ctypes.windll.shell32.ShellExecuteW(None, "runas", sys.executable, " ".join(sys.argv), None, 1)
                sys.exit()
        except:
            pass
    
    app = QApplication(sys.argv)
    window = DatabaseBackupApp()
    window.show()
    sys.exit(app.exec_())