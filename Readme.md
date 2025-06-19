Initial commit: Database Admin Tool v1.0
A Python-based desktop application with Windows UI that provides database administration capabilities for both PostgreSQL and MySQL systems.

Required to be installed ( Windows ):
https://www.pgadmin.org/download/pgadmin-4-windows/
https://visualstudio.microsoft.com/visual-cpp-build-tools/
https://www.postgresql.org/download/
https://www.python.org/downloads/

Features implemented:

- Cross-platform database administration (PostgreSQL/MySQL)
- Windows UI with PyQt5 including:
  - Connection management tab
  - Backup/restore functionality
  - User management system
- Comprehensive database operations:
  - Secure login/logout
  - Database backup/restore (pg_dump/mysqldump)
  - User CRUD operations
  - Role assignment/privilege management
- Service control for PostgreSQL (Windows)
- Configuration persistence
- Automatic tool detection
- Admin privilege handling

Technical components:

- PostgreSQL (psycopg2)
- MySQL (pymysql)
- PyQt5 GUI framework
- ConfigParser for settings
- Subprocess for backup operations
- WMI/win32service for Windows service control
