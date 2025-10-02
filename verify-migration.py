#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import MySQLdb
import psycopg2
import sys
from datetime import datetime

# Import configuration
try:
    from config import MYSQL_CONFIG, POSTGRES_CONFIG, MIGRATION_CONFIG, PRIORITY_TABLES_ORDER
except ImportError:
    print("Error: config.py file not found")
    sys.exit(1)

def log_message(message):
    """Logging messages with timestamp"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("[{0}] {1}".format(timestamp, message))

def is_view(postgres_conn, object_name):
    """Checks if object is a view"""
    cursor = postgres_conn.cursor()
    try:
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.views 
                WHERE table_schema = 'public' 
                AND table_name = %s
            )
        """, (object_name,))
        is_view = cursor.fetchone()[0]
        return is_view
    except Exception as e:
        log_message("Error checking view {0}: {1}".format(object_name, str(e)))
        return False
    finally:
        cursor.close()

def table_exists(conn, table_name, db_type='postgresql'):
    """Checks if table exists in database"""
    cursor = conn.cursor()
    try:
        if db_type == 'postgresql':
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                )
            """, (table_name,))
        else:  # mysql
            cursor.execute("""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_schema = DATABASE() 
                AND table_name = %s
            """, (table_name,))
        exists = cursor.fetchone()[0]
        return bool(exists)
    except Exception as e:
        log_message("Error checking table existence {0} in {1}: {2}".format(table_name, db_type, str(e)))
        return False
    finally:
        cursor.close()

def get_table_row_count(conn, table_name, db_type='postgresql'):
    """Gets row count from table"""
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM {0}".format(table_name))
        count = cursor.fetchone()[0]
        return count
    except Exception as e:
        log_message("Error getting row count from table {0} in {1}: {2}".format(table_name, db_type, str(e)))
        return -1
    finally:
        cursor.close()

def get_table_size(conn, table_name, db_type='postgresql'):
    """Gets approximate table size"""
    cursor = conn.cursor()
    try:
        if db_type == 'postgresql':
            cursor.execute("""
                SELECT pg_size_pretty(pg_total_relation_size(%s))
            """, (table_name,))
        else:  # mysql
            cursor.execute("""
                SELECT 
                    CONCAT(ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024, 2), ' MB')
                FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = DATABASE() 
                AND TABLE_NAME = %s
            """, (table_name,))
        size = cursor.fetchone()[0]
        return size
    except Exception as e:
        log_message("Error getting table size {0} in {1}: {2}".format(table_name, db_type, str(e)))
        return "N/A"
    finally:
        cursor.close()

def get_all_tables(mysql_conn, postgres_conn):
    """Gets list of all tables from both databases"""
    mysql_cursor = mysql_conn.cursor()
    postgres_cursor = postgres_conn.cursor()
    
    mysql_tables = []
    postgres_tables = []
    
    try:
        # Get tables from MySQL
        mysql_cursor.execute("SHOW TABLES")
        mysql_tables = [table[0] for table in mysql_cursor.fetchall()]
        
        # Get tables from PostgreSQL (excluding views)
        postgres_cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
        """)
        postgres_tables = [table[0] for table in postgres_cursor.fetchall()]
        
    except Exception as e:
        log_message("Error getting table list: {0}".format(str(e)))
    finally:
        mysql_cursor.close()
        postgres_cursor.close()
    
    # Combine tables from both databases and remove duplicates
    all_tables = set(mysql_tables + postgres_tables)
    
    # Filter tables: remove views and tables from skip_tables
    valid_tables = []
    for table in all_tables:
        if table in MIGRATION_CONFIG.get('skip_tables', []):
            continue
        
        if is_view(postgres_conn, table):
            continue
            
        valid_tables.append(table)
    
    # Sort tables according to priority, then alphabetically
    ordered_tables = []
    
    # 1. Add tables in priority order
    for priority_table in PRIORITY_TABLES_ORDER:
        if priority_table in valid_tables:
            ordered_tables.append(priority_table)
            valid_tables.remove(priority_table)
    
    # 2. Add remaining tables in alphabetical order
    valid_tables.sort()
    ordered_tables.extend(valid_tables)
    
    return ordered_tables

def verify_table_data(mysql_conn, postgres_conn, table_name):
    """Verifies table data between MySQL and PostgreSQL"""
    result = {
        'table': table_name,
        'mysql_exists': False,
        'postgres_exists': False,
        'mysql_count': 0,
        'postgres_count': 0,
        'mysql_size': 'N/A',
        'postgres_size': 'N/A',
        'status': 'UNKNOWN'
    }
    
    # Check table existence
    result['mysql_exists'] = table_exists(mysql_conn, table_name, 'mysql')
    result['postgres_exists'] = table_exists(postgres_conn, table_name, 'postgresql')
    
    if not result['mysql_exists'] and not result['postgres_exists']:
        result['status'] = 'MISSING_BOTH'
        return result
    
    if not result['mysql_exists']:
        result['status'] = 'MISSING_MYSQL'
        return result
    
    if not result['postgres_exists']:
        result['status'] = 'MISSING_POSTGRES'
        return result
    
    # Get row counts
    result['mysql_count'] = get_table_row_count(mysql_conn, table_name, 'mysql')
    result['postgres_count'] = get_table_row_count(postgres_conn, table_name, 'postgresql')
    
    # Get table sizes
    result['mysql_size'] = get_table_size(mysql_conn, table_name, 'mysql')
    result['postgres_size'] = get_table_size(postgres_conn, table_name, 'postgresql')
    
    # Determine status
    if result['mysql_count'] == result['postgres_count']:
        result['status'] = 'MATCH'
    elif result['postgres_count'] == 0:
        result['status'] = 'EMPTY_POSTGRES'
    elif result['mysql_count'] == 0:
        result['status'] = 'EMPTY_MYSQL'
    else:
        result['status'] = 'MISMATCH'
    
    return result

def generate_report(verification_results):
    """Generates data verification report"""
    log_message("=" * 100)
    log_message("DATA VERIFICATION REPORT MYSQL -> POSTGRESQL")
    log_message("=" * 100)
    
    # Statistics
    total_tables = len(verification_results)
    matched_tables = len([r for r in verification_results if r['status'] == 'MATCH'])
    mismatch_tables = len([r for r in verification_results if r['status'] == 'MISMATCH'])
    missing_mysql_tables = len([r for r in verification_results if r['status'] == 'MISSING_MYSQL'])
    missing_postgres_tables = len([r for r in verification_results if r['status'] == 'MISSING_POSTGRES'])
    missing_both_tables = len([r for r in verification_results if r['status'] == 'MISSING_BOTH'])
    empty_mysql_tables = len([r for r in verification_results if r['status'] == 'EMPTY_MYSQL'])
    empty_postgres_tables = len([r for r in verification_results if r['status'] == 'EMPTY_POSTGRES'])
    
    log_message("STATISTICS:")
    log_message("  Total tables: {0}".format(total_tables))
    log_message("  Matched: {0}".format(matched_tables))
    log_message("  Mismatched: {0}".format(mismatch_tables))
    log_message("  Missing in MySQL: {0}".format(missing_mysql_tables))
    log_message("  Missing in PostgreSQL: {0}".format(missing_postgres_tables))
    log_message("  Missing in both: {0}".format(missing_both_tables))
    log_message("  Empty in MySQL: {0}".format(empty_mysql_tables))
    log_message("  Empty in PostgreSQL: {0}".format(empty_postgres_tables))
    log_message("")
    
    # Detailed report
    log_message("DETAILED REPORT:")
    log_message("-" * 100)
    log_message("{:<40} {:<10} {:<10} {:<15} {:<15} {:<10}".format(
        "TABLE", "MySQL", "PostgreSQL", "MySQL Size", "PgSQL Size", "STATUS"))
    log_message("-" * 100)
    
    for result in verification_results:
        mysql_count_str = str(result['mysql_count']) if result['mysql_exists'] else "N/A"
        postgres_count_str = str(result['postgres_count']) if result['postgres_exists'] else "N/A"
        
        # Determine status display
        status = result['status']
        if status == 'MATCH':
            status_str = "✓ MATCH"
        elif status == 'MISMATCH':
            status_str = "✗ MISMATCH"
        elif status == 'MISSING_MYSQL':
            status_str = "! NO IN MYSQL"
        elif status == 'MISSING_POSTGRES':
            status_str = "! NO IN PGSQL"
        elif status == 'MISSING_BOTH':
            status_str = "! NO IN BOTH"
        elif status == 'EMPTY_MYSQL':
            status_str = "○ EMPTY MYSQL"
        elif status == 'EMPTY_POSTGRES':
            status_str = "○ EMPTY PGSQL"
        else:
            status_str = "? UNKNOWN"
        
        log_message("{:<40} {:<10} {:<10} {:<15} {:<15} {:<10}".format(
            result['table'][:39],
            mysql_count_str,
            postgres_count_str,
            result['mysql_size'],
            result['postgres_size'],
            status_str
        ))
    
    log_message("-" * 100)
    
    # Show tables with mismatches
    if mismatch_tables > 0:
        log_message("")
        log_message("TABLES WITH MISMATCHES:")
        for result in verification_results:
            if result['status'] == 'MISMATCH':
                difference = result['postgres_count'] - result['mysql_count']
                diff_str = "+{0}".format(difference) if difference > 0 else str(difference)
                log_message("  {0}: MySQL={1}, PostgreSQL={2} (difference: {3})".format(
                    result['table'], result['mysql_count'], result['postgres_count'], diff_str))
    
    # Show missing tables
    if missing_postgres_tables > 0:
        log_message("")
        log_message("TABLES MISSING IN POSTGRESQL:")
        for result in verification_results:
            if result['status'] == 'MISSING_POSTGRES':
                log_message("  {0}".format(result['table']))
    
    if missing_mysql_tables > 0:
        log_message("")
        log_message("TABLES MISSING IN MYSQL:")
        for result in verification_results:
            if result['status'] == 'MISSING_MYSQL':
                log_message("  {0}".format(result['table']))

def main():
    """Main data verification function"""
    log_message("Starting data verification between MySQL and PostgreSQL")
    
    mysql_conn = None
    postgres_conn = None
    
    try:
        # Connect to MySQL
        log_message("Connecting to MySQL...")
        mysql_conn = MySQLdb.connect(**MYSQL_CONFIG)
        
        # Connect to PostgreSQL
        log_message("Connecting to PostgreSQL...")
        postgres_conn = psycopg2.connect(**POSTGRES_CONFIG)
        
        # Get list of all tables
        log_message("Getting table list...")
        all_tables = get_all_tables(mysql_conn, postgres_conn)
        log_message("Found tables to check: {0}".format(len(all_tables)))
        
        # Verify each table
        verification_results = []
        total_tables = len(all_tables)
        
        for i, table in enumerate(all_tables, 1):
            log_message("Checking table {0}/{1}: {2}".format(i, total_tables, table))
            result = verify_table_data(mysql_conn, postgres_conn, table)
            verification_results.append(result)
        
        # Generate report
        generate_report(verification_results)
        
        # Check overall result
        mismatched_tables = [r for r in verification_results if r['status'] in ['MISMATCH', 'MISSING_POSTGRES', 'EMPTY_POSTGRES']]
        
        if mismatched_tables:
            log_message("")
            log_message("WARNING: Found mismatches in {0} tables!".format(len(mismatched_tables)))
            sys.exit(1)
        else:
            log_message("")
            log_message("SUCCESS: All tables verified successfully without mismatches!")
            sys.exit(0)
        
    except Exception as e:
        log_message("CRITICAL ERROR: {0}".format(str(e)))
        sys.exit(1)
    finally:
        # Close connections
        if mysql_conn:
            mysql_conn.close()
        if postgres_conn:
            postgres_conn.close()
        
        log_message("Database connections closed")

if __name__ == "__main__":
    main()