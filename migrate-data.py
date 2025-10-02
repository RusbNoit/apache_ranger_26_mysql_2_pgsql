#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import MySQLdb
import psycopg2
import sys
from datetime import datetime

# Import configuration
try:
    from config import MYSQL_CONFIG, POSTGRES_CONFIG, MIGRATION_CONFIG, PRIORITY_TABLES_ORDER, TYPE_CONVERSIONS
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

def table_exists(postgres_conn, table_name):
    """Checks if table exists in PostgreSQL"""
    cursor = postgres_conn.cursor()
    try:
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            )
        """, (table_name,))
        exists = cursor.fetchone()[0]
        return exists
    except Exception as e:
        log_message("Error checking table existence {0}: {1}".format(table_name, str(e)))
        return False
    finally:
        cursor.close()

def get_mysql_tables_ordered(mysql_conn, postgres_conn):
    """Gets ordered list of tables from MySQL according to priority"""
    cursor = mysql_conn.cursor()
    cursor.execute("SHOW TABLES")
    all_tables = [table[0] for table in cursor.fetchall()]
    cursor.close()
    
    # Filter tables: remove views and tables from skip_tables
    valid_tables = []
    for table in all_tables:
        if table in MIGRATION_CONFIG.get('skip_tables', []):
            log_message("Excluding table {0} (in skip_tables)".format(table))
            continue
        
        if is_view(postgres_conn, table):
            log_message("Excluding view {0}".format(table))
            continue
            
        if not table_exists(postgres_conn, table):
            log_message("Table {0} doesn't exist in PostgreSQL, excluding".format(table))
            continue
            
        valid_tables.append(table)
    
    # Order tables according to priority
    ordered_tables = []
    
    # 1. Add tables in priority order
    for priority_table in PRIORITY_TABLES_ORDER:
        if priority_table in valid_tables:
            ordered_tables.append(priority_table)
            valid_tables.remove(priority_table)
    
    # 2. Add remaining tables in alphabetical order
    valid_tables.sort()
    ordered_tables.extend(valid_tables)
    
    log_message("Ordered table list for migration ({0} tables):".format(len(ordered_tables)))
    for i, table in enumerate(ordered_tables, 1):
        log_message("  {0:3d}. {1}".format(i, table))
    
    return ordered_tables

def truncate_postgres_tables(postgres_conn):
    """Clears all tables in PostgreSQL before insertion (in reverse dependency order)"""
    
    # Use reverse order for clearing (from child to parent tables)
    tables_to_truncate = list(reversed(PRIORITY_TABLES_ORDER))
    
    truncated_count = 0
    for table_name in tables_to_truncate:
        cursor = None
        try:
            # Create new cursor for each table
            cursor = postgres_conn.cursor()
            
            # Check table existence before clearing
            if not table_exists(postgres_conn, table_name):
                if MIGRATION_CONFIG.get('skip_missing_tables', True):
                    log_message("Table {0} doesn't exist, skipping truncation".format(table_name))
                    continue
                else:
                    log_message("Warning: table {0} doesn't exist".format(table_name))
                    continue
            
            # Disable foreign key checks for safe truncation
            cursor.execute("SET session_replication_role = 'replica';")
            
            cursor.execute("TRUNCATE TABLE {0} CASCADE".format(table_name))
            truncated_count += 1
            log_message("Cleared table: {0}".format(table_name))
            
            # Enable foreign key checks back
            cursor.execute("SET session_replication_role = 'origin';")
            
            # Commit changes for each table separately
            postgres_conn.commit()
            
        except Exception as e:
            log_message("Warning: failed to clear table {0}: {1}".format(table_name, str(e)))
            # Rollback only current operation
            if cursor:
                try:
                    cursor.execute("ROLLBACK")
                except:
                    pass
            # Create new connection if transaction is in failed state
            try:
                cursor = postgres_conn.cursor()
                cursor.execute("ROLLBACK")
                postgres_conn.commit()
            except:
                # If recovery fails, reconnect
                log_message("Reconnecting to PostgreSQL due to failed transaction state")
                postgres_conn.close()
                postgres_conn = psycopg2.connect(**POSTGRES_CONFIG)
        finally:
            if cursor:
                cursor.close()
    
    log_message("Cleared tables: {0}".format(truncated_count))
    return postgres_conn  # Return possibly updated connection

def convert_data_types(table_name, columns, rows):
    """Converts data types between MySQL and PostgreSQL"""
    converted_rows = []
    
    for row in rows:
        converted_row = list(row)  # Create row copy
        
        # Apply type conversions
        for conversion_type, conversion_config in TYPE_CONVERSIONS.items():
            tables_config = conversion_config.get('tables', {})
            conversion_func = conversion_config.get('conversion')
            
            if table_name in tables_config and conversion_func:
                columns_to_convert = tables_config[table_name]
                
                for col_name in columns_to_convert:
                    if col_name in columns:
                        col_index = columns.index(col_name)
                        if col_index < len(converted_row):
                            try:
                                converted_row[col_index] = conversion_func(converted_row[col_index])
                            except Exception as e:
                                log_message("Type conversion error for table {0}, column {1}: {2}".format(
                                    table_name, col_name, str(e)))
                                # Keep original value in case of error
        
        converted_rows.append(tuple(converted_row))
    
    return converted_rows

def migrate_table_data(mysql_conn, postgres_conn, table_name):
    """Migrates data for specific table"""
    
    mysql_cursor = mysql_conn.cursor()
    postgres_cursor = None
    
    try:
        # Get data from MySQL
        mysql_cursor.execute("SELECT * FROM {0}".format(table_name))
        rows = mysql_cursor.fetchall()
        
        if not rows:
            log_message("Table {0} is empty, skipping".format(table_name))
            return 0
        
        # Get column names
        mysql_cursor.execute("SHOW COLUMNS FROM {0}".format(table_name))
        columns = [col[0] for col in mysql_cursor.fetchall()]
        
        # Convert data types
        log_message("Converting data types for table {0}".format(table_name))
        converted_rows = convert_data_types(table_name, columns, rows)
        
        # Create placeholders for PostgreSQL
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join(columns)
        
        # Create new cursor for PostgreSQL for this table
        postgres_cursor = postgres_conn.cursor()
        
        # Insert data in batches
        batch_size = MIGRATION_CONFIG.get('batch_size', 1000)
        insert_count = 0
        
        for i in range(0, len(converted_rows), batch_size):
            batch = converted_rows[i:i + batch_size]
            try:
                postgres_cursor.executemany(
                    "INSERT INTO {0} ({1}) VALUES ({2})".format(table_name, columns_str, placeholders),
                    batch
                )
                insert_count += len(batch)
                if len(converted_rows) > batch_size and insert_count % (batch_size * 5) == 0:
                    log_message("Table {0}: migrated {1} records".format(table_name, insert_count))
                
            except Exception as e:
                log_message("Error inserting batch into {0}: {1}".format(table_name, str(e)))
                # Rollback current transaction and try inserting one by one
                postgres_conn.rollback()
                # Create new cursor after rollback
                if postgres_cursor:
                    postgres_cursor.close()
                postgres_cursor = postgres_conn.cursor()
                
                # Try inserting one by one to identify problematic data
                for single_row in batch:
                    try:
                        postgres_cursor.execute(
                            "INSERT INTO {0} ({1}) VALUES ({2})".format(table_name, columns_str, placeholders),
                            single_row
                        )
                        insert_count += 1
                    except Exception as e2:
                        log_message("Error inserting single record into {0}: {1}".format(table_name, str(e2)))
                        log_message("Problematic record: {0}".format(str(single_row)[:500]))  # Log part of problematic record
                        continue
        
        postgres_conn.commit()
        log_message("Table {0}: SUCCESSFULLY migrated {1} records".format(table_name, insert_count))
        return insert_count
        
    except Exception as e:
        log_message("CRITICAL ERROR migrating table {0}: {1}".format(table_name, str(e)))
        try:
            postgres_conn.rollback()
        except:
            pass
        return 0
    finally:
        mysql_cursor.close()
        if postgres_cursor:
            postgres_cursor.close()

def update_sequences(postgres_conn):
    """Updates sequences in PostgreSQL"""
    
    # List of tables and corresponding sequences based on Ranger schema
    sequence_mappings = [
        ('x_portal_user', 'x_portal_user_seq'),
        ('x_portal_user_role', 'x_portal_user_role_seq'),
        ('xa_access_audit', 'xa_access_audit_seq'),
        ('x_asset', 'x_asset_seq'),
        ('x_auth_sess', 'x_auth_sess_seq'),
        ('x_cred_store', 'x_cred_store_seq'),
        ('x_db_base', 'x_db_base_seq'),
        ('x_group', 'x_group_seq'),
        ('x_group_groups', 'x_group_groups_seq'),
        ('x_user', 'x_user_seq'),
        ('x_group_users', 'x_group_users_seq'),
        ('x_policy_export_audit', 'x_policy_export_seq'),
        ('x_resource', 'x_resource_seq'),
        ('x_perm_map', 'x_perm_map_seq'),
        ('x_audit_map', 'x_audit_map_seq'),
        ('x_trx_log_v2', 'x_trx_log_v2_seq'),
        ('x_service_def', 'x_service_def_seq'),
        ('x_service', 'x_service_seq'),
        ('x_security_zone', 'x_security_zone_seq'),
        ('x_ranger_global_state', 'x_ranger_global_state_seq'),
        ('x_policy', 'x_policy_seq'),
        ('x_service_config_def', 'x_service_config_def_seq'),
        ('x_resource_def', 'x_resource_def_seq'),
        ('x_access_type_def', 'x_access_type_def_seq'),
        ('x_access_type_def_grants', 'x_access_type_def_grants_seq'),
        ('x_policy_condition_def', 'x_policy_condition_def_seq'),
        ('x_context_enricher_def', 'x_context_enricher_def_seq'),
        ('x_enum_def', 'x_enum_def_seq'),
        ('x_enum_element_def', 'x_enum_element_def_seq'),
        ('x_service_config_map', 'x_service_config_map_seq'),
        ('x_policy_resource', 'x_policy_resource_seq'),
        ('x_policy_resource_map', 'x_policy_resource_map_seq'),
        ('x_policy_item', 'x_policy_item_seq'),
        ('x_policy_item_access', 'x_policy_item_access_seq'),
        ('x_policy_item_condition', 'x_policy_item_condition_seq'),
        ('x_policy_item_user_perm', 'x_policy_item_user_perm_seq'),
        ('x_policy_item_group_perm', 'x_policy_item_group_perm_seq'),
        ('x_data_hist', 'x_data_hist_seq'),
        ('x_modules_master', 'x_modules_master_seq'),
        ('x_user_module_perm', 'x_user_module_perm_seq'),
        ('x_group_module_perm', 'x_group_module_perm_seq'),
        ('x_tag_def', 'x_tag_def_seq'),
        ('x_tag', 'x_tag_seq'),
        ('x_service_resource', 'x_service_resource_seq'),
        ('x_tag_resource_map', 'x_tag_resource_map_seq'),
        ('x_datamask_type_def', 'x_datamask_type_def_seq'),
        ('x_policy_item_datamask', 'x_policy_item_datamask_seq'),
        ('x_policy_item_rowfilter', 'x_policy_item_rowfilter_seq'),
        ('x_service_version_info', 'x_service_version_info_seq'),
        ('x_plugin_info', 'x_plugin_info_seq'),
        ('x_policy_label', 'x_policy_label_seq'),
        ('x_policy_label_map', 'x_policy_label_map_seq'),
        ('x_ugsync_audit_info', 'x_ugsync_audit_info_seq'),
        ('x_policy_ref_resource', 'x_policy_ref_resource_seq'),
        ('x_policy_ref_access_type', 'x_policy_ref_access_type_seq'),
        ('x_policy_ref_condition', 'x_policy_ref_condition_seq'),
        ('x_policy_ref_datamask_type', 'x_policy_ref_datamask_type_seq'),
        ('x_policy_ref_user', 'x_policy_ref_user_seq'),
        ('x_policy_ref_group', 'x_policy_ref_group_seq'),
        ('x_security_zone_ref_service', 'x_sec_zone_ref_service_seq'),
        ('x_security_zone_ref_tag_srvc', 'x_sec_zone_ref_tag_srvc_seq'),
        ('x_security_zone_ref_resource', 'x_sec_zone_ref_resource_seq'),
        ('x_security_zone_ref_user', 'x_sec_zone_ref_user_seq'),
        ('x_security_zone_ref_group', 'x_sec_zone_ref_group_seq'),
        ('x_policy_change_log', 'x_policy_change_log_seq'),
        ('x_role', 'x_role_seq'),
        ('x_role_ref_user', 'x_role_ref_user_seq'),
        ('x_role_ref_group', 'x_role_ref_group_seq'),
        ('x_policy_ref_role', 'x_policy_ref_role_seq'),
        ('x_role_ref_role', 'x_role_ref_role_seq'),
        ('x_security_zone_ref_role', 'x_sec_zone_ref_role_seq'),
        ('x_tag_change_log', 'x_tag_change_log_seq'),
        ('x_rms_service_resource', 'x_rms_service_resource_seq'),
        ('x_rms_notification', 'x_rms_notification_seq'),
        ('x_rms_resource_mapping', 'x_rms_resource_mapping_seq'),
        ('x_rms_mapping_provider', 'x_rms_mapping_provider_seq'),
        ('x_gds_dataset', 'x_gds_dataset_seq'),
        ('x_gds_project', 'x_gds_project_seq'),
        ('x_gds_data_share', 'x_gds_data_share_seq'),
        ('x_gds_shared_resource', 'x_gds_shared_resource_seq'),
        ('x_gds_data_share_in_dataset', 'x_gds_data_share_in_dataset_seq'),
        ('x_gds_dataset_in_project', 'x_gds_dataset_in_project_seq'),
        ('x_gds_dataset_policy_map', 'x_gds_dataset_policy_map_seq'),
        ('x_gds_project_policy_map', 'x_gds_project_policy_map_seq'),
    ]
    
    updated_count = 0
    for table_name, sequence_name in sequence_mappings:
        cursor = None
        try:
            # Create new cursor for each sequence
            cursor = postgres_conn.cursor()
            
            # Check table existence
            if not table_exists(postgres_conn, table_name):
                log_message("Table {0} doesn't exist in PostgreSQL, skipping sequence".format(table_name))
                continue
                
            # Check sequence existence
            cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.sequences WHERE sequence_name = %s)", (sequence_name,))
            sequence_exists = cursor.fetchone()[0]
            
            if not sequence_exists:
                log_message("Sequence {0} doesn't exist, skipping".format(sequence_name))
                continue
            
            # Get maximum ID from table
            cursor.execute("SELECT COALESCE(MAX(id), 0) FROM {0}".format(table_name))
            max_id = cursor.fetchone()[0]
            
            if max_id > 0:
                # Update sequence
                cursor.execute("SELECT setval(%s, %s)", (sequence_name, max_id))
                log_message("Updated sequence {0} to {1}".format(sequence_name, max_id))
                updated_count += 1
            
            # Commit changes for each sequence separately
            postgres_conn.commit()
                
        except Exception as e:
            log_message("Error updating sequence {0}: {1}".format(sequence_name, str(e)))
            # Rollback current operation
            if cursor:
                try:
                    cursor.execute("ROLLBACK")
                except:
                    pass
        finally:
            if cursor:
                cursor.close()
    
    log_message("Updated sequences: {0}".format(updated_count))

def main():
    """Main migration function"""
    log_message("Starting data migration from MySQL to PostgreSQL")
    
    mysql_conn = None
    postgres_conn = None
    
    try:
        # Connect to MySQL
        log_message("Connecting to MySQL...")
        mysql_conn = MySQLdb.connect(**MYSQL_CONFIG)
        
        # Connect to PostgreSQL
        log_message("Connecting to PostgreSQL...")
        postgres_conn = psycopg2.connect(**POSTGRES_CONFIG)
        
        # Clear tables in PostgreSQL before insertion
        if MIGRATION_CONFIG.get('truncate_before_insert', True):
            log_message("Clearing tables in PostgreSQL...")
            postgres_conn = truncate_postgres_tables(postgres_conn)
        else:
            log_message("Skipping table clearing (truncate_before_insert = False)")
        
        # Get ordered list of tables from MySQL
        log_message("Getting ordered table list...")
        mysql_tables = get_mysql_tables_ordered(mysql_conn, postgres_conn)
        
        # Migrate data for each table in correct order
        total_migrated = 0
        for table in mysql_tables:
            log_message(">>> Starting migration of table: {0}".format(table))
            migrated_count = migrate_table_data(mysql_conn, postgres_conn, table)
            total_migrated += migrated_count
            log_message("<<< Completed migration of table: {0}".format(table))
        
        # Update sequences
        log_message("Updating sequences in PostgreSQL...")
        update_sequences(postgres_conn)
        
        log_message("=" * 60)
        log_message("MIGRATION SUCCESSFULLY COMPLETED!")
        log_message("Total records migrated: {0}".format(total_migrated))
        log_message("=" * 60)
        
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