#!/usr/bin/env python2
# -*- coding: utf-8 -*-

# Database connection configuration
MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'mysql_user',
    'passwd': 'mysql_password',
    'db': 'mysql_database',
    'charset': 'utf8'
}

POSTGRES_CONFIG = {
    'host': 'localhost',
    'user': 'postgres_user',
    'password': 'postgres_password',
    'database': 'postgres_database',
    'port': 5432
}

# Migration settings
MIGRATION_CONFIG = {
    'batch_size': 1000,  # Batch size for insertion
    'skip_tables': ['vx_principal'],   # Tables to skip (including views)
    'log_level': 'INFO',  # Logging level
    'truncate_before_insert': True,  # Clear tables before insertion
    'skip_missing_tables': True  # Skip missing tables
}

# Priority order for table copying (from parent to child tables)
PRIORITY_TABLES_ORDER = [
    # 1. Basic user and authentication tables
    'x_portal_user',
    'x_cred_store',
    'x_user',
    'x_group',
    'x_group_groups',
    'x_group_users',
    'x_auth_sess',
    
    # 2. Services and definitions
    'x_service_def',
    'x_service',
    'x_service_config_def',
    'x_service_config_map',
    'x_resource_def',
    'x_access_type_def',
    'x_access_type_def_grants',
    'x_policy_condition_def',
    'x_context_enricher_def',
    'x_enum_def',
    'x_enum_element_def',
    
    # 3. Security and zones
    'x_security_zone',
    'x_ranger_global_state',
    
    # 4. Resources and assets
    'x_asset',
    'x_resource',
    
    # 5. Policies (main tables)
    'x_policy',
    'x_policy_resource',
    'x_policy_resource_map',
    'x_policy_item',
    'x_policy_item_access',
    'x_policy_item_condition',
    'x_policy_item_user_perm',
    'x_policy_item_group_perm',
    
    # 6. Data masking and row filtering
    'x_datamask_type_def',
    'x_policy_item_datamask',
    'x_policy_item_rowfilter',
    
    # 7. Tags
    'x_tag_def',
    'x_tag',
    'x_service_resource',
    'x_tag_resource_map',
    
    # 8. Roles
    'x_role',
    'x_role_ref_user',
    'x_role_ref_group',
    'x_role_ref_role',
    
    # 9. Policy references
    'x_policy_ref_resource',
    'x_policy_ref_access_type',
    'x_policy_ref_condition',
    'x_policy_ref_datamask_type',
    'x_policy_ref_user',
    'x_policy_ref_group',
    'x_policy_ref_role',
    
    # 10. Security zone references
    'x_security_zone_ref_service',
    'x_security_zone_ref_tag_srvc',
    'x_security_zone_ref_resource',
    'x_security_zone_ref_user',
    'x_security_zone_ref_group',
    'x_security_zone_ref_role',
    
    # 11. Modules and permissions
    'x_modules_master',
    'x_user_module_perm',
    'x_group_module_perm',
    
    # 12. GDS (Governed Data Sharing)
    'x_gds_dataset',
    'x_gds_project',
    'x_gds_data_share',
    'x_gds_shared_resource',
    'x_gds_data_share_in_dataset',
    'x_gds_dataset_in_project',
    'x_gds_dataset_policy_map',
    'x_gds_project_policy_map',
    
    # 13. RMS (Resource Mapping Service)
    'x_rms_service_resource',
    'x_rms_notification',
    'x_rms_resource_mapping',
    'x_rms_mapping_provider',
    
    # 14. Logs and audit (may reference many tables)
    'x_policy_change_log',
    'x_tag_change_log',
    'x_policy_export_audit',
    'x_ugsync_audit_info',
    'x_service_version_info',
    'x_plugin_info',
    'x_policy_label',
    'x_policy_label_map',
    'x_data_hist',
    
    # 15. Permissions and audit (reference resources, users, groups)
    'x_perm_map',
    'x_audit_map',
    
    # 16. Base tables and access audit
    'x_db_base',
    'x_db_version_h',
    'xa_access_audit',
    'x_trx_log_v2'
]

# Data type mapping between MySQL and PostgreSQL
TYPE_CONVERSIONS = {
    # Boolean fields - usually TINYINT(1) in MySQL, BOOLEAN in PostgreSQL
    'boolean': {
        'tables': {
            'x_service_def': ['is_enabled'],
            'x_service': ['is_enabled'],
            'x_policy': ['is_enabled', 'is_audit_enabled'],
            'x_service_config_def': ['is_mandatory'],
            'x_resource_def': ['mandatory', 'look_up_supported', 'recursive_supported', 'excludes_supported'],
            'x_policy_resource': ['is_excludes', 'is_recursive'],
#            'x_policy_item': ['delegate_admin', 'is_enabled'],
#            'x_policy_item_access': ['is_allowed'],
#            'x_policy_item_condition': ['is_allowed'],
#            'x_policy_item_user_perm': ['is_allowed'],
#            'x_policy_item_group_perm': ['is_allowed'],
#            'x_tag_def': ['is_enabled'],
#            'x_tag': ['is_enabled'],
#            'x_service_resource': ['is_enabled'],
#            'x_policy_item_datamask': ['is_allowed'],
#            'x_policy_item_rowfilter': ['is_allowed'],
#            'x_asset': ['sup_native'],
#            'x_perm_map': ['is_wild_card', 'grant_revoke'],
#            'x_resource': ['is_encrypt', 'is_recursive'],
#            'x_group': ['is_visible'],
#            'x_user': ['is_visible'],
#            'x_gds_dataset': ['is_enabled'],
#            'x_gds_project': ['is_enabled'],
#            'x_gds_data_share': ['is_enabled'],
#            'x_gds_shared_resource': ['is_enabled'],
#            'x_gds_data_share_in_dataset': ['is_enabled'],
#            'x_gds_dataset_in_project': ['is_enabled'],
        },
        'conversion': lambda x: True if x == 1 else False if x == 0 else None
    },
    # Other type conversions can be added here
}