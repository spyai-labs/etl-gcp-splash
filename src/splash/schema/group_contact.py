from typing import Dict, List
from google.cloud import bigquery

TABLE_SCHEMAS: Dict[str, List[bigquery.SchemaField]] = {
    'group_contact': [
        bigquery.SchemaField('id', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('contact_id', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('salesforce_campaign_member_id', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('event_id', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('event_rsvp_id', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('first_name', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('last_name', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('email', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('status', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('created', 'TIMESTAMP', mode='NULLABLE'),
        bigquery.SchemaField('modified', 'TIMESTAMP', mode='NULLABLE'),
        bigquery.SchemaField('deleted', 'BOOLEAN', mode='NULLABLE'),
        bigquery.SchemaField('_sync_time', 'TIMESTAMP', mode='NULLABLE'),
        bigquery.SchemaField('_deleted', 'BOOLEAN', mode='NULLABLE'),
    ],
    'group_contact_answer': [
        bigquery.SchemaField('id', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('event_id', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('group_contact_id', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('question_id', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('answer', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('_sync_time', 'TIMESTAMP', mode='NULLABLE'),
        bigquery.SchemaField('_deleted', 'BOOLEAN', mode='NULLABLE'),
    ],
    'group_contact_email_campaign_status': [
        bigquery.SchemaField('id', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('event_id', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('group_contact_id', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('email_campaign_id', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('status', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('_sync_time', 'TIMESTAMP', mode='NULLABLE'),
        bigquery.SchemaField('_deleted', 'BOOLEAN', mode='NULLABLE'),
    ],
    'group_contact_event_rsvp': [
        bigquery.SchemaField('id', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('event_id', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('group_contact_id', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('name', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('attending', 'BOOLEAN', mode='NULLABLE'),
        bigquery.SchemaField('date_rsvped', 'TIMESTAMP', mode='NULLABLE'),
        bigquery.SchemaField('checked_in', 'TIMESTAMP', mode='NULLABLE'),
        bigquery.SchemaField('checked_out', 'TIMESTAMP', mode='NULLABLE'),
        bigquery.SchemaField('plus_one', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('created', 'TIMESTAMP', mode='NULLABLE'),
        bigquery.SchemaField('modified', 'TIMESTAMP', mode='NULLABLE'),
        bigquery.SchemaField('deleted', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('ticket_sale_id', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('ticket_number', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('vip', 'BOOLEAN', mode='NULLABLE'),
        bigquery.SchemaField('waitlist', 'BOOLEAN', mode='NULLABLE'),
        bigquery.SchemaField('qr_url', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('unsub_tag', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('unsubscribed', 'BOOLEAN', mode='NULLABLE'),
        bigquery.SchemaField('_sync_time', 'TIMESTAMP', mode='NULLABLE'),
        bigquery.SchemaField('_deleted', 'BOOLEAN', mode='NULLABLE'),
    ],
}
