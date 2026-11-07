"""Subscription schema adapted from Thịnh; no import-time client or AWS writes."""

import re


def table_request(table_name, *, delivery_ledger=False):
    if not isinstance(table_name, str) or not re.fullmatch(
        r"[A-Za-z0-9_.-]{3,255}", table_name
    ):
        raise ValueError("Invalid DynamoDB table name")
    key = "delivery_id" if delivery_ledger else "chat_id"
    return {
        "TableName": table_name,
        "KeySchema": [{"AttributeName": key, "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": key, "AttributeType": "S"}],
        "BillingMode": "PAY_PER_REQUEST",
    }


def create_subscription_table(client, table_name, *, confirmed=False):
    """Future deliberate provisioning only; caller supplies an approved client."""
    if confirmed is not True:
        raise ValueError("Table creation requires explicit confirmation")
    return client.create_table(**table_request(table_name))
