"""DynamoDB single table: subscriptions, conversation step and delivery ledger.

pk "sub#<chat>"  subscription {station_id, threshold, active, state, version}
pk "flow#<chat>" pending custom-threshold input (expires)
pk "once#<key>"  idempotency for webhook updates and alert deliveries (expires)
Chat identifiers are stored only as keys and never logged.
"""

import time
from decimal import Decimal


def plain(value):
    if isinstance(value, Decimal):
        return int(value) if value == int(value) else float(value)
    if isinstance(value, dict):
        return {k: plain(v) for k, v in value.items()}
    if isinstance(value, list):
        return [plain(v) for v in value]
    return value


class Store:
    def __init__(self, table, clock=time.time):
        self.table, self.clock = table, clock

    def _conditional(self, e):
        return (
            getattr(e, "response", {}).get("Error", {}).get("Code")
            == "ConditionalCheckFailedException"
        )

    def subscription(self, chat):
        item = self.table.get_item(Key={"pk": "sub#" + chat}).get("Item")
        return plain(item) if item else None

    def subscribe(self, chat, station_id, threshold, state):
        old = self.subscription(chat)
        self.table.put_item(
            Item={
                "pk": "sub#" + chat,
                "chat": chat,
                "station_id": station_id,
                "threshold": threshold,
                "active": True,
                "state": state,
                "version": (old or {}).get("version", 0) + 1,
                "updated_at": int(self.clock()),
            }
        )

    def set_active(self, chat, active):
        self.table.update_item(
            Key={"pk": "sub#" + chat},
            UpdateExpression="SET #a = :a, #v = #v + :one",
            ConditionExpression="attribute_exists(pk)",
            ExpressionAttributeNames={"#a": "active", "#v": "version"},
            ExpressionAttributeValues={":a": active, ":one": 1},
        )

    def unsubscribe(self, chat):
        self.table.delete_item(Key={"pk": "sub#" + chat})
        self.table.delete_item(Key={"pk": "flow#" + chat})

    def save_state(self, chat, state, version):
        """Optimistic update; False if the user changed the subscription meanwhile."""
        try:
            self.table.update_item(
                Key={"pk": "sub#" + chat},
                UpdateExpression="SET #s = :s, #v = :next",
                ConditionExpression="#v = :v",
                ExpressionAttributeNames={"#s": "state", "#v": "version"},
                ExpressionAttributeValues={
                    ":s": state,
                    ":v": version,
                    ":next": version + 1,
                },
            )
            return True
        except Exception as e:
            if self._conditional(e):
                return False
            raise

    def active(self):
        items, start = [], None
        while True:
            kw = {
                "FilterExpression": "begins_with(pk, :p) AND #a = :t",
                "ExpressionAttributeNames": {"#a": "active"},
                "ExpressionAttributeValues": {":p": "sub#", ":t": True},
            }
            if start:
                kw["ExclusiveStartKey"] = start
            page = self.table.scan(**kw)
            items += [plain(i) for i in page.get("Items", [])]
            start = page.get("LastEvaluatedKey")
            if not start:
                return items

    def set_flow(self, chat, station_id):
        self.table.put_item(
            Item={
                "pk": "flow#" + chat,
                "station_id": station_id,
                "expires": int(self.clock()) + 600,
            }
        )

    def flow(self, chat):
        item = self.table.get_item(Key={"pk": "flow#" + chat}).get("Item")
        if not item or item["expires"] < self.clock():
            return None
        return int(item["station_id"])

    def clear_flow(self, chat):
        self.table.delete_item(Key={"pk": "flow#" + chat})

    def once(self, key, days=7):
        """Claim a key exactly once; False if it was already claimed."""
        try:
            self.table.put_item(
                Item={"pk": "once#" + key, "expires": int(self.clock()) + days * 86400},
                ConditionExpression="attribute_not_exists(pk)",
            )
            return True
        except Exception as e:
            if self._conditional(e):
                return False
            raise

    def release(self, key):
        self.table.delete_item(Key={"pk": "once#" + key})
