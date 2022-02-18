import json
from product_delivery.cnm.notify_message_creator import NotifyMessageCreator
from product_delivery.cnm import utilities

test_file = '/path/to/file'
notify_message = NotifyMessageCreator('collection', 'product_name', provider='JPL-OPERA')

# The add_file method adds a file reference to the Notification Message.
# Re-use this method to add multiple files to the Notification Message.
notify_message.add_file(test_file, 's3://bucket/path/to/file/object', 'data', checksum_type='md5')

print(json.dumps(notify_message.dump(), indent=2))
