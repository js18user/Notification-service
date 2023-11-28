# Notification service 
The software stack for implementing the task is as follows:
- Python 3.10.9 
- Fastapi
- Pydantic
- Asyncio
- Async/await
- Asyncpg
- SQL
- Postgresql 14.5  DBaaS  or Localhost
- JS
- CSS
- HTML
- Logging

Statement of the problem (Technical specifications for programming)
It is necessary to develop a mailing management service.
Design and develop a service that, according to given rules, launches a mailing list to a list of clients.

The "mailing" entity has the following attributes:

•
unique mailing id

•
mailing launch date and time

•
message text to be delivered to the client

•
filter properties of clients to whom the mailing should be carried out (mobile operator code, tag)

•
mailing end date and time: if for some reason all messages were not sent out, no messages should be delivered to clients after this time

The client entity has the following attributes:

•
unique client id

•
client phone number in the format 7XXXXXXXXXX (X is a number from 0 to 9)

•
mobile operator code

•
tag (arbitrary label)

•
Timezone

The message entity has the following attributes:

•
unique message id

•
date and time of creation (sending)

•
dispatch status

•
id of the mailing list within which the message was sent

•
id of the client to whom it was sent

Design and implement an API for:

•
adding a new client to the directory with all its attributes

•
client attribute data updates

•
removing a client from the directory

•
adding a new newsletter with all its attributes

•
obtaining general statistics on created mailings and the number of messages sent on them, grouped by status

•
obtaining detailed statistics of sent messages for a specific mailing list

•
mailing attribute updates

•
deleting the mailing list

•
processing active mailings and sending messages to clients
