# Notification service 
The software stack for implementing the task is as follows:
- Python 3.12.4 
- Fastapi
- Pydantic
- Asyncio
- Async/await
- Asyncpg
- SQL
- Postgresql 15  DBaaS  or Localhost
- JS
- CSS
- HTML
- Logging/loguru
- aio_pika

Statement of the problem (Technical specifications for programming)
It is necessary to develop a mailing management service.
Design and develop a service that, according to given rules, launches a mailing list to a list of clients.

The "mailing" entity has the following attributes:
- unique mailing id
- mailing launch date and time
- message text to be delivered to the client
- filter properties of clients to whom the mailing should be carried out (mobile operator code, tag)
- mailing end date and time: if for some reason all messages were not sent out, no messages should be delivered to clients after this time

The client entity has the following attributes:
- unique client id
- client phone number in the format 7XXXXXXXXXX (X is a number from 0 to 9)
- mobile operator code
- tag (arbitrary label)
- Timezone

The message entity has the following attributes:
- unique message id
- date and time of creation (sending)
- dispatch status
- id of the mailing list within which the message was sent
- id of the client to whom it was sent

Design and implement an API for:
- adding a new client to the directory with all its attributes
- client attribute data updates
- removing a client from the directory
- adding a new newsletter with all its attributes
- obtaining general statistics on created mailings and the number of messages sent on them, grouped by status
- obtaining detailed statistics of sent messages for a specific mailing list
- mailing attribute updates
- deleting the mailing list
- processing active mailings and sending messages to clients

Mailing logic:
- After creating a new mailing, if the current time is greater than the start time and less than the end time, all clients that match the filter values specified in this mailing must be selected from the directory and sending to all these clients must be started.

- If a mailing is created with a start time in the future, the sending should start automatically when this time arrives without additional actions on the part of the system user.
- As messages are sent, statistics should be collected (see the description of the “message” entity above) for each message for subsequent generation of reports.
- An external service that receives sent messages may take a long time to process the request, respond with incorrect data, or not accept requests at all for some time. It is necessary to implement correct handling of such errors. Problems with the external service 
  should not affect the stability of the developed mailing service.

List and functions of the presented scripts:

- mod.py  it is the main program to perform a task.
- asyncpg_pool.py
- data.html This is a WebUI interface.
- create_tables.sql  it is a SQL file with query
- requirements.txt no comments
- urls.py  no comments
- Dockerfile  no comment

This task is self-documented.
- docker pull js18user/smp:mode
- docker run -p 80:80 smp:mode
- 
- http://127.0.0.1:80/docs       forAPI documentation Swagger
- http://127.0.0.1:80/metrics
- http://127.0.0.1:80    for WEB UI(admin panel)

The database and Rabbitmq are relevant, you can work

This is an alternative way For a quick start:

- https://js18user-notification-service-7dae.twc1.net              for WEB UI(admin panel)
- https://js18user-notification-service-7dae.twc1.net/docs         forAPI documentation Swagger
- https://js18user-notification-service-7dae.twc1.net/metrics      

Write me for help: @js18user  telegram 
