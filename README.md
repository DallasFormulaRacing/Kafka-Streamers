# Kafka-Streamers
Using onboard databases to efficiently stream all data collected onboard to the cloud.


This project aims to solve some issues with the Telegraf Implementation 
- [ ] Resend missed data chunks and ensure that all data chunks have been sent once a session is completed
- [ ] Using a database to send data even if the car has been turned off and on again.
- [ ] Send more information then just data including session information (start, stop, error, etc) and raspberry pi telemetry (cpu/net/mem)

Some stretch goals include

- [ ] Running http servers onboard to allow data to be queried from devices connected to the car (via https/sockets)
- [ ] Timing data for how behind the PI is on receiving data.
