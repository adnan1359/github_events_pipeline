# Aiven Kafka Credentials

Download these three files from the Aiven Console
(your Kafka service → **Connect information** → Apache Kafka tab → **Show / Download**)
and drop them in this folder with **exactly these names**:

| Aiven label          | Save as here       |
|----------------------|--------------------|
| CA certificate       | `ca.pem`           |
| Access certificate   | `service.cert`     |
| Access key           | `service.key`      |

These files are gitignored and must **never** be committed.

Your service connection (already wired into `producer/config.py`):

```
Host: kafka-3ca0846-adnananam1359-fbe9.f.aivencloud.com
Port: 12815
Auth: Client certificate (SSL)
```
