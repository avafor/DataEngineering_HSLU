# Week 10 — Preparation before class

[All preparation checklists](README.md) · [Module homepage](../README.md)

In Week 10, you will build an incremental pipeline on **Databricks Free Edition**, the same workspace as in
Week 8. At the end, the pipeline reads order events from a Kafka topic, so you also need a small Kafka broker
of your own on **Aiven**. You only need a web browser; nothing is installed and no data needs to be downloaded.

## 1. Sign in

Open your workspace (`https://dbc-….cloud.databricks.com/`) and sign in. No account yet? Follow
[1. Create your account](week-08.md#1-create-your-account) from Week 8.

## 2. Update the Git folder

If you added the course repository as a Git folder in Week 8, open it, click the branch name → **Pull**.
You should then see the folder `weeks/10-structured-streaming-incremental`.

No Git folder yet? That's fine, we set it up together at the start of the practical.

## 3. Check that it works

In **SQL Editor**, run:

```sql
SELECT current_user(), current_catalog();
```

You should see your email. The first query may take up to a minute.

## 4. Create a free Kafka service on Aiven

Aiven runs Apache Kafka for you. The free tier is enough for this course, and you don't need a credit card.

1. Sign up at [console.aiven.io/signup](https://console.aiven.io/signup). Your HSLU email is fine.
2. Go to **Services** → **Create service** → **Aiven for Apache Kafka**.
3. Choose the service tier **Free** and the region group **Europe**, give the service a name (e.g.
   `deng-kafka`) and click **Create service**.
4. Wait until the status says **Running**. This takes a few minutes.

That's all for now. We create the topic and connect Databricks together in class.

A free service powers off after 24 hours without traffic, so yours will probably be off on the day of
the class. That's fine: open the service, choose **Power on service** in the service menu (the three dots)
at the start of the lesson, and it's back after about five minutes.

More about the free tier: [Aiven for Apache Kafka free tier](https://aiven.io/docs/products/kafka/free-tier/kafka-free-tier)

Do not run the Week 10 notebooks yet.
