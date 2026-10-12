# Week 5 — Part 2: Run a small Python task

[Week overview](README.md) · [Previous: your first flow](part-1-first-flow.md)

## Goal

You will construct a taxi filename and URL, observe task order, and investigate a deliberate failure. Allow about 15–20 minutes.

This flow does not download taxi files or connect to a database. There are no database credentials, custom network settings, or file mounts in this flow.

## 1. Predict the result

Complete the [preparation](../../preparation/week-05.md) and start the environment with `docker compose up -d`. This exercise uses the standard `python:3.13.11-slim-bookworm` image listed in preparation. If you prepared before this exercise was added, download it with `docker pull python:3.13.11-slim-bookworm`.

Open [02-python-starter.yaml](flows/02-python-starter.yaml). It has three tasks:

```text
Log the selected period → Run Python → Log completion
```

These top-level tasks execute in order. Before running anything, predict the output of this Python code when year is 2024 and month is 2:

```python
filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
```

`:02d` formats the month as a two-digit integer: `2` becomes `02`.

## 2. Import and observe the working starter

### What is the goal of this step?

The file [02-python-starter.yaml](flows/02-python-starter.yaml) is a **workflow definition**. It tells Kestra which inputs and tasks the workflow contains. It is not a Python file and you do not run it as a terminal command.

In this step, you will copy that definition into Kestra, execute it, and observe how Kestra runs three tasks in order. **There is no missing code to complete in Step 2.** The starter already performs its intended job: it constructs and prints a filename. The coding exercise begins in Step 3, where you extend the working flow to construct a URL.

### What should you do?

1. Open [02-python-starter.yaml](flows/02-python-starter.yaml) in your editor or browser.
2. Copy the **entire file**, beginning with `id: taxi_python` and ending with the `finished` task.
3. In Kestra, open **Flows** and choose **Create flow**.
4. Open the YAML/source editor and replace its example content with the copied YAML.
5. Save the flow. You should now have a new flow with:
   - namespace `deng.week5`;
   - flow ID `taxi_python`; and
   - three tasks: `announce`, `prepare_filename`, and `finished`.
6. Keep the earlier `taxi_intro` flow. This is a second flow, not a replacement for Part 1.
7. Choose **Execute**, enter `year = 2024` and `month = 2`, and start the execution.

Editing the YAML file on your laptop does not automatically update the saved flow in Kestra. For this exercise, make later changes in Kestra's flow editor and save them there.

### What should you inspect?

Open the execution and inspect its **Topology**, **Gantt**, or task list. Confirm that the tasks ran in this order:

```text
announce → prepare_filename → finished
```

Then open **Logs** and select the `prepare_filename` task. Expect:

```text
yellow_tripdata_2024-02.parquet
```

Find the messages from the first and last tasks as well. A printed filename is only text; no file has been created or downloaded.

### Where does Python run?

| Setting | Meaning |
|---|---|
| `type: ...python.Script` | This task executes Python code. |
| `containerImage` | Selects the prepared image containing Python. |
| Docker `taskRunner` | Starts a temporary container for this task. |
| `pullPolicy: NEVER` | Uses our locally built image. |
| `script: \|` | The indented lines below are the Python script. |

Kestra substitutes the input expressions before executing Python. With month 2, `month = {{ inputs.month }}` becomes `month = 2`.

**Discuss:** Which task runs Python? Which tasks simply write Kestra log messages? Why does the Python task need an image?

## 3. Coding exercise — extend the starter to build the source URL

The starter currently prints only a filename. Your solution must make it print the complete source URL as well.

In the `prepare_filename` Python task, add code that constructs and prints the URL using the existing `filename` variable and this base address:

```text
https://d37ci6vzurychx.cloudfront.net/trip-data/
```

Do not hard-code the year or month into the URL. Save your change in Kestra and execute for months 1 and 2.

You are finished when the task prints both lines:

```text
yellow_tripdata_2024-02.parquet
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet
```

**Check:** the logs should show a different filename and URL for each month. For February, the URL must end in `yellow_tripdata_2024-02.parquet`. This does not check whether the file exists at the source.

## 4. Your task — investigate a failure

At the end of the Python script, temporarily add:

```python
raise ValueError("Practice failure: stop before the final task")
```

**Predict first:** what will happen to `announce`, `prepare_filename`, and `finished`?

Save and execute. Find the Python error in the logs and inspect each task's state. Did the final log task execute? Remove the deliberate error, save, and execute again.

We have not configured automatic retries. Fixing and saving the flow does not repair an earlier execution; start a new one and compare the two.

**Finish with:** your URL-generating code, two successful runs with different months, one failed execution, and an explanation of why the final task did not run after the failure.

Next: [Part 3 — Transform taxi columns with SQL](part-3-sql-transformation.md). You will run a query through Kestra and inspect its returned rows.
