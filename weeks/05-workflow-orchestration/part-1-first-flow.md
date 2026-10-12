# Week 5 — Your first Kestra flow

[Week 5 overview and startup](README.md)

## Goal

Create a small workflow that accepts a year and month and writes them to a log. This introduces inputs, tasks, and executions before we connect the taxi loader. It does not download files or change the database.

**Before starting:** complete the [Kestra startup instructions](README.md) and open its web interface.

## 1. Read the flow

Open [01-taxi-intro.yaml](flows/01-taxi-intro.yaml). This YAML file defines the workflow:

```yaml
id: taxi_intro
namespace: deng.week5

inputs:
  - id: year
    type: INT
    defaults: 2024
  - id: month
    type: INT
    defaults: 1
    min: 1
    max: 12

tasks:
  - id: show_period
    type: io.kestra.plugin.core.log.Log
    message: "Selected taxi data: year={{ inputs.year }}, month={{ inputs.month }}"
```

| Setting | What it means |
|---|---|
| `id: taxi_intro` | The name identifying our flow within its namespace. |
| `namespace: deng.week5` | Groups our Week 5 flows. It is not a PostgreSQL schema. |
| `inputs` | Values supplied when starting an execution. |
| `type: INT` | The input must be a whole number. |
| `defaults` | The initial value offered when starting a run. |
| `min` and `max` | The allowed range for the month. |
| `tasks` | The steps Kestra will execute. This flow has one task. |
| `id: show_period` | The name of that task. |
| `type: io.kestra.plugin.core.log.Log` | Selects Kestra's built-in task for writing a log message. |
| `{{ inputs.year }}` | Inserts the year supplied for this execution. The month expression works the same way. |

YAML uses indentation to group settings. Preserve the spaces when copying the file; do not replace them with tabs.

**Two rules about IDs:**

1. **IDs must be unique within their scope.** A flow ID must be unique within its namespace: `deng.week5` can contain only one flow named `taxi_intro`, but another namespace can use the same name. Task IDs must be unique within a flow. Running a flow again does not require changing either ID; Kestra generates a separate execution ID for each run.
2. **The namespace and flow ID form the flow's identity.** You will investigate what this means after creating and executing the flow.

## 2. Create the flow in Kestra

1. In the Kestra interface, open **Flows** and choose **Create** or **Create flow**.
2. Open the YAML/source editor if necessary.
3. Replace the example content with the entire contents of `01-taxi-intro.yaml`.
4. Save the flow. You should see `taxi_intro` in the `deng.week5` namespace.

Saving creates the workflow definition. It does not run the task. Editing the file on your laptop also does not automatically update Kestra; save the revised definition in Kestra when you change it.

## 3. Execute it for January

Choose **Execute**, keep `year = 2024` and `month = 1`, and start the execution.

Open the execution and its **Logs** view. Look for the message from `show_period`:

```text
Selected taxi data: year=2024, month=1
```

The task and execution should finish with **SUCCESS**. This confirms the logging task ran; it does not mean taxi data was loaded.

## 4. Run the same flow for February

Start another execution of `taxi_intro`. Set `month = 2`, keeping `year = 2024`. Do not edit the YAML defaults for this step.

Find this message in the new execution's logs:

```text
Selected taxi data: year=2024, month=2
```

Open the execution history and compare both runs. You have one workflow definition and two separate executions, each with its own inputs and log.

## 5. Exercise: can you change a saved flow's ID?

You now have one saved flow called `taxi_intro` and two executions in its history. Test what Kestra does when you try to give that saved flow a different identity.

### Part A — Try to rename the saved flow

1. Open `taxi_intro` and note how many executions it has.
2. Open the flow's **Edit** view.
3. Change only this line:

   ```yaml
   id: taxi_intro
   ```

   to:

   ```yaml
   id: taxi_intro_copy
   ```

4. Try to save the flow.
5. Record exactly what Kestra does:
   - Does it save the change?
   - Does it display an error or warning?
   - Does `taxi_intro` still appear in the flow list?
   - Does a new flow named `taxi_intro_copy` appear?
6. If the editor still contains the changed ID, restore `id: taxi_intro` before leaving it.

### Part B — Save the same definition as a new flow

1. Return to **Flows** and choose **Create flow**.
2. Paste the same YAML definition.
3. Change only the flow ID to `taxi_intro_copy`.
4. Save it and return to the flow list.
5. Open both flows and compare their execution histories.

Answer these questions from your observations:

1. Is changing an existing flow the same operation as creating a new flow?
2. Which two YAML values identify a flow in Kestra?
3. Did the two earlier executions move to `taxi_intro_copy`? What does that tell you about execution history?
4. If you wanted to change only the log message, would you need a new flow ID? Explain.

Keep both flows until you have compared them. You may then delete `taxi_intro_copy`; continue the practical with the original `taxi_intro` flow.

## 6. Check an invalid input

Try entering `month = 13`. Check whether Kestra prevents submission or adjusts the value to `12`. If an execution starts, inspect its inputs to see which month was actually submitted. The permitted range is 1–12.

**Discuss:**

1. What is the difference between saving a flow and executing it?
2. Why can one flow process different months without editing its code?
3. Does a successful logging task prove the taxi records exist in PostgreSQL? Explain.

**Finish with:** one saved flow, two successful executions with different month values, and the log message from each. Next, follow [Run a small Python task](part-2-python.md).
