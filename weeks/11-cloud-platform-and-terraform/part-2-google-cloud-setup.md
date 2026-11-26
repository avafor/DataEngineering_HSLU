# Week 11 — Part 2: Set up your Google Cloud project

[Week 11 overview](README.md) · [Previous: platform map](part-1-platform-map.md)

## Goal

Create a Google Cloud project, connect it to your course coupon's billing account, and let your local tools access it. By the end, you should be able to explain the difference between your account, project, billing account and credentials.

Complete these steps **in class**. Before class, you only needed to activate the emailed coupon with your student email address and install Terraform. See the [preparation checklist](../../preparation/week-11.md).

## 1. Identify the account and billing account

Open the [Google Cloud Console](https://console.cloud.google.com/) and sign in using the student account you used to activate the coupon. If several Google accounts are signed in, check the profile icon before continuing.

Open **Billing** and find the billing account associated with your course coupon. Check the available credit and expiry information shown for your coupon. If the account or credit is missing, ask Forooz before proceeding.

| Item | What it means |
|---|---|
| Google account | Your identity: who is signing in. |
| Google Cloud project | Groups the resources for this exercise. |
| Billing account | Pays for usage in linked projects; the course coupon provides credit. |
| Service account | A separate identity used by a tool such as Terraform. |
| Credentials | Let a tool authenticate as an identity. They do not replace access permissions. |

**Discuss:** Does activating the coupon automatically create the bucket and BigQuery dataset we need?

## 2. Create a project

For this classroom exercise, use your own project unless the instructor assigns a shared one.

1. Open the project selector at the top of the Console.
2. Select **New project**.
3. Enter a recognizable project name, for example `DENG Week 11`.
4. Check the generated **project ID**. You can edit it before creation, but it must be globally unique. Keep the generated suffix if necessary.
5. If asked for an organization or location, select **No organization**. If this option is unavailable or account policies prevent project creation, ask the instructor.
6. Select **Create**, wait for completion, then select the new project.

Record the **project ID**, not just the display name or project number. We will use this ID in commands and later in Terraform. [Google's project-creation instructions](https://docs.cloud.google.com/resource-manager/docs/creating-managing-projects).

**Your turn:** Find the project name and project ID. Explain which one a command should use and why.

## 3. Link the project to the coupon billing account

If you selected the coupon billing account during project creation, verify that selection here.

1. With your new project selected, open **Billing**.
2. If the project has no billing account, choose **Link a billing account** and select the account associated with your course coupon.
3. If it is linked to a different account, use **Billing → Manage billing accounts → My projects** to find the project and choose **Change billing account** from its menu.
4. Confirm the linked account is the intended course account.

If you cannot select or link the account, ask the instructor to check access. Do not select an unrelated personal billing account to bypass the problem. [Google's billing-link instructions](https://docs.cloud.google.com/billing/docs/how-to/modify-project).

**Checkpoint:** Explain the difference between having an activated coupon and having a project linked to its billing account.

## 4. Check Terraform in your local terminal

Use a terminal **on your laptop**, not the browser's Cloud Shell or a Docker container. This is where we will run Terraform.

```sh
terraform version
```

The command should print the installed Terraform version. You do not need to sign in to the Google Cloud CLI for this exercise. Terraform will authenticate using the service-account JSON key configured below, and Part 3 will set the project ID explicitly in `main.tf`.

## 5. Enable the services used by the practical

An API lets tools access a Google Cloud service. Enabling an API makes the service available to the project; it does not create a bucket or dataset.

In the **Google Cloud Console**, first check that your course project is selected. Open **APIs & Services → Library** and search for each API below. Open its page and select **Enable**. If it already shows **Manage**, it is enabled.

| API | Why we use it |
|---|---|
| **Cloud Storage API** (`storage.googleapis.com`) | Create and manage the storage bucket. |
| **BigQuery API** (`bigquery.googleapis.com`) | Create and manage the BigQuery dataset. |
| **Cloud Resource Manager API** (`cloudresourcemanager.googleapis.com`) | Required by Google's documented Terraform setup for creating BigQuery objects. |
| **Identity and Access Management (IAM) API** (`iam.googleapis.com`) | Create the service account and its JSON key. |

Afterward, open **APIs & Services → Enabled APIs & services** and verify these services are enabled. If account permissions prevent enabling a service, ask the instructor.

References: [enable APIs in the Console](https://docs.cloud.google.com/service-usage/docs/enable-disable), [BigQuery's Terraform requirements](https://docs.cloud.google.com/bigquery/docs/datasets), and [service-account setup](https://docs.cloud.google.com/iam/docs/service-accounts-create).

## 6. Create a service account and grant permissions

Your student account handles the setup in the Console. **Terraform will use a separate service account**, authenticated with a downloaded JSON key.

1. In the correct project, open **IAM & Admin → Service Accounts → Create service account**.
2. Name it `terraform-week11` and select **Create and continue**.
3. In the step that grants the service account access to the project, add these roles:

   | Role | Purpose in this exercise |
   |---|---|
   | **Storage Admin** (`roles/storage.admin`) | Create and manage the bucket. |
   | **BigQuery Admin** (`roles/bigquery.admin`) | Create and manage the dataset. |

4. Continue. Leave the optional step for granting users access to the service account empty, then finish creation.
5. Open **IAM & Admin → IAM** and find the service account's email. Verify both roles are assigned to it in this project.

These are broad service-specific roles for our classroom infrastructure exercise. We do not need Compute Engine Admin or Owner. A later pipeline identity should receive permissions suited to its data tasks rather than automatically inheriting these administrator roles.

The email looks like `terraform-week11@YOUR_PROJECT_ID.iam.gserviceaccount.com`. Record it; it identifies the account Terraform will use. Your student account needs permission to create service accounts and grant project roles. If either action is blocked, ask the instructor to check access.

**Discuss:** Who gets these roles: your student account, the Terraform program, or the service account? Does creating a JSON key grant these roles automatically?

## 7. Download the JSON key

1. Open **IAM & Admin → Service Accounts** and select `terraform-week11`.
2. Open **Keys → Add key → Create new key**.
3. Select **JSON**, then **Create**. The browser downloads the key file.
4. Move it to a private folder **outside your Git repository**, and give it a recognizable filename, such as `terraform-week11.json`.

For example, create a private folder on macOS/Linux:

```sh
mkdir -p "$HOME/.config/deng/keys"
```

Move the downloaded file there using your file manager. Its resulting path will be `/Users/YOUR_USERNAME/.config/deng/keys/terraform-week11.json` on macOS. On Windows, use a private folder under your user profile instead.

The JSON contains a private key. Do not share it, paste it into code, or commit it to Git. If key creation is disabled by an organization policy, ask the instructor; do not change the organization policy yourself. [Google's key-creation instructions](https://docs.cloud.google.com/iam/docs/keys-create-delete).

## 8. Tell Terraform where to find the key

In the **same terminal where you will run Terraform**, set the path. For macOS/Linux, using the folder above:

```sh
export GOOGLE_APPLICATION_CREDENTIALS="$HOME/.config/deng/keys/terraform-week11.json"
```

For Windows PowerShell, replace the example path with your actual path:

```powershell
$env:GOOGLE_APPLICATION_CREDENTIALS = "C:\Users\YOUR_USERNAME\.config\deng\keys\terraform-week11.json"
```

This environment variable contains the **path**, not the JSON contents. Terraform uses it to find the key and authenticate as the service account. The key identifies the service account; Part 3 sets the resource project explicitly in `main.tf`.

On macOS/Linux, check the path and whether the file exists without printing its contents:

```sh
echo "$GOOGLE_APPLICATION_CREDENTIALS"
test -f "$GOOGLE_APPLICATION_CREDENTIALS" && echo "Key file found"
```

On PowerShell:

```powershell
$env:GOOGLE_APPLICATION_CREDENTIALS
Test-Path $env:GOOGLE_APPLICATION_CREDENTIALS
```

Repeat the environment-variable command when opening a new terminal. You do **not** need to generate another key. The path check only confirms that the file exists; the later Terraform operations check authentication and resource permissions.

**Your turn:** You can open the project in the Console, but Terraform cannot find credentials. Which file and environment variable would you check? If the key is found but bucket creation is denied, which IAM account would you inspect?

## 9. Finish with a setup check

In your notes, record:

- The project ID and the account you used.
- Whether the intended billing account is linked and billing is enabled.
- Whether all four APIs listed in section 5 are enabled.
- The service account email and its two assigned roles.
- Whether `GOOGLE_APPLICATION_CREDENTIALS` points to the downloaded key in your current terminal.

Keep credentials, tokens and coupon codes out of notes you share or commit.

**Discuss:** We now have a project, billing and authentication. What still needs to be created before we can upload a taxi file and query it?

Next: [Part 3 — Create cloud resources with Terraform](part-3-terraform.md). Leave the bucket and dataset for that exercise so we can see how infrastructure is created from code.
