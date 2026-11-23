# Week 11 — Part 2: Set up your Google Cloud project

[Week 11 overview](README.md) · [Previous: platform map](part-1-platform-map.md)

## Goal

Create a Google Cloud project, connect it to your course coupon's billing account, and let your local tools access it. By the end, you should be able to explain the difference between your account, project, billing account and credentials.

Complete these steps **in class**. Before class, you only needed to activate the emailed coupon with your student email address and install Google Cloud CLI and Terraform. See the [preparation checklist](../../preparation/week-11.md).

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
5. If asked for an organization or location, use the location available to your student account. If account policies prevent creation, ask the instructor.
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

## 4. Connect your local terminal to the project

Use a terminal **on your laptop**, not the browser's Cloud Shell or a Docker container. This is where we will run Terraform.

Check the tools:

```sh
gcloud version
terraform version
```

Sign in to the Google Cloud CLI:

```sh
gcloud auth login
```

A browser opens. Choose the same student account as in the Console and complete the sign-in.

In every command below, replace `YOUR_PROJECT_ID` with the project ID you recorded. Set the project:

```sh
gcloud config set project YOUR_PROJECT_ID
```

Check both the account and the project:

```sh
gcloud auth list
gcloud config get-value project
```

The active account should be your student account, and the project ID should match the Console. If the wrong account is active, select the correct signed-in account using `gcloud config set account YOUR_STUDENT_EMAIL`.

Verify the project and billing:

```sh
gcloud projects describe YOUR_PROJECT_ID
gcloud billing projects describe YOUR_PROJECT_ID
```

The billing output should show the expected `billingAccountName` and `billingEnabled: true`. This confirms billing is enabled; it does not show how much coupon credit remains. Check that in the Console. [Billing verification](https://docs.cloud.google.com/billing/docs/how-to/verify-billing-enabled).

## 5. Enable the services used by the practical

An API lets tools access a Google Cloud service. Enabling an API makes the service available to the project; it does not create a bucket or dataset.

Run this as one command:

```sh
gcloud services enable serviceusage.googleapis.com cloudresourcemanager.googleapis.com iam.googleapis.com storage.googleapis.com bigquery.googleapis.com --project=YOUR_PROJECT_ID
```

If prompted to enable Service Usage, accept it for this course project. If the command cannot enable that API, open **APIs & Services → Library** in the Console, enable **Service Usage API**, and rerun the command. Permission errors need the instructor's help; repeatedly retrying does not grant access.

Check the enabled services:

```sh
gcloud services list --enabled --project=YOUR_PROJECT_ID
```

Find Cloud Storage and BigQuery in the output. Enabling these services again is safe if they were already enabled. [Google's service-enabling instructions](https://docs.cloud.google.com/service-usage/docs/enable-disable).

## 6. Create a service account and grant permissions

Your student account handles the setup in the Console and `gcloud`. **Terraform will use a separate service account**, authenticated with a downloaded JSON key.

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

This environment variable contains the **path**, not the JSON contents. Terraform uses it to find the key and authenticate as the service account. It does not change the account used by `gcloud auth login` or select the resource project; Part 3 sets the project explicitly in `main.tf`.

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

**Your turn:** Your `gcloud` commands work, but Terraform cannot find credentials. Which file and environment variable would you check? If the key is found but bucket creation is denied, which IAM account would you inspect?

## 9. Finish with a setup check

In your notes, record:

- The project ID and the account you used.
- Whether the intended billing account is linked and billing is enabled.
- Whether Cloud Storage and BigQuery APIs are enabled.
- The service account email and its two assigned roles.
- Whether `GOOGLE_APPLICATION_CREDENTIALS` points to the downloaded key in your current terminal.

Keep credentials, tokens and coupon codes out of notes you share or commit.

**Discuss:** We now have a project, billing and authentication. What still needs to be created before we can upload a taxi file and query it?

Next: [Part 3 — Create cloud resources with Terraform](part-3-terraform.md). Leave the bucket and dataset for that exercise so we can see how infrastructure is created from code.
