# Week 6 — Downloads before class

[All preparation checklists](README.md) · [Module homepage](../README.md)

In Week 6, you will extend the local NYC Taxi pipeline toward a cloud data platform. You will compare a data lake, warehouse, and data mart; distinguish ETL from ELT; and use Terraform to describe a Google Cloud Storage bucket and a BigQuery dataset as code. Before class, think about these questions: If the same cloud environment must be created twice, how can we ensure both copies have the same configuration? How can we inspect a proposed infrastructure change before applying it? Which NYC Taxi data belongs in object storage, and which data should be presented as queryable tables? You do not need to prepare answers.

This checklist prepares your account and command-line tools. Do not create cloud resources or start the Terraform exercise before class.

## 1. Confirm your Google Cloud access

Sign in to the [Google Cloud Console](https://console.cloud.google.com/) using the account intended for this module. Confirm that you can select the Google Cloud project assigned to you for the course. If you are expected to create your own project, follow the course instructions and confirm that billing is enabled for that project.

Record the **project ID**, which is different from the project name. You will use the project ID in commands and Terraform variables. Do not put passwords, downloaded credentials, or service-account keys in the repository.

If you do not have access to a suitable project, resolve this before class. Do not create chargeable resources merely to test access.

## 2. Install the Google Cloud CLI

Install the Google Cloud CLI using the [official instructions for your operating system](https://cloud.google.com/sdk/docs/install).

Open a new terminal and check the installation:

```sh
gcloud version
```

The command should print the installed Google Cloud CLI version. It must not return `command not found`.

## 3. Sign in locally

Sign in to the Google Cloud CLI:

```sh
gcloud auth login
```

Set the active project, replacing `YOUR_PROJECT_ID` with your real project ID:

```sh
gcloud config set project YOUR_PROJECT_ID
```

Terraform uses **Application Default Credentials** when it runs locally. Create those credentials with:

```sh
gcloud auth application-default login
```

These two login commands serve different clients: `gcloud auth login` signs in the `gcloud` command-line tool, while `gcloud auth application-default login` makes your user credentials available to local tools such as Terraform. Both commands may open a browser.

Check the selected account and project:

```sh
gcloud auth list
gcloud config get-value project
```

The second command should print the project ID you recorded in Step 1.

## 4. Install Terraform

Install the Terraform CLI using the [official HashiCorp instructions](https://developer.hashicorp.com/terraform/install). Then check it:

```sh
terraform version
```

The command should print a Terraform version. You do not need a HashiCorp Cloud account for this local exercise.

## 5. Get the current course files

Update your course copy using your usual Git workflow while preserving your own work. The Week 6 practical folder and its exact Terraform provider versions will be added as the practical develops. Recheck this preparation file before class; it will be updated with a provider-download command once that configuration is available.

Do not run `terraform apply` from an unrelated example or copy credentials into a `.tf` file.

## Ready-to-attend checklist

- You can open the Google Cloud Console and select the intended project.
- You know the project's project ID.
- `gcloud version` works.
- `gcloud auth list` shows the intended account as active.
- `gcloud config get-value project` shows the intended project ID.
- You completed `gcloud auth application-default login` without an error.
- `terraform version` works.
- You have not created any Week 6 cloud resources yet.

## If something fails

For installation problems, include your operating system, the command, and the complete error message when asking for help. For Google Cloud access problems, include the project ID and the action that failed. Never share access tokens, passwords, `.env` contents, credential files, or service-account keys.
