# Week 11 — Preparation before class

[All preparation checklists](README.md) · [Module homepage](../README.md)

In Week 11, you will extend the local NYC Taxi pipeline toward a cloud data platform. You will compare a data lake, warehouse, and data mart; distinguish ETL from ELT; and use Terraform to describe a Google Cloud Storage bucket and a BigQuery dataset as code. Before class, think about these questions: If the same cloud environment must be created twice, how can we ensure both copies have the same configuration? How can we inspect a proposed infrastructure change before applying it? Which NYC Taxi data belongs in object storage, and which data should be presented as queryable tables? You do not need to prepare answers.

Before class, activate your Google Cloud coupon and install Terraform as described below. We will create the project, configure access and run the cloud exercises together in class.

## 1. Activate your Google Cloud coupon

You will receive an email from Forooz with a link to activate your Google Cloud coupon. Follow the instructions in that email and use your **student email address** for activation.

Complete the activation before class and keep the confirmation email or page available. If you have not received the email, or the activation does not work, contact Forooz before the session.

You do not need to create a Google Cloud project yet. We will create the project and connect it to the coupon's billing account together in class.

## 2. Install Terraform

Install the Terraform CLI using the [official HashiCorp instructions](https://developer.hashicorp.com/terraform/install). Then check it:

```sh
terraform version
```

The command should print a Terraform version. You do not need a HashiCorp Cloud account for this local exercise.

## Optional: Install the Google Cloud CLI

The Google Cloud CLI is optional for this session; the required setup uses the browser Console and a service-account JSON key. If you want the CLI available for later work, install it using the [official instructions for your operating system](https://cloud.google.com/sdk/docs/install).

Open a new terminal and check the installation:

```sh
gcloud version
```

The command should print the installed Google Cloud CLI version. It must not return `command not found`.

If the installer offers to run `gcloud init`, you can skip it. This exercise does not require CLI login or project selection.

## What we will do together in class

- Create a Google Cloud project and record its project ID.
- Connect the project to the billing account associated with the coupon and check access.
- Enable the required APIs in the Google Cloud Console.
- Create a service account, grant its permissions and configure its JSON key for Terraform.
- Initialize Terraform, inspect the proposed infrastructure and create the resources for the practical.

## Ready-to-attend checklist

- You activated the coupon using your student email address.
- You have the activation confirmation available.
- `terraform version` works.

No project creation, CLI authentication or Terraform deployment is required before class.

## If something fails

For installation problems, include your operating system, the command and the error message when asking for help. For coupon problems, describe which activation step failed. Do not share the coupon link or code publicly, or include passwords, access tokens or credential files in screenshots.
