# Week 11 — Part 3: Create cloud resources with Terraform

[Week 11 overview](README.md) · [Previous: Google Cloud setup](part-2-google-cloud-setup.md)

## Goal

Create a **Cloud Storage bucket** and a **BigQuery dataset** from a Terraform configuration. First, complete a guided bucket example. Then use the documentation to add the dataset yourself.

Terraform prepares the infrastructure. Downloading taxi files, uploading them and loading tables are later pipeline tasks coordinated by Kestra. At the end of this exercise, the bucket and dataset will be empty.

Complete [Part 2](part-2-google-cloud-setup.md) first: your project must have billing, the required APIs and a service account with Storage Admin and BigQuery Admin roles. Set `GOOGLE_APPLICATION_CREDENTIALS` to that service account's downloaded JSON key in the terminal you use here. Terraform acts as this service account, not your personal CLI account. Creating a separate pipeline identity with permissions for its data tasks is a later exercise.

## 1. Open the starter and check your tools

From the **repository root**, run:

```sh
cd weeks/11-cloud-platform-and-terraform/terraform
terraform version
gcloud config get-value project
```

Keep this terminal in the `terraform` folder for all commands below. Open [terraform/main.tf](terraform/main.tf) in your editor. You can optionally install the **HashiCorp Terraform** VS Code extension for syntax highlighting and completion.

Terraform CLI runs commands such as `plan` and `apply`. The **Google provider** is a separate plugin that lets Terraform communicate with Google Cloud; `terraform init` downloads it automatically.

The starter accepts Terraform CLI **1.11.4 or later within version 1.x** and currently specifies Google provider **6.44.0**, with which this configuration passed local validation. This is not a requirement to use versions from previous semesters, nor a claim that this is the latest provider. We specify a provider version so everyone uses the same setup for the exercise.

**Your turn:** Record your project ID. In `main.tf`, replace both `REPLACE_WITH_...` values:

- `project`: your actual project ID from Part 2.
- `name`: a globally unique bucket name, for example your project ID followed by `-taxi-raw`. Use lowercase letters, digits and hyphens for this exercise.

Save `main.tf`. No credentials need to be pasted into it. The provider finds the service-account JSON key through `GOOGLE_APPLICATION_CREDENTIALS`. If you opened a new terminal, repeat the environment-variable command from Part 2. The provider's `project` setting explicitly selects where resources are created; changing the CLI's default project does not change this value.

## 2. Read the configuration before running it

### Choose the project where resources will be created

Before creating the bucket, Terraform needs to know which Google Cloud project it belongs to. In our starter, we set this once in the `provider "google"` block. Resources such as the bucket and BigQuery dataset use that project unless we specify a different project inside their resource blocks.

For example, if your project ID were `dengtaxi`:

```hcl
provider "google" {
  project = "dengtaxi"
}
```

The following bucket has no `project` argument, so it inherits `dengtaxi` from the provider. This is what the resource documentation means when it says “the provider project is used”:

```hcl
resource "google_storage_bucket" "raw_taxi" {
  name     = "dengtaxi-taxi-raw"
  location = "EU"
}
```

These are explanatory snippets, not extra blocks to add to the starter. Keep your own project ID, globally unique bucket name and the starter's other bucket settings.

If you explicitly set `project` inside the bucket resource, it overrides the provider's project **for that bucket**. The service account still needs permission in the selected project. The BigQuery dataset can also inherit the provider project.

Here, “provider project” refers to the Terraform configuration, **not** the default project selected by `gcloud config set project`.

### Understand the starter settings

| Code | Meaning |
|---|---|
| `terraform` | Sets requirements for the CLI and providers. |
| `required_providers` | Identifies the Google provider and its version. |
| `provider "google"` | Configures access to the target project. |
| `resource "google_storage_bucket" "raw_taxi"` | Describes a bucket; `raw_taxi` is its name **inside Terraform**. |
| `name` | The actual bucket name shown in Google Cloud; it must be globally unique. |
| `location = "EU"` | Stores the bucket's data in the EU multi-region. Use EU for the dataset too. |
| `storage_class = "STANDARD"` | Selects Standard storage for this exercise. |
| `uniform_bucket_level_access = true` | Uses IAM to control access instead of individual object ACLs. |
| `public_access_prevention = "enforced"` | Prevents public access to the bucket. |
| `force_destroy = false` | Prevents Terraform from deleting a bucket while it still contains objects. |

**Discuss:** Which value identifies the bucket in Terraform, and which value must be globally unique? Does setting `uniform_bucket_level_access` grant anyone permission by itself?

We start with explicit values in one file. Moving these values into `variables.tf` will be a later refactoring exercise.

### Choose a storage class

The bucket's `storage_class` setting determines the default storage class for newly uploaded objects. For our bucket, the main choices are:

| Terraform value | Suitable for | Minimum billable storage duration | Example |
|---|---|---|---|
| `"STANDARD"` | Frequently accessed or short-lived data | None | Taxi files used repeatedly in class |
| `"NEARLINE"` | Access roughly once a month or less | 30 days | Monthly backups |
| `"COLDLINE"` | Access roughly once a quarter or less | 90 days | Older taxi files rarely reprocessed |
| `"ARCHIVE"` | Access less than once a year | 365 days | Long-term historical archives |

Colder classes cost less to store, but charge for retrieval and have minimum storage durations. You can delete files earlier, but early-deletion charges may apply. Even Archive remains accessible immediately; it does not require hours of restoration. See [Google's storage-class documentation](https://docs.cloud.google.com/storage/docs/storage-classes).

For example, the following setting would select Nearline as the bucket's default:

```hcl
storage_class = "NEARLINE"
```

Changing the bucket's default class does **not** change the class of existing objects. New objects inherit the default unless a different class is explicitly selected when they are uploaded.

**Keep `STANDARD` for this exercise**, because we will access and potentially delete files frequently.

**Discuss:** Which class would you consider for taxi files retained for several years but almost never read? What would change if you began reprocessing those files every week?

## 3. Initialize and validate

Run:

```sh
terraform init
terraform fmt
terraform validate
```

| Command | What happens |
|---|---|
| `init` | Prepares the working directory and downloads the provider. It does not create a bucket. |
| `fmt` | Formats the Terraform code. |
| `validate` | Checks the configuration against Terraform and provider rules. It does not prove your credentials or cloud permissions work. |

After initialization, notice the `.terraform` directory and `.terraform.lock.hcl` file. The lock file records the selected provider version and checksums. Keep it in Git; the downloaded provider directory is ignored by the supplied [.gitignore](terraform/.gitignore).

**Checkpoint:** Validation should succeed. If it fails, read the file and line number in the error and correct the configuration before continuing.

## 4. Predict, plan and create the bucket

**Before running:** How many resources should Terraform propose to create? Will it download any taxi data?

```sh
terraform plan
```

For a fresh exercise, expect **1 to add, 0 to change, 0 to destroy**. Find `google_storage_bucket.raw_taxi` in the plan and check its project, bucket name and location. Values marked `known after apply` will be determined during creation.

If the plan matches your intended project and bucket, run:

```sh
terraform apply
```

This command generates a plan again. Read it and enter `yes` at the confirmation prompt to create the bucket. If it differs from what you intended, cancel and correct the configuration.

Open **Google Cloud Console → Cloud Storage → Buckets**, with the correct project selected. Find your bucket and confirm its location. It should contain no objects.

**Explain to a partner:** What did Terraform create? Why is the bucket empty?

## 5. Your turn: add a BigQuery dataset

Keep the existing bucket configuration in `main.tf`. At the end of the same file, replace the exercise comments with a new resource block.

Use the [Google provider's BigQuery dataset documentation](https://registry.terraform.io/providers/hashicorp/google/6.44.0/docs/resources/bigquery_dataset). Find the resource syntax and its arguments. Your configuration must describe:

| Setting | Required value |
|---|---|
| Resource type | `google_bigquery_dataset` |
| Terraform resource name | `taxi_analytics` |
| `dataset_id` | `taxi_analytics` |
| `location` | `EU`, matching the bucket |
| Project | Inherit the project from the Google provider. |

Do not create the dataset manually in the Console. Your task is to write the Terraform block that creates it. A dataset is a container for BigQuery tables and views; it is not itself a taxi table.

Save `main.tf`, then run:

```sh
terraform fmt
terraform validate
terraform plan
```

**Predict:** Does adding this block recreate the bucket? What should appear in the plan?

With the existing bucket unchanged, expect **one new dataset**, with no bucket deletion or replacement. Inspect the plan, then run `terraform apply` and confirm it.

Open **BigQuery** in the Console. In its Explorer, expand your project and find `taxi_analytics`. Refresh the Explorer if necessary. Confirm that it has no tables yet.

## 6. What happens if we run it again?

Run:

```sh
terraform state list
terraform plan
```

The state list should contain:

```text
google_bigquery_dataset.taxi_analytics
google_storage_bucket.raw_taxi
```

If nobody changed the resources or configuration, the plan should report **No changes**.

Terraform's local state connects the resource names in your code to the resources it manages in Google Cloud. Keep the state files on your machine for subsequent exercises; do not delete them to start a new command. The supplied `.gitignore` excludes them from Git because they can contain sensitive information. State is not a backup of the data stored in your bucket or tables.

**Discuss:** Why does running Terraform again not create a second bucket? Would a newly created folder containing only a copy of `main.tf` automatically have the same state?


## 7. Remove the exercise resources

Finish the exercise by deleting the empty bucket and dataset using Terraform. Use the **same `terraform` directory and state** as before, with the service-account key still configured in your terminal.

**Predict:** Will this delete the Google Cloud project, service account or your local `main.tf` file as well?

First, preview the removal:

```sh
terraform plan -destroy
```

For this exercise, expect **0 to add, 0 to change, 2 to destroy**. Check that the plan names only your exercise bucket and dataset. If it lists other resources, stop and check that you are using the correct directory and state.

Now type and run:

```sh
terraform destroy
```

Read the destruction plan shown by this command. Enter `yes` to confirm the removal of the two exercise resources.

Terraform removes the resources managed by this configuration and state. It does **not** delete your local configuration files, or the project, service account and roles you created manually in Part 2. Keep the state files too; Terraform updates them after removal.

Refresh Cloud Storage and BigQuery in the Console and confirm that the bucket and dataset are gone. Then run:

```sh
terraform state list
terraform plan
```

The state list should be empty. Because `main.tf` still describes the two resources, the new plan should propose **2 to add**. A plan alone does not recreate them. When a later exercise needs them, you can run `terraform apply` again.

Our bucket uses `force_destroy = false`. If you uploaded objects, Terraform will refuse to delete the nonempty bucket. Check its contents before removing any exercise files; do not change that setting just to bypass the error. This practical expects an empty bucket and an empty dataset.

## If something fails

| Symptom | What to check |
|---|---|
| `terraform` is not found | Return to the installation steps in the [preparation](../../preparation/week-11.md). |
| No configuration files found | Run commands from the `terraform` directory containing `main.tf`. |
| Missing credentials | Set `GOOGLE_APPLICATION_CREDENTIALS` in this terminal and check that its JSON key file exists. `gcloud auth login` alone is not sufficient. |
| Wrong credentials | Check that the key belongs to the intended service account and that an old `GOOGLE_CREDENTIALS` setting or explicit provider `credentials` value is not overriding this setup. See Part 2. |
| Permission denied | Check the service account's project roles with the instructor. A valid key authenticates the account but does not grant permissions. |
| Bucket name already exists | Choose a different globally unique name and inspect the new plan. |
| Billing or API error | Recheck the project setup in Part 2. |
| One resource succeeds and the other fails | Correct the reported issue and rerun `plan`; retain the state so Terraform can track the successful resource. |

References: [Google provider](https://registry.terraform.io/providers/hashicorp/google/6.44.0/docs) · [Bucket resource](https://registry.terraform.io/providers/hashicorp/google/6.44.0/docs/resources/storage_bucket) · [Terraform initialization](https://developer.hashicorp.com/terraform/cli/commands/init) · [Google authentication](https://docs.cloud.google.com/docs/terraform/authentication).
