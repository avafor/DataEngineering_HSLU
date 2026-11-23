# Week 11 — Part 4: Make the configuration reusable

[Week 11 overview](README.md) · [Previous: create cloud resources](part-3-terraform.md)

## Goal

Adapt the working configuration from Part 3 so another student can reuse it without editing the resource definitions. Separate **what we create** from **the values used for a particular project**.

Keep using the same `terraform` directory, configuration and state. Complete the BigQuery exercise in Part 3 first. Keep the service-account key configured through `GOOGLE_APPLICATION_CREDENTIALS`; we are not changing authentication.

**Discuss:** If you give your current `main.tf` to another team, which values must they change? What could go wrong if they overlook the project ID or bucket name?

## 1. Identify the values that vary

Open your completed `main.tf`. Record the existing values before changing anything:

| Existing setting | New variable name |
|---|---|
| Provider project ID | `project_id` |
| Bucket name | `bucket_name` |
| BigQuery dataset ID | `dataset_id` |
| Bucket and dataset location | `location` |

Use **exactly the same values** during this refactoring. Keep resource names such as `raw_taxi` and `taxi_analytics` unchanged too: they form Terraform's resource addresses.

We will use three files in the same directory:

| File | Purpose |
|---|---|
| `main.tf` | Describes the provider and resources, using variable references. |
| `variables.tf` | Declares the inputs: their names, types and descriptions. |
| `terraform.tfvars` | Supplies your project's actual input values. |

Terraform reads the `.tf` files in this directory together. You do not import `variables.tf` into `main.tf`. It automatically loads values from a file named `terraform.tfvars` in this directory.

## 2. Declare the variables

Open the supplied [terraform/variables.tf](terraform/variables.tf) beside `main.tf`. It contains a completed `project_id` example and spaces with instructions for the remaining variables.

First remove the temporary `default = ""` line from the example. It only prevents an unused input prompt during Part 3, before `main.tf` uses variables. Your declaration should now look like this:

```hcl
variable "project_id" {
  description = "Google Cloud project in which to create the resources."
  type        = string
}
```

This declares an input named `project_id`. `string` means its value is text. It does not yet supply the project ID.

**Your turn:** Fill the spaces marked TODO in `variables.tf` with declarations for `bucket_name`, `dataset_id` and `location`. Use `string` for all four variables and give each a meaningful description. Do not add defaults for this exercise; provide the values in the next step.

## 3. Supply your values

Create **`terraform.tfvars`** in the same directory. Complete the following with the values you recorded in Step 1:

```hcl
project_id  = "YOUR_EXISTING_PROJECT_ID"
bucket_name = "YOUR_EXISTING_BUCKET_NAME"
dataset_id  = "YOUR_EXISTING_DATASET_ID"
location    = "EU"
```

Replace every placeholder. Use `EU` only if that was the location of both resources in Part 3. This file assigns values; it does not contain `variable` blocks.

The supplied `.gitignore` keeps your local `terraform.tfvars` out of Git. The IDs here are not passwords, but keeping personal settings separate makes the shared exercise easier to reuse. Do not put the JSON key contents into any Terraform file. A shared example can be named `terraform.tfvars.example`; Terraform will not automatically load that example filename.

## 4. Use the variables in main.tf

Change the provider to:

```hcl
provider "google" {
  project = var.project_id
}
```

`var.project_id` means “use the value supplied for the input variable named `project_id`.” Do not surround the reference with quotation marks: `"var.project_id"` would be literal text.

**Your turn:** Replace the remaining hardcoded values:

- Bucket `name`: refer to `bucket_name`.
- Dataset `dataset_id`: refer to `dataset_id`.
- Both resources' `location`: refer to the same `location` variable.

Keep the provider version, resource types, Terraform resource names and other settings unchanged. This is a refactoring exercise, not a request to create different resources.

After trying the changes yourself, compare your result with [main_var.tf.example](terraform/main_var.tf.example). It shows the provider, bucket and dataset using all four variables. Terraform ignores its `.example` extension. If you use it directly, replace the contents of `main.tf` with the example's contents; do not create a second active `.tf` file with the same resource definitions. Complete `variables.tf` and `terraform.tfvars` as described above.

## 5. Predict and inspect the plan

Save all three files. From the same `terraform` directory, run:

```sh
terraform fmt
terraform validate
terraform plan
```

No new provider was added, so you do not need another `init` merely because you created `variables.tf`. If this directory has not been initialized, run `terraform init` first.

**Before inspecting the result:** Which outcome do you expect?

| Where you left Part 3 | Expected plan, with identical values |
|---|---|
| You completed `terraform destroy`. | Two resources to create. |
| You kept both resources, with the original state. | No changes. |

If the plan proposes replacing or deleting an existing resource, check the values and resource names before applying. Moving a value into a variable should not itself change the infrastructure.

If you destroyed the resources in Part 3, inspect the plan and run `terraform apply` to recreate them. Confirm the two additions. Then run `terraform plan` again: it should report no changes. If the resources already exist and your plan reports no changes, no apply is needed.

## 6. Make one controlled change

Now make an intentional infrastructure change. Add this block **inside the bucket resource** in `main.tf`:

```hcl
labels = {
  course = "deng"
}
```

**Predict:** Will this create a second bucket, replace the existing bucket, or update it?

Run `terraform fmt`, `terraform validate` and `terraform plan`. With only this label added, expect **0 to add, 1 to change, 0 to destroy**. Find the label change in the plan. Apply it and inspect the bucket's labels in the Console.

**Your turn:** Make the label's value configurable too. Declare a new string variable, assign its value in `terraform.tfvars`, and reference it from `labels`. Keep the value `deng`. What should the next plan show?

## 7. Explain your design

Show a partner the three files and answer:

1. Where do you declare that a project ID is required, and where do you supply its value?
2. Why does moving an unchanged value into a variable not require a new bucket?
3. Why can two students use the same dataset ID in different projects, but need globally unique bucket names?
4. Does changing `project_id` grant your service account permission in another project?

Keep the resources if you are continuing to the data-pipeline exercise. If you are finishing the practical, use `terraform plan -destroy` and `terraform destroy` as in Part 3 to remove the empty exercise resources. Keep the configuration and state; you can recreate the resources with `terraform apply` later.

Reference: [Terraform input variables](https://developer.hashicorp.com/terraform/language/values/variables).
