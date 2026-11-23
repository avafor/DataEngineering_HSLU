# Part 4: declare inputs here. Supply values in terraform.tfvars OR add defaults
# to every variable below, as explained in Part 4, Step 3.
# Completed example:
variable "project_id" {
  description = "Google Cloud project in which to create the resources."
  type        = string

  # Temporary empty default keeps the Part 3 exercise from prompting for an
  # unused input. Remove this line when you start Part 4.
  default = ""
}

# TODO 1: Declare a variable named bucket_name.
# It holds the globally unique name of the Cloud Storage bucket.
# Follow the project_id example: add a variable block, description and
# type = string. Supply the value in terraform.tfvars or add a default here.
# Write your block below:


# TODO 2: Declare a variable named dataset_id.
# It holds the BigQuery dataset ID (not the project ID or a table name).
# Add a description and type = string; optionally supply its value with default.
# Write your block below:


# TODO 3: Declare a variable named location.
# Both the bucket and dataset will use this input for their storage location.
# Add a description and type = string; optionally supply its value with default.
# Write your block below:

