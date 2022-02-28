# Use Terraform to create GCP resources

* 2022-03-03 - set up a GCS bucket called `energy_project_bucket_data-eng-zoomcamp-339102` for the project
  * note that in [`variables.tf`](variables.tf), there is no default value given for the project variable. 
  Terraform will prompt for this variable upon `terraform apply` and `terraform plan`. You can also supply
  it in the prompt like this: 
  
    ```shell
    # Create new infra
    terraform apply -var="project=<your-gcp-project-id>"
    ```