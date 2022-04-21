# Steps to recreate project

## Provision Cloud Infrastructure
This section will walk you through how to use the existing terraform configuration files to create a Big Query instance, a GCS storage bucket, and a Google Cloud Compute Engine Instance (virtual machine). Then it will walk through configuring the compute instance to be ready to use. 

1. Create a new project on google cloud platform

2. Set up IAM (Identity and Access Management) for a Service Account. Grant this account **Storage Admin** + **Storage Object Admin** + **BigQuery Admin** + **Compute Admin** privileges. This service account will be used by terraform to provision the infrastructure for the project.

![](./images/01_service_account.PNG)

3. Create and download a json key for credentials. Store the file in `~/.config/gcloud`
   Create an environment variable with the path to the credentials json. This will be used by terraform to access GCP.

   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="path/to/credentials.json"
   ```

![](./images/02_service_account_key.PNG)

4. On GCP, enable the Compute Engine API

4. Create an ssh key that will be used to connect the the VM. Update the value of the `metadata: ssh_public_key` variable in the `variables.tf` file with the path to the public key.

    ```bash
    ssh-keygen -t rsa -f ~/.ssh/<name_of_credential> -C <user-name> -b 2048
    ```

5. Use terraform to provision the infrastructure. Terraform will prompt you to enter the project id from GCP.

    ```bash
    cd 01_terraform
    terraform init
    terraform plan
    terraform apply
    ```

Now we can see that the infrastructure has been created on GCP. For example, the VM instance: 

![](./images/03_vm.PNG)


5. Configure local ssh to connect to the vm. 

    On Linux (or WSL in my case) create or modify a file called `~/.ssh/config` to contain the following block of text:

        ```
        Host energy-vm
            HostName <external IP of Compute instance>
            User <username>
            IdentityFile <path to ssh private key created above. something like ~/.ssh/gcp2 >
        ```
    
    For windows you will want to add the same text to the windows ssh config located in 
    
    `/mnt/c/Users/<username>/.ssh/config`

6. Now connect to the vm from the terminal with `ssh energy-vm`

7. In order to authenticate with GitHub, you will need to create a new ssh key and add it to GitHub. Generate the key with the command: 

    ```bash
    ssh-keygen -t rsa -f ~/.ssh/id_rsa -b 4096
    ```

    On GitHub, go to settings, ssh keys, and click New SSH Key. Paste in the contents of the file `~/.ssh/id_rsa.pub` into the textbox. 

7. Git clone this repo using ssh

    ```bash
    git clone git@github.com:mharty3/energy_data_capstone.git
    ```

8. Run the bash setup script and the below commands to install `oh-my-fish` shell and install the theme that I like. 
    
    ```bash
     bash energy_data_capstone/01_terraform/vm_init.sh
     curl https://raw.githubusercontent.com/oh-my-fish/oh-my-fish/master/bin/install | fish
     omf install agnoster
     ```

9. Disconnect and re-connect from the VM via SSH and it will be all set up! Remember that if you shut down and restart the VM, the IP will probably change and you will need to update your local ssh config files accordingly.

### Useful links for GCP and Terraform: 

* https://cloud.google.com/community/tutorials/getting-started-on-gcp-with-terraform

* https://stackoverflow.com/questions/62638916/how-to-provide-image-name-in-gcp-terraform-script
