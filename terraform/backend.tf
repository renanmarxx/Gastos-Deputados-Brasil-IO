terraform {
    backend "s3" {
        #---------------------------
        # NEVER CHANGE THESE VALUES
        #---------------------------
        #This is the bucket where to store your state file.
        bucket = "aws-<PENDING>-terraform-state"

        #This will allow you to download and view your state file.
        acl = "bucket-owner-full-control"

        #This ensures the state file is stored encrypted at rest in S3.
        encrypt = true

        #This is the region of your S3 bucket.
        region = "us-east-1"

        #---------------------------
        # Configurable Options
        #---------------------------

        #This will be the state file's name.
        key = "<PENDING>-source-system-workflows"

        #This will be used as a folder in which to store your state file.
        workspace_key_prefix = "<PENDING>"

    }
}
