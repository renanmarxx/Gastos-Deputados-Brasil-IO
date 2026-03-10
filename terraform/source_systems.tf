module "<PENDING>" {
    source = "./modules/source_system_job"

    country = "brazil"
    system = "brasil_io"
    environment = var.environment
    workspace_path = var.workspace_path

    data_contract = "<PENDING>"
    workflow_name = "<PENDING>"
    job_schedule = "<PENDING>"

    workflow_tasks = [
        {
            name = "MOVE_RAW_FILES",
            depends_on = [],
            entrypoint_file = "move_raw_files.py",
            extra_parameters = [
                "--data_contracts",
                "<PENDING>",
                "--landing_path",
                "s3://<PENDING>"
            ]
        },
        {
            name = "CREATE_ENHANCE_TABLES",
            depends_on = ["MOVE_RAW_FILES"],
            entrypoint_file = "create_enhance_tables.py"
            extra_parameters = []
        }
    ]

    pause_status = {
        devl = "PAUSED",
        qual = "PAUSED",
        prod = "PAUSED"
    }
}
