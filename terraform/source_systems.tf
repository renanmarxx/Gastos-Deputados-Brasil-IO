module "gastos_deputados" {
  source = "./modules/source_system_job"

  policy_id = "var.policy_id"

  country        = "brazil"
  system         = "brasil_io"
  environment    = var.environment
  workspace_path = var.workspace_path

  data_contract = "gastos_deputados"
  workflow_name = "gastos_deputados"
  job_schedule  = "<PENDING>"

  workflow_tasks = [
    {
      name            = "MOVE_RAW_FILES",
      depends_on      = [],
      entrypoint_file = "move_raw_files.py",
      extra_parameters = [
        "--data_contracts",
        "gastos_deputados",
        "--landing_path",
        "s3://renan-marx-data-engineering-projects/gastos-deputados-brasil-io/landing-bucket-gastos-deputados-brasil-io"
      ]
    },
    {
      name             = "CREATE_ENHANCE_TABLES",
      depends_on       = ["MOVE_RAW_FILES"],
      entrypoint_file  = "create_enhance_tables.py"
      extra_parameters = []
    }
  ]

  pause_status = {
    devl = "PAUSED",
    qual = "PAUSED",
    prod = "PAUSED"
  }
}
