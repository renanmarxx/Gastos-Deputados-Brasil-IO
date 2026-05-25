module "gastos_deputados" {
  source = "./modules/source_system_job"

  policy_id = "var.policy_id"

  country        = "brazil"
  system         = "brasil_io"
  environment    = var.environment
  workspace_path = var.workspace_path

  data_contract = "cota_parlamentar"
  workflow_name = "gastos_deputados"
  job_schedule  = "0 8 * * 1" # Job runs every monday at 8AM

  workflow_tasks = [
    {
      name            = "EXTRACT_RAW_FILES",
      depends_on      = [],
      entrypoint_file = "ingest_csv_to_s3.py",
      extra_parameters = [
        "--data_contracts",
        "cota_parlamentar",
        "--landing_path",
        "s3://renan-marx-data-engineering-projects/gastos-deputados-brasil-io/landing-bucket-gastos-deputados-brasil-io"
      ]
    },
    {
      name             = "CREATE_ENHANCE_TABLES",
      depends_on       = ["EXTRACT_RAW_FILES"],
      entrypoint_file  = "create_enhance_tables.py"
      extra_parameters = []
    }
  ]

  pause_status = {
    devl = "PAUSED",
    prod = "PAUSED"
  }
}
